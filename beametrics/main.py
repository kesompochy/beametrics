import json
from typing import List

import apache_beam as beam
from apache_beam import Pipeline
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    StandardOptions,
)

from beametrics.filter import FilterCondition
from beametrics.metrics import MetricDefinition, MetricType
from beametrics.metrics_exporter import (
    GoogleCloudConnectionConfig,
    GoogleCloudMetricsConfig,
)
from beametrics.pipeline import MessagesToMetricsPipeline


class BeametricsOptions(PipelineOptions):
    """Pipeline options for Beametrics."""

    @classmethod
    def _add_argparse_args(cls, parser):
        if any(group.title == "Beametrics Options" for group in parser._action_groups):
            return
        parser.add_argument_group("Beametrics Options")

        # Required parameters
        parser.add_value_provider_argument(
            "--metric-name",
            type=str,
            required=True,
            help="Name of the metric to create",
        )
        parser.add_value_provider_argument(
            "--subscription",
            type=str,
            required=True,
            help="Pub/Sub subscription to read from",
        )
        parser.add_value_provider_argument(
            "--metric-labels",
            type=str,
            required=True,
            help="Labels to attach to the metric (JSON format)",
        )
        parser.add_value_provider_argument(
            "--filter-conditions",
            type=str,
            required=True,
            help="Filter conditions (JSON format)",
        )

        # Optional parameters
        parser.add_value_provider_argument(
            "--metric-type",
            type=str,
            default="count",
            help="Type of metric to generate (count or sum)",
        )
        parser.add_value_provider_argument(
            "--metric-field", type=str, help="Field to use for sum metrics"
        )
        parser.add_value_provider_argument(
            "--window-size", type=int, default=120, help="Window size in seconds"
        )
        parser.add_value_provider_argument(
            "--export-type",
            type=str,
            default="monitoring",
            help="Type of export destination",
        )
        parser.add_value_provider_argument(
            "--dataflow-template-type",
            type=str,
            help="Type of Dataflow template (flex or classic)",
        )

    def validate_options(self):
        standard_options = self.view_as(StandardOptions)
        if standard_options.runner not in ["DirectRunner", "DataflowRunner"]:
            raise ValueError(f"Unsupported runner type: {standard_options.runner}")

        export_type = self.export_type
        if isinstance(export_type, beam.options.value_provider.ValueProvider):
            if isinstance(export_type, beam.options.value_provider.StaticValueProvider):
                export_type = export_type.value
            else:
                export_type = "monitoring"

        if export_type != "monitoring":
            raise ValueError(f"Unsupported export type: {export_type}")

        metric_type = self.metric_type
        if isinstance(metric_type, beam.options.value_provider.ValueProvider):
            if isinstance(metric_type, beam.options.value_provider.StaticValueProvider):
                metric_type = metric_type.value
            else:
                metric_type = "count"

        if metric_type not in ["count", "sum"]:
            raise ValueError(f"Unsupported metric type: {metric_type}")

        if metric_type == "sum":
            metric_field = getattr(self, "metric_field", None)
            if isinstance(metric_field, beam.options.value_provider.ValueProvider):
                if isinstance(
                    metric_field, beam.options.value_provider.StaticValueProvider
                ):
                    metric_field = metric_field.value
                else:
                    metric_field = None
            if not metric_field:
                raise ValueError("field is required for sum metric type")

    def get(self, option_name, default_value=None):
        return self._all_options.get(option_name, default_value)


def parse_filter_conditions(conditions_json: str) -> List[FilterCondition]:
    """Parse filter conditions from JSON string"""
    conditions = json.loads(conditions_json)
    if not isinstance(conditions, list) or len(conditions) == 0:
        raise ValueError("Filter conditions must be a non-empty list")

    return [
        FilterCondition(
            field=condition["field"],
            value=condition["value"],
            operator=condition["operator"],
        )
        for condition in conditions
    ]


def create_metrics_config(
    metric_name: str,
    metric_labels: dict,
    project_id: str,
    export_type: str,
) -> GoogleCloudMetricsConfig:
    """Create metrics configuration based on export type.

    Args:
        metric_name: Name of the metric
        metric_labels: Dictionary of labels to attach to the metric
        project_id: GCP project ID
        export_type: Type of export destination ("monitoring", etc)

    Returns:
        GoogleCloudMetricsConfig: Configuration for the specified export type

    Raises:
        ValueError: If export_type is not supported
    """
    if isinstance(export_type, beam.options.value_provider.ValueProvider):
        if isinstance(export_type, beam.options.value_provider.StaticValueProvider):
            export_type = export_type.value
        else:
            export_type = "monitoring"

    if export_type != "monitoring":
        raise ValueError(f"Unsupported export type: {export_type}")

    return GoogleCloudMetricsConfig(
        metric_name=f"custom.googleapis.com/{metric_name}",
        metric_labels=metric_labels,
        connection_config=GoogleCloudConnectionConfig(project_id=project_id),
    )


def run(pipeline_options: BeametricsOptions) -> None:
    """Run the pipeline with the given options."""
    options = pipeline_options.view_as(BeametricsOptions)
    options.view_as(StandardOptions).streaming = True
    options.validate_options()

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    project_id = google_cloud_options.project
    metric_name = options.metric_name
    metric_labels = options.metric_labels
    filter_conditions = options.filter_conditions
    metric_type = options.metric_type
    metric_field = getattr(options, "metric_field", None)
    window_size = options.window_size
    export_type = options.export_type

    # Must be str or None as arg for ReadFromPubSub with DataflowRunner, not ValueProvider
    subscription = options.subscription.get()

    metrics_config = create_metrics_config(
        metric_name=metric_name,
        metric_labels=metric_labels,
        project_id=project_id,
        export_type=export_type,
    )

    try:
        metric_type_enum = MetricType[metric_type.get().upper()]
    except KeyError:
        raise ValueError(f"Unsupported metric type: {metric_type.get()}")

    metric_definition = MetricDefinition(
        name=metric_name,
        type=metric_type_enum,
        field=metric_field,
        metric_labels=metric_labels,
    )

    with Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadFromPubSub" >> ReadFromPubSub(subscription=subscription)
            | "ProcessMessages"
            >> MessagesToMetricsPipeline(
                filter_conditions=parse_filter_conditions(filter_conditions.get()),
                metrics_config=metrics_config,
                metric_definition=metric_definition,
                window_size=window_size,
                export_type=export_type,
            )
        )


def main():
    """Main entry point."""
    pipeline_options = BeametricsOptions()
    run(pipeline_options)


if __name__ == "__main__":
    main()
