import json
import logging
from typing import List

import apache_beam as beam
from apache_beam import Pipeline, error
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    StandardOptions,
)

from beametrics.config import load_yaml_config
from beametrics.filter import FilterCondition
from beametrics.metrics import MetricDefinition
from beametrics.metrics_exporter import (
    ExporterConfig,
    GoogleCloudConnectionConfig,
    GoogleCloudExporterConfig,
    LocalExporterConfig,
)
from beametrics.pipeline import MessagesToMetricsPipeline, MetricConfig


class BeametricsOptions(PipelineOptions):
    """Pipeline options for Beametrics."""

    @classmethod
    def _add_argparse_args(cls, parser):
        if any(group.title == "Beametrics Options" for group in parser._action_groups):
            return
        parser.add_argument_group("Beametrics Options")

        # Required parameters
        parser.add_value_provider_argument(
            "--subscription",
            type=str,
            required=True,
            help="Pub/Sub subscription to read from",
        )
        # Optional parameters
        parser.add_value_provider_argument(
            "--dataflow-template-type",
            type=str,
            help="Type of Dataflow template (flex or classic)",
        )

        # Single metric options
        parser.add_value_provider_argument(
            "--metric-name",
            type=str,
            help="Name of the metric to create",
        )
        parser.add_value_provider_argument(
            "--metric-labels",
            type=str,
            default="{}",
            help="Labels to attach to the metric (JSON format)",
        )
        parser.add_value_provider_argument(
            "--filter-conditions",
            type=str,
            help="Filter conditions (JSON format)",
        )

        # Optional parameters for single metric
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
            default="google-cloud-monitoring",
            help="Type of export destination",
        )
        parser.add_value_provider_argument(
            "--dynamic-labels",
            type=str,
            help="Dynamic labels (JSON format)",
            default="{}",
        )

        # Multiple metrics options
        parser.add_value_provider_argument(
            "--metrics",
            help="JSON array of metric configurations",
        )

        parser.add_value_provider_argument(
            "--config",
            help="Path to YAML config file in GCS (gs://bucket/path/to/config.yaml)",
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
                export_type = "google-cloud-monitoring"

        if export_type != "google-cloud-monitoring" and export_type != "local":
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


def create_metrics_configs(config_dict: dict, project_id: str) -> List[MetricConfig]:
    """Convert config dictionary to list of MetricConfig objects."""
    metrics_configs = []
    for metric_config in config_dict.get("metrics", []):
        exporter_config = create_exporter_config(
            metric_name=metric_config["name"],
            metric_labels=metric_config["labels"],
            project_id=project_id,
            export_type=metric_config.get("export_type", "google-cloud-monitoring"),
        )

        metric_definition = MetricDefinition(
            name=metric_config["name"],
            type=metric_config["type"],
            field=metric_config.get("field"),
            metric_labels=metric_config["labels"],
            dynamic_labels=metric_config.get("dynamic_labels", {}),
        )

        metrics_configs.append(
            MetricConfig(
                filter_conditions=parse_filter_conditions(
                    json.dumps(metric_config["filter-conditions"])
                ),
                exporter_config=exporter_config,
                metric_definition=metric_definition,
            )
        )
    return metrics_configs


def create_exporter_config(
    metric_name: str,
    metric_labels: dict,
    project_id: str,
    export_type: str,
) -> ExporterConfig:
    """Create metrics configuration based on export type.

    Args:
        metric_name: Name of the metric
        metric_labels: Dictionary of labels to attach to the metric
        project_id: GCP project ID
        export_type: Type of export destination ("google-cloud-monitoring", etc)

    Returns:
        GoogleCloudMetricsConfig: Configuration for the specified export type

    Raises:
        ValueError: If export_type is not supported
    """
    if isinstance(export_type, beam.options.value_provider.ValueProvider):
        if isinstance(export_type, beam.options.value_provider.StaticValueProvider):
            export_type = export_type.value
        else:
            export_type = "google-cloud-monitoring"

    if export_type != "google-cloud-monitoring" and export_type != "local":
        raise ValueError(f"Unsupported export type: {export_type}")

    if export_type == "local":
        return LocalExporterConfig(
            metric_name=metric_name,
            metric_labels=metric_labels,
            connection_config=GoogleCloudConnectionConfig(project_id=project_id),
        )

    return GoogleCloudExporterConfig(
        metric_name=f"custom.googleapis.com/{metric_name}",
        metric_labels=metric_labels,
        connection_config=GoogleCloudConnectionConfig(project_id=project_id),
    )


def create_single_metric_config(
    options: BeametricsOptions, project_id: str
) -> MetricConfig:
    """Create a single metric configuration from command line options."""
    exporter_config = create_exporter_config(
        metric_name=options.metric_name,
        metric_labels=options.metric_labels,
        project_id=project_id,
        export_type=options.export_type,
    )

    metric_definition = MetricDefinition(
        name=options.metric_name,
        type=options.metric_type,
        field=getattr(options, "metric_field", None),
        metric_labels=options.metric_labels,
        dynamic_labels=options.dynamic_labels,
    )

    filter_conditions = (
        options.filter_conditions.get()
        if isinstance(
            options.filter_conditions, beam.options.value_provider.StaticValueProvider
        )
        else (
            options.filter_conditions
            if not isinstance(
                options.filter_conditions,
                beam.options.value_provider.RuntimeValueProvider,
            )
            else '[{"field": "severity", "value": "ERROR", "operator": "equals"}]'
        )
    )

    return MetricConfig(
        filter_conditions=parse_filter_conditions(filter_conditions),
        exporter_config=exporter_config,
        metric_definition=metric_definition,
    )


def run(pipeline_options: BeametricsOptions) -> None:
    """Run the pipeline with the given options."""
    options = pipeline_options.view_as(BeametricsOptions)
    options.view_as(StandardOptions).streaming = True
    options.validate_options()

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    project_id = google_cloud_options.project
    # Must be str or None as arg for ReadFromPubSub with DataflowRunner, not ValueProvider
    subscription = options.subscription.get()
    window_size = options.window_size

    metrics_configs = []
    if (
        options.config is not None
        and isinstance(options.config, beam.options.value_provider.StaticValueProvider)
        and options.config.get() is not None
    ):

        config = load_yaml_config(options.config.get())
        metrics_configs = create_metrics_configs(config, project_id)
    elif (
        options.metrics is not None
        and isinstance(options.metrics, beam.options.value_provider.StaticValueProvider)
        and options.metrics.get() is not None
    ):
        try:
            config = {"metrics": json.loads(options.metrics.get())}
            metrics_configs = create_metrics_configs(config, project_id)
        except (json.JSONDecodeError, error.RuntimeValueProviderError):
            metrics_configs = [create_single_metric_config(options, project_id)]
    else:
        metrics_configs = [create_single_metric_config(options, project_id)]

    with Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadFromPubSub" >> ReadFromPubSub(subscription=subscription)
            | "ProcessMessages"
            >> MessagesToMetricsPipeline(
                metrics_configs=metrics_configs,
                window_size=window_size,
            )
        )


def main():
    """Main entry point."""
    logging.basicConfig(level=logging.INFO)
    pipeline_options = BeametricsOptions()
    run(pipeline_options)


if __name__ == "__main__":
    main()
