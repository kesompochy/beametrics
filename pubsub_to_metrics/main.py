import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from .pipeline import PubsubToCloudMonitoringPipeline
from .filter import FilterCondition
from .metrics import MetricType, MetricDefinition
from .metrics_exporter import GoogleCloudMetricsConfig, GoogleCloudConnectionConfig
import json
import argparse
from .pipeline_factory import GoogleCloudPipelineFactory, DataflowPipelineConfig
from apache_beam import Pipeline
from enum import Enum
from typing import Dict, Optional, List


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
    labels: dict,
    project_id: str,
    export_type: str,
) -> GoogleCloudMetricsConfig:
    """Create metrics configuration based on export type.

    Args:
        metric_name: Name of the metric
        labels: Dictionary of labels to attach to the metric
        project_id: GCP project ID
        export_type: Type of export destination ("monitoring", etc)

    Returns:
        GoogleCloudMetricsConfig: Configuration for the specified export type

    Raises:
        ValueError: If export_type is not supported
    """
    if export_type == "monitoring":
        return GoogleCloudMetricsConfig(
            metric_name=f"custom.googleapis.com/{metric_name}",
            labels=labels,
            connection_config=GoogleCloudConnectionConfig(project_id=project_id),
        )
    else:
        raise ValueError(f"Unsupported export type: {export_type}")


def run(
    project_id: str,
    subscription: str,
    labels: str,
    metric_name: str,
    filter_conditions: str,
    region: Optional[str] = None,
    temp_location: Optional[str] = None,
    runner: str = "DirectRunner",
    export_type: str = "monitoring",
    metric_type: str = "count",
    metric_field: Optional[str] = None,
    window_size: int = 60,
) -> None:
    """Run the pipeline"""
    if runner not in ["DataflowRunner", "DirectRunner"]:
        raise ValueError(f"Unsupported runner type: {runner}")

    if export_type != "monitoring":
        raise ValueError(f"Unsupported export type: {export_type}")

    if runner == "DataflowRunner":
        if not region or not temp_location:
            raise ValueError("region and temp_location are required for DataflowRunner")

    try:
        metric_type_enum = MetricType[metric_type.upper()]
    except KeyError:
        raise ValueError(f"Unsupported metric type: {metric_type}")

    if metric_type_enum == MetricType.SUM and not metric_field:
        raise ValueError("field is required for sum metric type")

    pipeline_options = [
        f"--runner={runner}",
        f"--project={project_id}",
        "--streaming",
    ]

    if runner == "DataflowRunner":
        if not region or not temp_location:
            raise ValueError("region and temp_location are required for DataflowRunner")
        pipeline_options.extend(
            [
                f"--region={region}",
                f"--temp_location={temp_location}",
                "--setup_file=./setup.py",
            ]
        )

    parsed_filter_conditions = parse_filter_conditions(filter_conditions)
    metrics_config = create_metrics_config(
        metric_name=metric_name,
        labels=json.loads(labels),
        project_id=project_id,
        export_type=export_type,
    )

    metric_definition = MetricDefinition(
        name=metric_name,
        type=metric_type_enum,
        field=metric_field,
        labels=json.loads(labels),
    )

    with Pipeline(options=PipelineOptions(pipeline_options)) as p:
        (
            p
            | "ReadFromPubSub" >> ReadFromPubSub(subscription=subscription)
            | "ProcessMessages"
            >> PubsubToCloudMonitoringPipeline(
                filter_conditions=parsed_filter_conditions,
                metrics_config=metrics_config,
                metric_definition=metric_definition,
                window_size=window_size,
            )
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--subscription", required=True)
    parser.add_argument("--metric-name", required=True)
    parser.add_argument("--labels", required=True)
    parser.add_argument("--filter-conditions", required=True)
    parser.add_argument("--region")
    parser.add_argument("--temp-location")
    parser.add_argument("--runner", default="DataflowRunner")
    parser.add_argument("--export-type", default="monitoring")
    parser.add_argument(
        "--metric-type",
        default="count",
        choices=["count", "sum"],
        help="Type of metric to generate",
    )
    parser.add_argument("--metric-field", help="Field to use for sum/average metrics")
    parser.add_argument(
        "--window-size",
        type=int,
        default=60,
    )

    args = parser.parse_args()
    run(
        project_id=args.project_id,
        subscription=args.subscription,
        metric_name=args.metric_name,
        labels=args.labels,
        filter_conditions=args.filter_conditions,
        region=args.region,
        temp_location=args.temp_location,
        runner=args.runner,
        export_type=args.export_type,
        metric_type=args.metric_type,
        metric_field=args.metric_field,
        window_size=args.window_size,
    )


if __name__ == "__main__":
    main()
