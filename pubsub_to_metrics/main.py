import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from .pipeline import PubsubToMetricsPipeline
from .filter import FilterCondition
from .metrics_publisher import GoogleCloudMetricsConfig, GoogleCloudConnectionConfig
import json


def parse_filter_conditions(filter_conditions: str) -> FilterCondition:
    """
    Create a FilterCondition from a JSON string

    Args:
        filter_conditions: JSON string. Example: '[{"field": "severity", "value": "ERROR", "operator": "equals"}]'

    Returns:
        FilterCondition: Parsed filter condition

    Raises:
        ValueError: If JSON parsing fails or required fields are missing
    """
    try:
        conditions = json.loads(filter_conditions)
    except json.JSONDecodeError:
        raise ValueError("Invalid filter conditions format")

    if not isinstance(conditions, list) or len(conditions) == 0:
        raise ValueError("Filter conditions must be a non-empty array")

    condition = conditions[0]  # Currently using only the first condition

    required_fields = ["field", "value", "operator"]
    if not all(field in condition for field in required_fields):
        raise ValueError("Missing required field in filter condition")

    return FilterCondition(
        field=condition["field"],
        value=condition["value"],
        operator=condition["operator"],
    )


def run(
    project_id: str,
    subscription: str,
    metric_name: str,
    labels: str,
    filter_conditions: str,
    region: str,
    temp_location: str,
):
    options = PipelineOptions(
        streaming=True,
        project=project_id,
        runner="DataflowRunner",
        region=region,
        temp_location=temp_location,
        setup_file="./setup.py",
    )

    filter_condition = parse_filter_conditions(filter_conditions)
    metric_labels = json.loads(labels)

    metrics_config = GoogleCloudMetricsConfig(
        metric_name=f"custom.googleapis.com/{metric_name}",
        labels=metric_labels,
        connection_config=GoogleCloudConnectionConfig(project_id=project_id),
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadFromPubSub" >> ReadFromPubSub(subscription=subscription)
            | "ProcessMessages"
            >> PubsubToMetricsPipeline(filter_condition, metrics_config)
        )


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--subscription", required=True)
    parser.add_argument("--metric-name", required=True)
    parser.add_argument("--labels", required=True)
    parser.add_argument("--filter-conditions", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--temp-location", required=True)

    args = parser.parse_args()
    run(
        project_id=args.project_id,
        subscription=args.subscription,
        metric_name=args.metric_name,
        labels=args.labels,
        filter_conditions=args.filter_conditions,
        region=args.region,
        temp_location=args.temp_location,
    )


if __name__ == "__main__":
    main()
