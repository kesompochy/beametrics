from pubsub_to_metrics.main import parse_filter_conditions
from unittest.mock import patch, MagicMock, call
from pubsub_to_metrics.main import run
from pubsub_to_metrics.filter import FilterCondition
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from pubsub_to_metrics.pipeline import PubsubToMetricsPipeline
from pubsub_to_metrics.metrics_publisher import (
    GoogleCloudMetricsConfig,
    GoogleCloudConnectionConfig,
)


def test_parse_filter_conditions():
    """
    Test parsing a valid filter condition from JSON string
    """
    json_str = '[{"field": "severity", "value": "ERROR", "operator": "equals"}]'
    condition = parse_filter_conditions(json_str)

    assert isinstance(condition, FilterCondition)
    assert condition.field == "severity"
    assert condition.value == "ERROR"
    assert condition.operator == "equals"


@patch("apache_beam.Pipeline")
@patch("google.cloud.monitoring_v3.MetricServiceClient")
def test_run_with_filter_conditions(mock_metrics_client, mock_pipeline):
    """
    Test that run() correctly sets up the pipeline with filter conditions
    """
    # Arrange
    mock_pipeline_instance = MagicMock()
    mock_pipeline.return_value.__enter__.return_value = mock_pipeline_instance

    # Act
    run(
        project_id="test-project",
        subscription="projects/test-project/subscriptions/test-subscription",
        labels='{"service": "test-service"}',
        metric_name="test-metric",
        filter_conditions='[{"field": "severity", "value": "ERROR", "operator": "equals"}]',
        region="us-central1",
        temp_location="gs://test-bucket/temp",
    )

    # Assert
    mock_pipeline.assert_called_once()

    # Verify pipeline options
    pipeline_options = mock_pipeline.call_args[1]["options"]
    assert pipeline_options.get_all_options().get("streaming") is True
    assert pipeline_options.get_all_options().get("project") == "test-project"
    assert pipeline_options.get_all_options().get("runner") == "DataflowRunner"
    assert pipeline_options.get_all_options().get("region") == "us-central1"
    assert (
        pipeline_options.get_all_options().get("temp_location")
        == "gs://test-bucket/temp"
    )
