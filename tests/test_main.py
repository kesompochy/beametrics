from pubsub_to_metrics.main import parse_filter_conditions, run, create_metrics_config
from unittest.mock import patch, MagicMock, call
from pubsub_to_metrics.filter import FilterCondition
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from pubsub_to_metrics.pipeline import PubsubToCloudMonitoringPipeline
from pubsub_to_metrics.metrics_publisher import (
    GoogleCloudMetricsConfig,
    GoogleCloudConnectionConfig,
)
from pubsub_to_metrics.pipeline_factory import DataflowPipelineConfig
import pytest


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


@patch("pubsub_to_metrics.main.Pipeline")
@patch("google.cloud.monitoring_v3.MetricServiceClient")
def test_run_with_dataflow_and_monitoring(mock_metrics_client, mock_pipeline):
    """
    Test pipeline with DataflowRunner and Cloud Monitoring export
    """
    mock_pipeline_instance = MagicMock()
    mock_pipeline.return_value.__enter__.return_value = mock_pipeline_instance

    run(
        project_id="test-project",
        subscription="projects/test-project/subscriptions/test-subscription",
        labels='{"service": "test-service"}',
        metric_name="test-metric",
        filter_conditions='[{"field": "severity", "value": "ERROR", "operator": "equals"}]',
        region="us-central1",
        temp_location="gs://test-bucket/temp",
        runner="DataflowRunner",
        export_type="monitoring",
    )

    mock_pipeline.assert_called_once()
    pipeline_options = mock_pipeline.call_args[1]["options"]
    assert pipeline_options.get_all_options().get("runner") == "DataflowRunner"
    mock_pipeline_instance | MagicMock(spec=PubsubToCloudMonitoringPipeline)


@patch("pubsub_to_metrics.main.Pipeline")
def test_run_with_direct_and_monitoring(mock_pipeline):
    """
    Test pipeline with DirectRunner and Cloud Monitoring export
    """
    mock_pipeline_instance = MagicMock()
    mock_pipeline.return_value.__enter__.return_value = mock_pipeline_instance

    run(
        project_id="test-project",
        subscription="projects/test-project/subscriptions/test-subscription",
        labels='{"service": "test-service"}',
        metric_name="test-metric",
        filter_conditions='[{"field": "severity", "value": "ERROR", "operator": "equals"}]',
        runner="DirectRunner",
        export_type="monitoring",
    )

    mock_pipeline.assert_called_once()
    pipeline_options = mock_pipeline.call_args[1]["options"]
    assert pipeline_options.get_all_options().get("runner") == "DirectRunner"

    mock_pipeline_instance | MagicMock(spec=PubsubToCloudMonitoringPipeline)


@patch("pubsub_to_metrics.pipeline_factory.Pipeline")
def test_run_with_unsupported_runner(mock_pipeline):
    """
    Test pipeline with unsupported runner
    """
    with pytest.raises(ValueError) as exc_info:
        run(
            project_id="test-project",
            subscription="projects/test-project/subscriptions/test-subscription",
            labels='{"service": "test-service"}',
            metric_name="test-metric",
            filter_conditions='[{"field": "severity", "value": "ERROR", "operator": "equals"}]',
            region="us-central1",
            temp_location="gs://test-bucket/temp",
            runner="UnsupportedRunner",
            export_type="monitoring",
        )

    assert "Unsupported runner type: UnsupportedRunner" in str(exc_info.value)


@patch("pubsub_to_metrics.pipeline_factory.Pipeline")
def test_run_with_unsupported_export_type(mock_pipeline):
    """
    Test pipeline with unsupported export type
    """
    with pytest.raises(ValueError) as exc_info:
        run(
            project_id="test-project",
            subscription="projects/test-project/subscriptions/test-subscription",
            labels='{"service": "test-service"}',
            metric_name="test-metric",
            filter_conditions='[{"field": "severity", "value": "ERROR", "operator": "equals"}]',
            region="us-central1",
            temp_location="gs://test-bucket/temp",
            runner="DataflowRunner",
            export_type="unsupported",
        )

    assert "Unsupported export type: unsupported" in str(exc_info.value)


def test_create_metrics_config_for_monitoring():
    """Test metrics config creation for Cloud Monitoring"""
    config = create_metrics_config(
        metric_name="test-metric",
        labels={"service": "test-service"},
        project_id="test-project",
        export_type="monitoring",
    )

    assert isinstance(config, GoogleCloudMetricsConfig)
    assert config.metric_name == "custom.googleapis.com/test-metric"
    assert config.labels == {"service": "test-service"}
    assert isinstance(config.connection_config, GoogleCloudConnectionConfig)
    assert config.connection_config.project_id == "test-project"


def test_create_metrics_config_with_unsupported_type():
    """Test metrics config creation with unsupported export type"""
    with pytest.raises(ValueError) as exc_info:
        create_metrics_config(
            metric_name="test-metric",
            labels={"service": "test-service"},
            project_id="test-project",
            export_type="unsupported",
        )

    assert "Unsupported export type: unsupported" in str(exc_info.value)
