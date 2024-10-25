from beametrics.main import parse_filter_conditions, run, create_metrics_config
from unittest.mock import patch, MagicMock, call
from beametrics.filter import FilterCondition
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from beametrics.pipeline import PubsubToCloudMonitoringPipeline
from beametrics.metrics_exporter import (
    GoogleCloudMetricsConfig,
    GoogleCloudConnectionConfig,
)
from beametrics.pipeline_factory import DataflowPipelineConfig
import pytest


def test_parse_filter_conditions():
    """
    Test parsing a valid filter condition from JSON string
    """
    json_str = '[{"field": "severity", "value": "ERROR", "operator": "equals"}]'
    conditions = parse_filter_conditions(json_str)

    assert isinstance(conditions, list)
    assert len(conditions) == 1

    condition = conditions[0]
    assert isinstance(condition, FilterCondition)
    assert condition.field == "severity"
    assert condition.value == "ERROR"
    assert condition.operator == "equals"


@patch("beametrics.main.Pipeline")
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
        metric_labels='{"service": "test-service"}',
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


@patch("beametrics.main.Pipeline")
def test_run_with_direct_and_monitoring(mock_pipeline):
    """Test pipeline with DirectRunner and Cloud Monitoring export"""
    mock_pipeline_instance = MagicMock()
    mock_pipeline.return_value.__enter__.return_value = mock_pipeline_instance

    run(
        project_id="test-project",
        subscription="projects/test-project/subscriptions/test-subscription",
        metric_labels='{"service": "test-service"}',
        metric_name="test-metric",
        filter_conditions='[{"field": "severity", "value": "ERROR", "operator": "equals"}]',
        runner="DirectRunner",
        export_type="monitoring",
    )

    mock_pipeline.assert_called_once()
    pipeline_options = mock_pipeline.call_args[1]["options"]
    all_options = pipeline_options.get_all_options()

    assert all_options["runner"] == "DirectRunner"
    assert all_options["project"] == "test-project"
    assert all_options["streaming"] is True

    assert all_options.get("region") is None
    assert all_options.get("temp_location") is None
    assert all_options.get("setup_file") is None

    mock_pipeline_instance | MagicMock(spec=PubsubToCloudMonitoringPipeline)


@patch("beametrics.pipeline_factory.Pipeline")
def test_run_with_unsupported_runner(mock_pipeline):
    """
    Test pipeline with unsupported runner
    """
    with pytest.raises(ValueError) as exc_info:
        run(
            project_id="test-project",
            subscription="projects/test-project/subscriptions/test-subscription",
            metric_labels='{"service": "test-service"}',
            metric_name="test-metric",
            filter_conditions='[{"field": "severity", "value": "ERROR", "operator": "equals"}]',
            region="us-central1",
            temp_location="gs://test-bucket/temp",
            runner="UnsupportedRunner",
            export_type="monitoring",
        )

    assert "Unsupported runner type: UnsupportedRunner" in str(exc_info.value)


@patch("beametrics.pipeline_factory.Pipeline")
def test_run_with_unsupported_export_type(mock_pipeline):
    """
    Test pipeline with unsupported export type
    """
    with pytest.raises(ValueError) as exc_info:
        run(
            project_id="test-project",
            subscription="projects/test-project/subscriptions/test-subscription",
            metric_labels='{"service": "test-service"}',
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
        metric_labels={"service": "test-service"},
        project_id="test-project",
        export_type="monitoring",
    )

    assert isinstance(config, GoogleCloudMetricsConfig)
    assert config.metric_name == "custom.googleapis.com/test-metric"
    assert config.metric_labels == {"service": "test-service"}
    assert isinstance(config.connection_config, GoogleCloudConnectionConfig)
    assert config.connection_config.project_id == "test-project"


def test_create_metrics_config_with_unsupported_type():
    """Test metrics config creation with unsupported export type"""
    with pytest.raises(ValueError) as exc_info:
        create_metrics_config(
            metric_name="test-metric",
            metric_labels={"service": "test-service"},
            project_id="test-project",
            export_type="unsupported",
        )

    assert "Unsupported export type: unsupported" in str(exc_info.value)


@patch("beametrics.main.Pipeline")
def test_run_with_sum_metric(mock_pipeline):
    """Test pipeline with SUM metric type"""
    mock_pipeline_instance = MagicMock()
    mock_pipeline.return_value.__enter__.return_value = mock_pipeline_instance

    run(
        project_id="test-project",
        subscription="projects/test-project/subscriptions/test-subscription",
        metric_labels='{"service": "test-service"}',
        metric_name="test-metric",
        metric_type="sum",
        metric_field="response_time",  # Required for SUM metric
        filter_conditions='[{"field": "severity", "value": "ERROR", "operator": "equals"}]',
        runner="DirectRunner",
        export_type="monitoring",
    )

    mock_pipeline.assert_called_once()
    pipeline_options = mock_pipeline.call_args[1]["options"]
    all_options = pipeline_options.get_all_options()

    assert all_options["runner"] == "DirectRunner"
    assert all_options["project"] == "test-project"
    assert all_options["streaming"] is True

    mock_pipeline_instance | MagicMock(spec=PubsubToCloudMonitoringPipeline)


@patch("beametrics.main.Pipeline")
def test_run_with_invalid_metric_type(mock_pipeline):
    """Test pipeline with invalid metric type"""
    with pytest.raises(ValueError) as exc_info:
        run(
            project_id="test-project",
            subscription="projects/test-project/subscriptions/test-subscription",
            metric_labels='{"service": "test-service"}',
            metric_name="test-metric",
            metric_type="invalid_type",  # Invalid metric type
            filter_conditions='[{"field": "severity", "value": "ERROR", "operator": "equals"}]',
            runner="DirectRunner",
            export_type="monitoring",
        )
    assert "Unsupported metric type: invalid_type" in str(exc_info.value)


@patch("beametrics.main.Pipeline")
def test_run_without_required_field(mock_pipeline):
    """Test pipeline without required field for SUM metric"""
    with pytest.raises(ValueError) as exc_info:
        run(
            project_id="test-project",
            subscription="projects/test-project/subscriptions/test-subscription",
            metric_labels='{"service": "test-service"}',
            metric_name="test-metric",
            metric_type="sum",  # SUM requires field
            filter_conditions='[{"field": "severity", "value": "ERROR", "operator": "equals"}]',
            runner="DirectRunner",
            export_type="monitoring",
        )
    assert "field is required for sum metric type" in str(exc_info.value)


@patch("beametrics.main.Pipeline")
def test_run_with_flex_template(mock_pipeline):
    """Test pipeline with Flex Template type"""
    mock_pipeline_instance = MagicMock()
    mock_pipeline.return_value.__enter__.return_value = mock_pipeline_instance

    run(
        project_id="test-project",
        subscription="projects/test-project/subscriptions/test-subscription",
        metric_labels='{"service": "test-service"}',
        metric_name="test-metric",
        filter_conditions='[{"field": "severity", "value": "ERROR", "operator": "equals"}]',
        runner="DataflowRunner",
        export_type="monitoring",
        dataflow_template_type="flex",
        region="us-central1",
        temp_location="gs://test-bucket/temp",
    )

    mock_pipeline.assert_called_once()
    pipeline_options = mock_pipeline.call_args[1]["options"]
    expected_options = [
        "--runner=DataflowRunner",
        "--project=test-project",
        "--streaming",
        "--region=us-central1",
        "--temp_location=gs://test-bucket/temp",
    ]
    assert pipeline_options.get_all_options()["runner"] == "DataflowRunner"
    assert pipeline_options.get_all_options()["project"] == "test-project"
    assert pipeline_options.get_all_options()["streaming"] is True
    assert pipeline_options.get_all_options()["region"] == "us-central1"
    assert (
        pipeline_options.get_all_options()["temp_location"] == "gs://test-bucket/temp"
    )
    assert "--setup_file=./setup.py" not in expected_options
