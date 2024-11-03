from unittest.mock import MagicMock, patch

import pytest

from beametrics.filter import FilterCondition
from beametrics.main import (
    BeametricsOptions,
    create_metrics_config,
    parse_filter_conditions,
    run,
)
from beametrics.metrics_exporter import (
    GoogleCloudConnectionConfig,
    GoogleCloudMetricsConfig,
)
from beametrics.pipeline import MessagesToMetricsPipeline


def test_parse_filter_conditions():
    """Test parsing a valid filter condition from JSON string"""
    json_str = '[{"field": "severity", "value": "ERROR", "operator": "equals"}]'
    conditions = parse_filter_conditions(json_str)

    assert isinstance(conditions, list)
    assert len(conditions) == 1
    assert isinstance(conditions[0], FilterCondition)
    assert conditions[0].field == "severity"
    assert conditions[0].value == "ERROR"
    assert conditions[0].operator == "equals"


@patch("beametrics.main.Pipeline")
@patch("google.cloud.monitoring_v3.MetricServiceClient")
def test_run_with_dataflow_and_monitoring(mock_metrics_client, mock_pipeline):
    """Test pipeline with DataflowRunner and Cloud Monitoring export"""
    mock_pipeline_instance = MagicMock()
    mock_pipeline.return_value.__enter__.return_value = mock_pipeline_instance

    options = BeametricsOptions(
        [
            "--runner=DataflowRunner",
            "--project=test-project",
            "--region=us-central1",
            "--temp_location=gs://test-bucket/temp",
            "--export-metric-name=test-metric",
            "--subscription=projects/test-project/subscriptions/test-subscription",
            '--metric-labels={"service": "test-service"}',
            '--filter-conditions=[{"field": "severity", "value": "ERROR", "operator": "equals"}]',
            "--export-type=monitoring",
            "--metric-type=count",
        ]
    )

    run(options)

    mock_pipeline.assert_called_once()
    mock_pipeline_instance | MagicMock(spec=MessagesToMetricsPipeline)


@patch("beametrics.main.Pipeline")
def test_run_with_direct_and_monitoring(mock_pipeline):
    """Test pipeline with DirectRunner and Cloud Monitoring export"""
    mock_pipeline_instance = MagicMock()
    mock_pipeline.return_value.__enter__.return_value = mock_pipeline_instance

    options = BeametricsOptions(
        [
            "--runner=DirectRunner",
            "--project=test-project",
            "--export-metric-name=test-metric",
            "--subscription=projects/test-project/subscriptions/test-subscription",
            '--metric-labels={"service": "test-service"}',
            '--filter-conditions=[{"field": "severity", "value": "ERROR", "operator": "equals"}]',
            "--export-type=monitoring",
        ]
    )

    from apache_beam.options.value_provider import RuntimeValueProvider

    RuntimeValueProvider.set_runtime_options(options)
    try:
        run(options)
    finally:
        RuntimeValueProvider.set_runtime_options(None)

    mock_pipeline.assert_called_once()
    mock_pipeline_instance | MagicMock(spec=MessagesToMetricsPipeline)


@patch("beametrics.main.Pipeline")
def test_run_with_unsupported_runner(mock_pipeline):
    """Test pipeline with unsupported runner"""
    with pytest.raises(ValueError) as exc_info:
        options = BeametricsOptions(
            [
                "--runner=UnsupportedRunner",
                "--project=test-project",
                "--region=us-central1",
                "--temp_location=gs://test-bucket/temp",
                "--export-metric-name=test-metric",
                "--subscription=projects/test-project/subscriptions/test-subscription",
                '--metric-labels={"service": "test-service"}',
                '--filter-conditions=[{"field": "severity", "value": "ERROR", "operator": "equals"}]',
                "--export-type=monitoring",
            ]
        )
        run(options)

    assert "Unsupported runner type: UnsupportedRunner" in str(exc_info.value)


@patch("beametrics.main.Pipeline")
def test_run_with_unsupported_export_type(mock_pipeline):
    """Test pipeline with unsupported export type"""
    with pytest.raises(ValueError) as exc_info:
        options = BeametricsOptions(
            [
                "--runner=DataflowRunner",
                "--project=test-project",
                "--region=us-central1",
                "--temp_location=gs://test-bucket/temp",
                "--export-metric-name=test-metric",
                "--subscription=projects/test-project/subscriptions/test-subscription",
                '--metric-labels={"service": "test-service"}',
                '--filter-conditions=[{"field": "severity", "value": "ERROR", "operator": "equals"}]',
                "--export-type=unsupported",
            ]
        )
        run(options)

    assert "Unsupported export type: unsupported" in str(exc_info.value)


def test_create_metrics_config_for_monitoring():
    """Test metrics config creation for Cloud Monitoring"""
    config = create_metrics_config(
        export_metric_name="test-metric",
        metric_labels={"service": "test-service"},
        project_id="test-project",
        export_type="monitoring",
    )

    assert isinstance(config, GoogleCloudMetricsConfig)
    assert config.metric_name == "custom.googleapis.com/test-metric"
    assert config.metric_labels == {"service": "test-service"}
    assert isinstance(config.connection_config, GoogleCloudConnectionConfig)
    assert config.connection_config.project_id == "test-project"


@patch("beametrics.main.Pipeline")
def test_run_with_sum_metric(mock_pipeline):
    """Test pipeline with SUM metric type"""
    mock_pipeline_instance = MagicMock()
    mock_pipeline.return_value.__enter__.return_value = mock_pipeline_instance

    options = BeametricsOptions(
        [
            "--runner=DirectRunner",
            "--project=test-project",
            "--export-metric-name=test-metric",
            "--subscription=projects/test-project/subscriptions/test-subscription",
            '--metric-labels={"service": "test-service"}',
            '--filter-conditions=[{"field": "severity", "value": "ERROR", "operator": "equals"}]',
            "--metric-type=sum",
            "--metric-field=response_time",
            "--export-type=monitoring",
        ]
    )

    run(options)

    mock_pipeline.assert_called_once()
    mock_pipeline_instance | MagicMock(spec=MessagesToMetricsPipeline)


@patch("beametrics.main.Pipeline")
def test_run_with_invalid_metric_type(mock_pipeline):
    """Test pipeline with invalid metric type"""
    with pytest.raises(ValueError) as exc_info:
        options = BeametricsOptions(
            [
                "--runner=DirectRunner",
                "--project=test-project",
                "--export-metric-name=test-metric",
                "--subscription=projects/test-project/subscriptions/test-subscription",
                '--metric-labels={"service": "test-service"}',
                '--filter-conditions=[{"field": "severity", "value": "ERROR", "operator": "equals"}]',
                "--metric-type=invalid_type",
                "--export-type=monitoring",
            ]
        )
        run(options)

    assert "Unsupported metric type: invalid_type" in str(exc_info.value)


@patch("beametrics.main.Pipeline")
def test_run_without_required_field(mock_pipeline):
    """Test pipeline without required field for SUM metric"""
    with pytest.raises(ValueError) as exc_info:
        options = BeametricsOptions(
            [
                "--runner=DirectRunner",
                "--project=test-project",
                "--export-metric-name=test-metric",
                "--subscription=projects/test-project/subscriptions/test-subscription",
                '--metric-labels={"service": "test-service"}',
                '--filter-conditions=[{"field": "severity", "value": "ERROR", "operator": "equals"}]',
                "--metric-type=sum",
                "--export-type=monitoring",
            ]
        )
        run(options)

    assert "field is required for sum metric type" in str(exc_info.value)


@patch("beametrics.main.Pipeline")
def test_run_with_flex_template(mock_pipeline):
    """Test pipeline with Flex Template type"""
    mock_pipeline_instance = MagicMock()
    mock_pipeline.return_value.__enter__.return_value = mock_pipeline_instance

    options = BeametricsOptions(
        [
            "--runner=DataflowRunner",
            "--project=test-project",
            "--region=us-central1",
            "--temp_location=gs://test-bucket/temp",
            "--export-metric-name=test-metric",
            "--subscription=projects/test-project/subscriptions/test-subscription",
            '--metric-labels={"service": "test-service"}',
            '--filter-conditions=[{"field": "severity", "value": "ERROR", "operator": "equals"}]',
            "--export-type=monitoring",
            "--dataflow_template_type=flex",
            "--metrics-type=count",
        ]
    )

    from apache_beam.options.value_provider import RuntimeValueProvider

    runtime_options = {
        "metric_type": "count",
        "metric_labels": '{"service": "test-service"}',
    }
    RuntimeValueProvider.set_runtime_options(runtime_options)
    try:
        run(options)
    finally:
        RuntimeValueProvider.set_runtime_options(None)
