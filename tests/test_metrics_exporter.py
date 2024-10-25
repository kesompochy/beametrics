from beametrics.metrics_exporter import (
    GoogleCloudConnectionConfig,
    GoogleCloudMetricsConfig,
    GoogleCloudMetricsExporter,
)
from unittest.mock import patch


def test_google_cloud_connection_config():
    """
    Test GoogleCloudConnectionConfig initialization
    """
    config = GoogleCloudConnectionConfig(project_id="test-project")
    assert config.project_id == "test-project"


def test_metrics_config_with_google_cloud_connection_config():
    """
    Test MetricsConfig with GoogleCloudConnectionConfig
    """
    config = GoogleCloudMetricsConfig(
        metric_name="custom.googleapis.com/pubsub/error_count",
        metric_labels={"service": "api"},
        connection_config=GoogleCloudConnectionConfig(project_id="test-project"),
    )

    assert config.metric_name == "custom.googleapis.com/pubsub/error_count"
    assert config.metric_labels == {"service": "api"}
    assert config.connection_config.project_id == "test-project"


def test_google_cloud_metrics_exporter():
    """
    Test GoogleCloudMetricsExporter
    """
    config = GoogleCloudMetricsConfig(
        metric_name="custom.googleapis.com/pubsub/error_count",
        metric_labels={"service": "api"},
        connection_config=GoogleCloudConnectionConfig(project_id="test-project"),
    )
    with patch("google.cloud.monitoring_v3.MetricServiceClient") as mock_client:
        exporter = GoogleCloudMetricsExporter(config)
        exporter.export(1)

        mock_client.return_value.create_time_series.assert_called_once()


def test_google_cloud_metrics_exporter_parameters():
    """
    Test GoogleCloudMetricsExporter passes correct parameters
    """
    config = GoogleCloudMetricsConfig(
        metric_name="custom.googleapis.com/pubsub/error_count",
        metric_labels={"service": "api"},
        connection_config=GoogleCloudConnectionConfig(project_id="test-project"),
    )
    with patch("google.cloud.monitoring_v3.MetricServiceClient") as mock_client:
        exporter = GoogleCloudMetricsExporter(config)
        exporter.export(1.0)

        mock_client.return_value.create_time_series.assert_called_once()
        call_args = mock_client.return_value.create_time_series.call_args[1]

        # Get properties directly from CreateTimeSeriesRequest object
        request = call_args["request"]
        assert request.name == "projects/test-project"

        time_series = request.time_series[0]
        assert time_series.metric.type == config.metric_name
        assert time_series.metric.labels == config.metric_labels
        assert time_series.resource.type == "global"
        assert time_series.points[0].value.double_value == 1.0
        assert time_series.points[0].interval.end_time.timestamp() > 0
