from pubsub_to_metrics.metrics_publisher import (
    GoogleCloudConnectionConfig,
    GoogleCloudMetricsConfig,
    GoogleCloudMetricsPublisher,
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
        labels={"service": "api"},
        connection_config=GoogleCloudConnectionConfig(project_id="test-project"),
    )

    assert config.metric_name == "custom.googleapis.com/pubsub/error_count"
    assert config.labels == {"service": "api"}
    assert config.connection_config.project_id == "test-project"


def test_google_cloud_metrics_publisher():
    """
    Test GoogleCloudMetricsPublisher
    """
    config = GoogleCloudMetricsConfig(
        metric_name="custom.googleapis.com/pubsub/error_count",
        labels={"service": "api"},
        connection_config=GoogleCloudConnectionConfig(project_id="test-project"),
    )
    with patch("google.cloud.monitoring_v3.MetricServiceClient") as mock_client:
        publisher = GoogleCloudMetricsPublisher(config)
        publisher.publish(1)

        mock_client.return_value.create_time_series.assert_called_once()


def test_google_cloud_metrics_publisher_parameters():
    """
    Test GoogleCloudMetricsPublisher passes correct parameters
    """
    config = GoogleCloudMetricsConfig(
        metric_name="custom.googleapis.com/pubsub/error_count",
        labels={"service": "api"},
        connection_config=GoogleCloudConnectionConfig(project_id="test-project"),
    )
    with patch("google.cloud.monitoring_v3.MetricServiceClient") as mock_client:
        publisher = GoogleCloudMetricsPublisher(config)
        publisher.publish(1.0)

        mock_client.return_value.create_time_series.assert_called_once()
        call_args = mock_client.return_value.create_time_series.call_args[1]

        # Get properties directly from CreateTimeSeriesRequest object
        request = call_args["request"]
        assert request.name == "projects/test-project"

        time_series = request.time_series[0]
        assert time_series.metric.type == config.metric_name
        assert time_series.metric.labels == config.labels
        assert time_series.resource.type == "global"
        assert time_series.points[0].value.double_value == 1.0
        assert time_series.points[0].interval.end_time.timestamp() > 0
