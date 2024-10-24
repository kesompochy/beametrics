from pubsub_to_metrics.pipeline import PubsubToCloudMonitoringPipeline, parse_json
from pubsub_to_metrics.filter import FilterCondition
from pubsub_to_metrics.metrics_exporter import (
    GoogleCloudMetricsConfig,
    GoogleCloudConnectionConfig,
)
from unittest.mock import MagicMock, patch


def test_parse_json():
    """Test JSON parsing function"""
    input_bytes = '{"severity": "ERROR", "message": "test error"}'.encode("utf-8")
    result = parse_json(input_bytes)
    assert result["severity"] == "ERROR"
    assert result["message"] == "test error"


def test_pubsub_to_metrics_pipeline():
    """Test PubsubToMetricsPipeline transformation"""
    filter_condition = FilterCondition(
        field="severity", value="ERROR", operator="equals"
    )

    metrics_config = GoogleCloudMetricsConfig(
        metric_name="custom.googleapis.com/pubsub/error_count",
        labels={"service": "test"},
        connection_config=GoogleCloudConnectionConfig(project_id="test-project"),
    )

    # Mock the beam transforms
    with patch("apache_beam.ParDo") as mock_pardo, patch(
        "apache_beam.Filter"
    ) as mock_filter, patch("apache_beam.WindowInto") as mock_window:

        mock_pcoll = MagicMock()
        mock_window_result = MagicMock()
        mock_pardo_result = MagicMock()
        mock_filter_result = MagicMock()

        # Setup the pipeline chain
        mock_pcoll.__or__.return_value = mock_window_result
        mock_window_result.__or__.return_value = mock_pardo_result
        mock_pardo_result.__or__.return_value = mock_filter_result

        # Act
        pipeline = PubsubToCloudMonitoringPipeline(filter_condition, metrics_config)
        pipeline.expand(mock_pcoll)

        # Assert
        assert mock_window.called
        assert mock_pardo.called
        assert mock_filter.called
