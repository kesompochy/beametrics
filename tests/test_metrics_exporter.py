import json
from unittest.mock import Mock, patch

import pytest

from beametrics.metrics_exporter import (
    ExportMetrics,
    GoogleCloudConnectionConfig,
    GoogleCloudExporterConfig,
    GoogleCloudMetricsExporter,
    LocalExporterConfig,
    LocalMetricsExporter,
    MetricsExporterFactory,
)


def test_create_exporter_monitoring():
    config = GoogleCloudExporterConfig(
        metric_name="custom.googleapis.com/test",
        metric_labels={},
        connection_config=GoogleCloudConnectionConfig(project_id="test-project"),
    )

    with patch("google.cloud.monitoring_v3.MetricServiceClient") as mock_client:
        mock_client.return_value = Mock()
        exporter = MetricsExporterFactory.create_exporter(config)
        assert exporter.__class__.__name__ == "GoogleCloudMetricsExporter"


def test_create_exporter_invalid_config():
    config = "invalid_config"
    with pytest.raises(ValueError, match="Invalid config type for metrics exporter"):
        MetricsExporterFactory.create_exporter(config)


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
    config = GoogleCloudExporterConfig(
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
    config = GoogleCloudExporterConfig(
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
    config = GoogleCloudExporterConfig(
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
        assert time_series.points[0].interval.end_time.timestamp() > 0


def test_export_metrics():
    """Test ExportMetrics DoFn"""
    config = GoogleCloudExporterConfig(
        metric_name="custom.googleapis.com/test",
        metric_labels={},
        connection_config=GoogleCloudConnectionConfig(project_id="test-project"),
    )

    input_element = {"labels": {}, "value": 1.0}

    with patch("google.cloud.monitoring_v3.MetricServiceClient"):
        dofn = ExportMetrics(config)
        dofn.setup()
        result = list(dofn.process(input_element))
        assert result == [input_element]

    with patch("google.cloud.monitoring_v3.MetricServiceClient") as mock_client:
        mock_client.return_value.create_time_series.side_effect = Exception(
            "Export failed"
        )
        dofn = ExportMetrics(config)
        dofn.setup()
        result = list(dofn.process(input_element))
        assert result == [input_element]


def test_google_cloud_metrics_exporter_with_dynamic_labels():
    """Test GoogleCloudMetricsExporter with dynamic labels"""
    config = GoogleCloudExporterConfig(
        metric_name="custom.googleapis.com/pubsub/error_count",
        metric_labels={"service": "api"},
        connection_config=GoogleCloudConnectionConfig(project_id="test-project"),
    )
    with patch("google.cloud.monitoring_v3.MetricServiceClient") as mock_client:
        exporter = GoogleCloudMetricsExporter(config)
        exporter.export(1.0, dynamic_labels={"region": "us-east1"})

        mock_client.return_value.create_time_series.assert_called_once()
        call_args = mock_client.return_value.create_time_series.call_args[1]
        request = call_args["request"]

        time_series = request.time_series[0]
        assert time_series.metric.labels == {"service": "api", "region": "us-east1"}


def test_create_exporter_local():
    """Test creating local exporter"""
    config = LocalExporterConfig(
        metric_name="test_metric",
        metric_labels={},
        connection_config=GoogleCloudConnectionConfig(project_id="test-project"),
    )

    exporter = MetricsExporterFactory.create_exporter(config)
    assert exporter.__class__.__name__ == "LocalMetricsExporter"


def test_local_metrics_exporter():
    """Test LocalMetricsExporter exports metrics correctly"""
    config = LocalExporterConfig(
        metric_name="test_metric",
        metric_labels={"service": "test"},
        connection_config=GoogleCloudConnectionConfig(project_id="test-project"),
    )

    with patch("builtins.print") as mock_print:
        exporter = LocalMetricsExporter(config)
        exporter.export(1.0, dynamic_labels={"region": "test-region"})

        mock_print.assert_called_once()
        output = json.loads(mock_print.call_args[0][0])
        assert output["metric_name"] == "test_metric"
        assert output["value"] == 1.0
        assert output["labels"] == {"service": "test", "region": "test-region"}
        assert "timestamp" in output
