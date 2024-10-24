from pubsub_to_metrics.pipeline import PubsubToCloudMonitoringPipeline, parse_json
from pubsub_to_metrics.filter import FilterCondition
from pubsub_to_metrics.metrics_exporter import (
    GoogleCloudMetricsConfig,
    GoogleCloudConnectionConfig,
)
from unittest.mock import MagicMock, patch
from pubsub_to_metrics.metrics import MetricType, MetricDefinition
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
import apache_beam as beam
import pytest
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows


class TestMetricsExporter(beam.DoFn):
    """Test metrics exporter that stores values for verification"""

    def __init__(self):
        super().__init__()
        self.exported_values = []

    def process(self, value):
        self.exported_values.append(value)
        yield value


def test_parse_json():
    """Test JSON parsing function"""
    input_bytes = '{"severity": "ERROR", "message": "test error"}'.encode("utf-8")
    result = parse_json(input_bytes)
    assert result["severity"] == "ERROR"
    assert result["message"] == "test error"


def test_pubsub_to_metrics_pipeline_structure():
    """Test PubsubToMetricsPipeline basic structure"""
    filter_condition = FilterCondition(
        field="severity", value="ERROR", operator="equals"
    )

    metrics_config = GoogleCloudMetricsConfig(
        metric_name="custom.googleapis.com/pubsub/error_count",
        labels={"service": "test"},
        connection_config=GoogleCloudConnectionConfig(project_id="test-project"),
    )

    metric_definition = MetricDefinition(
        name="error_count",
        type=MetricType.COUNT,
        field=None,
        labels={"service": "test"},
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
        pipeline = PubsubToCloudMonitoringPipeline(
            filter_condition, metrics_config, metric_definition
        )
        pipeline.expand(mock_pcoll)

        # Assert
        assert mock_window.called
        assert mock_pardo.called
        assert mock_filter.called


@patch("pubsub_to_metrics.pipeline.ExportMetricsToCloudMonitoring")
def test_count_metric_aggregation(mock_export):
    """Test COUNT metric aggregation"""
    with TestPipeline() as p:
        input_data = [
            b'{"severity": "ERROR", "message": "test1"}',
            b'{"severity": "ERROR", "message": "test2"}',
            b'{"severity": "INFO", "message": "test3"}',
        ]

        result = (
            p
            | beam.Create(input_data)
            | beam.Map(parse_json)
            | beam.Filter(lambda x: x["severity"] == "ERROR")
            | beam.CombineGlobally(beam.combiners.CountCombineFn()).without_defaults()
        )

        assert_that(result, equal_to([2]))


@patch("pubsub_to_metrics.pipeline.ExportMetricsToCloudMonitoring")
def test_sum_metric_aggregation(mock_export):
    """Test SUM metric aggregation"""
    with TestPipeline() as p:
        input_data = [
            b'{"severity": "ERROR", "bytes": 100}',
            b'{"severity": "ERROR", "bytes": 150}',
            b'{"severity": "INFO", "bytes": 200}',
        ]

        result = (
            p
            | beam.Create(input_data)
            | beam.Map(parse_json)
            | beam.Filter(lambda x: x["severity"] == "ERROR")
            | beam.Map(lambda x: x["bytes"])
            | beam.CombineGlobally(sum).without_defaults()
        )

        assert_that(result, equal_to([250]))


class MockFilterCondition(FilterCondition):
    def __init__(self):
        super().__init__(field="severity", value="ERROR", operator="equals")


class MockMetricsConfig(GoogleCloudMetricsConfig):
    def __init__(self):
        super().__init__(
            metric_name="test-metric",
            labels={"service": "test"},
            connection_config=MockConnectionConfig(),
        )


class MockConnectionConfig:
    def __init__(self):
        self.project_name = "projects/test-project"


class MockMetricDefinition(MetricDefinition):
    def __init__(self):
        super().__init__(
            name="test-metric",
            type=MetricType.COUNT,
            field=None,
            labels={"service": "test"},
        )


def test_fixed_window_size_validation():
    """Test fixed window size validation"""
    pipeline = PubsubToCloudMonitoringPipeline(
        filter_conditions=[MockFilterCondition()],
        metrics_config=MockMetricsConfig(),
        metric_definition=MockMetricDefinition(),
        window_size=60,
    )
    transform = pipeline._get_window_transform()
    assert isinstance(transform.windowing.windowfn, FixedWindows)
    assert transform.windowing.windowfn.size == 60

    pipeline = PubsubToCloudMonitoringPipeline(
        filter_conditions=[MockFilterCondition()],
        metrics_config=MockMetricsConfig(),
        metric_definition=MockMetricDefinition(),
        window_size=120,
    )
    transform = pipeline._get_window_transform()
    assert transform.windowing.windowfn.size == 120

    with pytest.raises(ValueError) as exc_info:
        PubsubToCloudMonitoringPipeline(
            filter_conditions=[MockFilterCondition()],
            metrics_config=MockMetricsConfig(),
            metric_definition=MockMetricDefinition(),
            window_size=59,
        )
    assert "window_size must be at least 60 seconds" in str(exc_info.value)
