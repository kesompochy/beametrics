from unittest.mock import MagicMock, patch

import apache_beam as beam
import pytest
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import FixedWindows

from beametrics.filter import FilterCondition
from beametrics.metrics import MetricDefinition, MetricType
from beametrics.metrics_exporter import (
    GoogleCloudConnectionConfig,
    GoogleCloudMetricsConfig,
)
from beametrics.pipeline import PubsubToCloudMonitoringPipeline, parse_json


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


def test_beametrics_pipeline_structure():
    """Test PubsubToMetricsPipeline basic structure"""
    filter_condition = FilterCondition(
        field="severity", value="ERROR", operator="equals"
    )

    metrics_config = GoogleCloudMetricsConfig(
        metric_name="custom.googleapis.com/pubsub/error_count",
        metric_labels={"service": "test"},
        connection_config=GoogleCloudConnectionConfig(project_id="test-project"),
    )

    metric_definition = MetricDefinition(
        name="error_count",
        type=MetricType.COUNT,
        field=None,
        metric_labels={"service": "test"},
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
            filter_condition, metrics_config, metric_definition, window_size=60
        )
        pipeline.expand(mock_pcoll)

        # Assert
        assert mock_window.called
        assert mock_pardo.called
        assert mock_filter.called


@patch("beametrics.pipeline.ExportMetricsToCloudMonitoring")
def test_count_metric_aggregation(mock_export):
    """Test COUNT metric aggregation"""
    with TestPipeline(
        options=PipelineOptions(
            [
                "--export-metric-name=test-metric",
                "--subscription=projects/test-project/subscriptions/test-sub",
                '--metric-labels={"service": "test"}',
                '--filter-conditions=[{"field": "severity", "value": "ERROR"}]',
            ]
        )
    ) as p:
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


@patch("beametrics.pipeline.ExportMetricsToCloudMonitoring")
def test_sum_metric_aggregation(mock_export):
    """Test SUM metric aggregation"""
    with TestPipeline(
        options=PipelineOptions(
            [
                "--export-metric-name=test-metric",
                "--subscription=projects/test-project/subscriptions/test-sub",
                '--metric-labels={"service": "test"}',
                '--filter-conditions=[{"field": "severity", "value": "ERROR"}]',
                "--metric-type=sum",
                "--metric-field=response_time",
            ]
        )
    ) as p:
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
            metric_labels={"service": "test"},
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
            metric_labels={"service": "test"},
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


def test_beametrics_pipeline_with_runtime_value_provider():
    """Test pipeline with ValueProvider for metric type"""
    from apache_beam.options.value_provider import StaticValueProvider

    metric_definition = MetricDefinition(
        name="error_count",
        type=StaticValueProvider(str, "count"),
        field=None,
        metric_labels={"service": "test"},
    )

    metrics_config = GoogleCloudMetricsConfig(
        metric_name="custom.googleapis.com/pubsub/error_count",
        metric_labels={"service": "test"},
        connection_config=GoogleCloudConnectionConfig(project_id="test-project"),
    )

    filter_condition = FilterCondition(
        field="severity", value="ERROR", operator="equals"
    )

    with patch("apache_beam.ParDo") as mock_pardo, patch(
        "apache_beam.Filter"
    ) as mock_filter, patch("apache_beam.WindowInto") as mock_window:

        mock_pcoll = MagicMock()
        pipeline = PubsubToCloudMonitoringPipeline(
            filter_condition, metrics_config, metric_definition, window_size=60
        )
        pipeline.expand(mock_pcoll)

        assert mock_pardo.called
        assert mock_filter.called
        assert mock_window.called


def test_beametrics_pipeline_with_deferred_value_resolution():
    """Verify that pipeline construction does not resolve ValueProvider values"""
    from apache_beam.options.value_provider import RuntimeValueProvider

    class UnresolvedValueProvider(RuntimeValueProvider):
        def get(self):
            raise ValueError("ValueProvider accessed during graph construction")

    metric_definition = MetricDefinition(
        name="error_count",
        type=UnresolvedValueProvider(
            option_name="metric_type", value_type=str, default_value="count"
        ),
        field=None,
        metric_labels={"service": "test"},
    )

    pipeline = PubsubToCloudMonitoringPipeline(
        filter_conditions=[MockFilterCondition()],
        metrics_config=MockMetricsConfig(),
        metric_definition=metric_definition,
        window_size=300,
    )

    result = pipeline.expand(MagicMock())


def test_deferred_metric_combiner_with_dict_input():
    """Test DeferredMetricCombiner handles dict inputs correctly"""
    from apache_beam.options.value_provider import StaticValueProvider

    metric_definition = MetricDefinition(
        name="error_count",
        type=StaticValueProvider(str, "count"),
        field=None,
        metric_labels={"service": "test"},
    )

    pipeline = PubsubToCloudMonitoringPipeline(
        filter_conditions=[MockFilterCondition()],
        metrics_config=MockMetricsConfig(),
        metric_definition=metric_definition,
        window_size=300,
    )

    combiner = pipeline._get_combiner()
    acc = combiner.create_accumulator()

    dict_input = {"field": "value"}
    acc = combiner.add_input(acc, dict_input)
    assert acc == 1

    acc = combiner.add_input(acc, {"another": "value"})
    assert acc == 2

    acc = combiner.add_input(acc, 5)
    assert acc == 7
