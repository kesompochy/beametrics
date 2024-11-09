import unittest
from unittest.mock import MagicMock, patch

import apache_beam as beam
import pytest
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import RuntimeValueProvider, StaticValueProvider
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import FixedWindows, IntervalWindow, WindowFn
from apache_beam.utils.timestamp import Timestamp

from beametrics.filter import FilterCondition
from beametrics.metrics import MetricDefinition, MetricType
from beametrics.metrics_exporter import (
    GoogleCloudConnectionConfig,
    GoogleCloudMetricsConfig,
)
from beametrics.pipeline import (
    DecodeAndParse,
    DynamicFixedWindows,
    ExtractField,
    MessagesToMetricsPipeline,
    MetricTypeRouter,
    parse_json,
)


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

    inputs_str = '{"severity": "ERROR", "message": "テスト"}'  # Shift-JIS
    inputs_bytes = inputs_str.encode("shift-jis")
    result = parse_json(inputs_bytes)
    assert result["message"] == "テスト"

    with pytest.raises(ValueError) as exc_info:
        parse_json(b"invalid json data")
    assert "Failed to decode message" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        parse_json(b"\xFF\xFF\xFF")
    assert "Failed to decode message" in str(exc_info.value)


def test_beametrics_pipeline_structure():
    """Test MessagesToMetricsPipeline basic structure"""
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
        pipeline = MessagesToMetricsPipeline(
            filter_condition,
            metrics_config,
            metric_definition,
            window_size=60,
            export_type="google-cloud-monitoring",
        )
        pipeline.expand(mock_pcoll)

        # Assert
        assert mock_window.called
        assert mock_pardo.called
        assert mock_filter.called


@patch("beametrics.metrics_exporter.ExportMetrics")
def test_count_metric_aggregation(mock_export):
    """Test COUNT metric aggregation"""
    with TestPipeline(
        options=PipelineOptions(
            [
                "--metric-name=test-metric",
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


@patch("beametrics.metrics_exporter.ExportMetrics")
def test_sum_metric_aggregation(mock_export):
    """Test SUM metric aggregation"""
    with TestPipeline(
        options=PipelineOptions(
            [
                "--metric-name=test-metric",
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
    pipeline = MessagesToMetricsPipeline(
        filter_conditions=[MockFilterCondition()],
        metrics_config=MockMetricsConfig(),
        metric_definition=MockMetricDefinition(),
        window_size=60,
        export_type="google-cloud-monitoring",
    )
    transform = pipeline._get_window_transform()
    assert isinstance(transform.windowing.windowfn, DynamicFixedWindows)
    assert transform.windowing.windowfn.size == 60

    pipeline = MessagesToMetricsPipeline(
        filter_conditions=[MockFilterCondition()],
        metrics_config=MockMetricsConfig(),
        metric_definition=MockMetricDefinition(),
        window_size=120,
        export_type="google-cloud-monitoring",
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
        pipeline = MessagesToMetricsPipeline(
            filter_condition,
            metrics_config,
            metric_definition,
            window_size=60,
            export_type="google-cloud-monitoring",
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

    pipeline = MessagesToMetricsPipeline(
        filter_conditions=[MockFilterCondition()],
        metrics_config=MockMetricsConfig(),
        metric_definition=metric_definition,
        window_size=300,
        export_type="google-cloud-monitoring",
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

    pipeline = MessagesToMetricsPipeline(
        filter_conditions=[MockFilterCondition()],
        metrics_config=MockMetricsConfig(),
        metric_definition=metric_definition,
        window_size=300,
        export_type="google-cloud-monitoring",
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


class TestDynamicFixedWindows(unittest.TestCase):
    def test_with_static_value_provider(self):
        window_size = StaticValueProvider(int, 60)
        windows = DynamicFixedWindows(window_size)

        context = WindowFn.AssignContext(Timestamp(1234567890))
        assigned = windows.assign(context)

        self.assertEqual(len(assigned), 1)
        window = assigned[0]
        self.assertIsInstance(window, IntervalWindow)
        self.assertEqual(window.end - window.start, 60)

    def test_with_runtime_value_provider(self):
        window_size = RuntimeValueProvider(
            option_name="window_size", value_type=int, default_value=60
        )
        windows = DynamicFixedWindows(window_size)

        RuntimeValueProvider.set_runtime_options({"window_size": 120})

        context = WindowFn.AssignContext(Timestamp(1234567890))
        assigned = windows.assign(context)

        self.assertEqual(len(assigned), 1)
        window = assigned[0]
        self.assertIsInstance(window, IntervalWindow)
        self.assertEqual(window.end - window.start, 120)

    def test_invalid_window_size(self):
        window_size = StaticValueProvider(int, 0)
        windows = DynamicFixedWindows(window_size)

        context = WindowFn.AssignContext(Timestamp(1234567890))
        with self.assertRaises(ValueError):
            windows.assign(context)
        context = WindowFn.AssignContext(Timestamp(1234567890))
        with self.assertRaises(ValueError):
            windows.assign(context)

    def test_non_integer_window_size(self):
        window_size = StaticValueProvider(str, "not_a_number")
        windows = DynamicFixedWindows(window_size)

        context = WindowFn.AssignContext(Timestamp(1234567890))
        with self.assertRaises(ValueError) as cm:
            windows.assign(context)
        self.assertEqual(str(cm.exception), "Window size must be an integer")

    def test_string_integer_window_size(self):
        window_size = StaticValueProvider(str, "60")
        windows = DynamicFixedWindows(window_size)

        context = WindowFn.AssignContext(Timestamp(1234567890))
        assigned = windows.assign(context)

        self.assertEqual(len(assigned), 1)
        window = assigned[0]
        self.assertIsInstance(window, IntervalWindow)
        self.assertEqual(window.end - window.start, 60)


def test_decode_and_parse_dofn():
    """Test DecodeAndParse DoFn"""
    dofn = DecodeAndParse()

    valid_input = b'{"severity": "ERROR", "message": "test"}'
    result = list(dofn.process(valid_input))
    assert result == [{"severity": "ERROR", "message": "test"}]

    invalid_json = b"invalid json data"
    result = list(dofn.process(invalid_json))
    assert result == []

    invalid_encoding = b"\xa1\xa1\xa1invalid"
    result = list(dofn.process(invalid_encoding))
    assert result == []


def test_extract_field_dofn():
    """Test ExtractField DoFn"""
    dofn = ExtractField(field="count")

    valid_input = {"count": 10}
    result = list(dofn.process(valid_input))
    assert result == [10.0]

    missing_field = {"other_field": 10}
    result = list(dofn.process(missing_field))
    assert result == []

    invalid_type = {"count": "not a number"}
    result = list(dofn.process(invalid_type))
    assert result == []

    none_value = {"count": None}
    result = list(dofn.process(none_value))
    assert result == []


def test_metric_type_router_dofn():
    """Test MetricTypeRouter DoFn"""
    dofn = MetricTypeRouter(metric_type=MetricType.COUNT, field="value")
    input_data = {"field": "test"}
    result = list(dofn.process(input_data))
    assert result == [input_data]

    dofn = MetricTypeRouter(
        metric_type=StaticValueProvider(str, "COUNT"), field="value"
    )
    result = list(dofn.process(input_data))
    assert result == [input_data]

    dofn = MetricTypeRouter(metric_type="SUM", field="value")
    input_data = {"value": 10}
    result = list(dofn.process(input_data))
    assert result == [10.0]

    input_data = {"other_field": 10}
    result = list(dofn.process(input_data))
    assert result == [0.0]

    input_data = {"value": "not a number"}
    result = list(dofn.process(input_data))
    assert result == [0.0]
