import unittest
from unittest.mock import MagicMock, patch

import apache_beam as beam
import pytest
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import (
    RuntimeValueProvider,
    StaticValueProvider,
    ValueProvider,
)
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
    MessagesToMetricsPipeline,
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


def test_pipeline_with_dynamic_labels():
    """Test pipeline with dynamic labels extraction"""
    with TestPipeline(
        options=PipelineOptions(
            [
                "--metric-name=test-metric",
                "--subscription=projects/test-project/subscriptions/test-sub",
                '--dynamic-labels={"region": "region"}',
                "--metric-field=count",
                '--filter-conditions=[{"field": "severity", "value": "ERROR"}]',
                '--metric-labels={"service": "test"}',
            ]
        )
    ) as p:
        input_data = [
            b'{"severity": "ERROR", "region": "us-east1", "count": 1}',
            b'{"severity": "ERROR", "region": "us-west1", "count": 1}',
            b'{"severity": "ERROR", "region": "us-east1", "count": 1}',
        ]

        filter_condition = FilterCondition(
            field="severity", value="ERROR", operator="equals"
        )

        metrics_config = GoogleCloudMetricsConfig(
            metric_name="custom.googleapis.com/test",
            metric_labels={"service": "test"},
            connection_config=GoogleCloudConnectionConfig(project_id="test-project"),
        )

        metric_definition = MetricDefinition(
            name="error_count",
            type=MetricType.COUNT,
            field=None,
            metric_labels={"service": "test"},
            dynamic_labels={"region": "region"},
        )

        test_exporter = TestMetricsExporter()

        with patch("beametrics.pipeline.ExportMetrics") as mock_export:
            mock_export.return_value = test_exporter  # TestMetricsExporterを使用

            result = (
                p
                | beam.Create(input_data)
                | MessagesToMetricsPipeline(
                    filter_conditions=[filter_condition],
                    metrics_config=metrics_config,
                    metric_definition=metric_definition,
                    window_size=60,
                )
            )

        expected_metrics = [
            {"value": 2, "labels": {"service": "test", "region": "us-east1"}},
            {"value": 1, "labels": {"service": "test", "region": "us-west1"}},
        ]

        assert_that(result, equal_to(expected_metrics))


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
    )
    transform = pipeline._get_window_transform()
    assert isinstance(transform.windowing.windowfn, DynamicFixedWindows)
    assert transform.windowing.windowfn.size == 60

    pipeline = MessagesToMetricsPipeline(
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
        pipeline = MessagesToMetricsPipeline(
            filter_condition,
            metrics_config,
            metric_definition,
            window_size=60,
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
    )

    result = pipeline.expand(MagicMock())


def test_metric_type_evaluation():
    """Test metric type evaluation with ValueProvider"""
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
    )

    msg = {"field": "value", "count": 100}
    result = pipeline._get_metric_type()
    assert result is True

    metric_definition = MetricDefinition(
        name="bytes_total",
        type=StaticValueProvider(str, "sum"),
        field="bytes",
        metric_labels={"service": "test"},
    )

    pipeline = MessagesToMetricsPipeline(
        filter_conditions=[MockFilterCondition()],
        metrics_config=MockMetricsConfig(),
        metric_definition=metric_definition,
        window_size=300,
    )

    msg = {"bytes": 100}
    result = pipeline._get_metric_type()
    assert result is False


def test_metric_type_late_evaluation():
    """Test that metric type is evaluated at runtime"""
    with patch("beametrics.pipeline.ExportMetrics") as mock_export:
        mock_export.return_value = TestMetricsExporter()
        RuntimeValueProvider.set_runtime_options(None)
        options = PipelineOptions(
            [
                "--metric-name=test-metric",
                "--subscription=projects/test-project/subscriptions/test-sub",
                '--metric-labels={"service": "test"}',
                '--filter-conditions=[{"field": "severity", "value": "ERROR"}]',
            ]
        )

        runtime_provider = RuntimeValueProvider(
            option_name="metric_type", value_type=str, default_value="sum"
        )

        metric_definition = MetricDefinition(
            name="test_metric",
            type=runtime_provider,
            field="value",
            metric_labels={"service": "test"},
        )

        pipeline = MessagesToMetricsPipeline(
            filter_conditions=[MockFilterCondition()],
            metrics_config=MockMetricsConfig(),
            metric_definition=metric_definition,
            window_size=300,
        )

        RuntimeValueProvider.set_runtime_options({"metric_type": "sum"})

        with TestPipeline(options=options) as p:
            input_data = [b'{"severity": "ERROR", "value": 100}']
            result = (
                p
                | beam.Create(input_data)
                | MessagesToMetricsPipeline(
                    filter_conditions=[MockFilterCondition()],
                    metrics_config=MockMetricsConfig(),
                    metric_definition=metric_definition,
                    window_size=300,
                )
            )
            assert_that(
                result, equal_to([{"labels": {"service": "test"}, "value": 100}])
            )


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
        assigned = windows.assign(context)
        assert len(assigned) == 1
        assert isinstance(assigned[0], IntervalWindow)
        assert assigned[0].end - assigned[0].start == 60

    def test_non_integer_window_size(self):
        window_size = StaticValueProvider(str, "not_a_number")
        windows = DynamicFixedWindows(window_size)

        context = WindowFn.AssignContext(Timestamp(1234567890))
        assigned = windows.assign(context)
        assert len(assigned) == 1
        assert isinstance(assigned[0], IntervalWindow)
        assert assigned[0].end - assigned[0].start == 60

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


def test_dynamic_fixed_windows_error_handling():
    """Test error handling in DynamicFixedWindows"""
    window_size = StaticValueProvider(int, 60)
    windows = DynamicFixedWindows(window_size)
    context = WindowFn.AssignContext(Timestamp(1234567890))
    result = windows.assign(context)
    assert len(result) == 1
    assert isinstance(result[0], IntervalWindow)
    assert result[0].end - result[0].start == 60

    class NoneValueProvider(ValueProvider):
        def get(self):
            return None

        def is_accessible(self):
            return True

    windows = DynamicFixedWindows(NoneValueProvider())
    result = windows.assign(context)
    assert result[0].end - result[0].start == windows.DEFAULT_WINDOW_SIZE

    windows = DynamicFixedWindows(StaticValueProvider(int, -10))
    result = windows.assign(context)
    assert result[0].end - result[0].start == windows.DEFAULT_WINDOW_SIZE

    windows = DynamicFixedWindows(StaticValueProvider(str, "invalid"))
    result = windows.assign(context)
    assert result[0].end - result[0].start == windows.DEFAULT_WINDOW_SIZE

    class ErrorValueProvider(ValueProvider):
        def get(self):
            raise RuntimeError("Failed to get value")

        def is_accessible(self):
            return True

    windows = DynamicFixedWindows(ErrorValueProvider())
    result = windows.assign(context)
    assert result[0].end - result[0].start == windows.DEFAULT_WINDOW_SIZE


def test_pipeline_with_sum_metric():
    """Test pipeline with SUM metric type"""
    with patch("beametrics.pipeline.ExportMetrics") as mock_export:
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
                b'{"severity": "ERROR", "region": "us-east1", "bytes": 100}',
                b'{"severity": "ERROR", "region": "us-west1", "bytes": 150}',
                b'{"severity": "ERROR", "region": "us-east1", "bytes": 200}',
            ]

            filter_condition = FilterCondition(
                field="severity", value="ERROR", operator="equals"
            )

            metrics_config = GoogleCloudMetricsConfig(
                metric_name="custom.googleapis.com/test",
                metric_labels={"service": "test"},
                connection_config=GoogleCloudConnectionConfig(
                    project_id="test-project"
                ),
            )

            metric_definition = MetricDefinition(
                name="bytes_total",
                type=MetricType.SUM,
                field="bytes",
                metric_labels={"service": "test"},
                dynamic_labels={"region": "region"},
            )

            test_exporter = TestMetricsExporter()
            mock_export.return_value = test_exporter

            result = (
                p
                | beam.Create(input_data)
                | MessagesToMetricsPipeline(
                    filter_conditions=[filter_condition],
                    metrics_config=metrics_config,
                    metric_definition=metric_definition,
                    window_size=60,
                )
            )

            expected_metrics = [
                {
                    "value": 300,
                    "labels": {"service": "test", "region": "us-east1"},
                },
                {"value": 150, "labels": {"service": "test", "region": "us-west1"}},
            ]

            assert_that(result, equal_to(expected_metrics))


def test_pipeline_with_none_metric_labels():
    """Test pipeline with None metric_labels"""
    with patch("beametrics.pipeline.ExportMetrics") as mock_export:
        with TestPipeline(
            options=PipelineOptions(
                [
                    "--metric-name=test-metric",
                    "--subscription=projects/test-project/subscriptions/test-sub",
                    '--filter-conditions=[{"field": "severity", "value": "ERROR"}]',
                ]
            )
        ) as p:
            input_data = [b'{"severity": "ERROR", "region": "us-east1", "count": 1}']

            filter_condition = FilterCondition(
                field="severity", value="ERROR", operator="equals"
            )

            metrics_config = GoogleCloudMetricsConfig(
                metric_name="custom.googleapis.com/test",
                metric_labels={},
                connection_config=GoogleCloudConnectionConfig(
                    project_id="test-project"
                ),
            )

            metric_definition = MetricDefinition(
                name="error_count",
                type=MetricType.COUNT,
                field=None,
                metric_labels=None,
            )

            test_exporter = TestMetricsExporter()
            mock_export.return_value = test_exporter

            result = (
                p
                | beam.Create(input_data)
                | MessagesToMetricsPipeline(
                    filter_conditions=[filter_condition],
                    metrics_config=metrics_config,
                    metric_definition=metric_definition,
                    window_size=60,
                )
            )

            expected_metrics = [{"value": 1, "labels": {}}]

            assert_that(result, equal_to(expected_metrics))


def test_pipeline_with_none_dynamic_labels():
    """Test pipeline with None dynamic_labels"""
    with TestPipeline(
        options=PipelineOptions(
            [
                "--metric-name=test-metric",
                "--subscription=projects/test-project/subscriptions/test-sub",
                '--filter-conditions=[{"field": "severity", "value": "ERROR"}]',
            ]
        )
    ) as p:
        input_data = [b'{"severity": "ERROR", "region": "us-east1", "count": 1}']

        filter_condition = FilterCondition(
            field="severity", value="ERROR", operator="equals"
        )

        metrics_config = GoogleCloudMetricsConfig(
            metric_name="custom.googleapis.com/test",
            metric_labels={"service": "test"},
            connection_config=GoogleCloudConnectionConfig(project_id="test-project"),
        )

        metric_definition = MetricDefinition(
            name="error_count",
            type=MetricType.COUNT,
            field=None,
            metric_labels={"service": "test"},
            dynamic_labels=None,
        )

        test_exporter = TestMetricsExporter()

        with patch("beametrics.pipeline.ExportMetrics") as mock_export:
            mock_export.return_value = test_exporter

            result = (
                p
                | beam.Create(input_data)
                | MessagesToMetricsPipeline(
                    filter_conditions=[filter_condition],
                    metrics_config=metrics_config,
                    metric_definition=metric_definition,
                    window_size=60,
                )
            )

            expected_metrics = [
                {"value": 1, "labels": {"service": "test"}},
            ]

            assert_that(result, equal_to(expected_metrics))


def test_pipeline_with_runtime_value_provider_and_none_dynamic_labels():
    """Test pipeline with RuntimeValueProvider and None dynamic_labels"""
    RuntimeValueProvider.set_runtime_options({"metric_type": "count"})

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
        input_data = [b'{"severity": "ERROR", "count": 1}']

        metric_definition = MetricDefinition(
            name="error_count",
            type=RuntimeValueProvider(
                option_name="metric_type", value_type=str, default_value="count"
            ),
            field=None,
            metric_labels={"service": "test"},
            dynamic_labels=None,
        )

        test_exporter = TestMetricsExporter()

        with patch("beametrics.pipeline.ExportMetrics") as mock_export:
            mock_export.return_value = test_exporter

            result = (
                p
                | beam.Create(input_data)
                | MessagesToMetricsPipeline(
                    filter_conditions=[MockFilterCondition()],
                    metrics_config=MockMetricsConfig(),
                    metric_definition=metric_definition,
                    window_size=60,
                )
            )

            expected_metrics = [{"value": 1, "labels": {"service": "test"}}]
            assert_that(result, equal_to(expected_metrics))
