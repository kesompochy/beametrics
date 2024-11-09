import logging
from typing import Any, Dict, Generator, List, Union

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.options.value_provider import StaticValueProvider, ValueProvider
from apache_beam.transforms.window import IntervalWindow, NonMergingWindowFn
from apache_beam.utils.timestamp import Duration

from beametrics.filter import FilterCondition, MessageFilter
from beametrics.metrics import MetricDefinition, MetricType
from beametrics.metrics_exporter import ExportMetrics, GoogleCloudMetricsConfig


class DynamicFixedWindows(NonMergingWindowFn):
    """A windowing function that assigns each element to one time interval,
    with a window size that can be determined at runtime.

    Args:
        window_size_provider: A ValueProvider that provides the size of the window in seconds.
    """

    DEFAULT_WINDOW_SIZE = 60

    def __init__(self, window_size_provider):
        super().__init__()
        if not isinstance(window_size_provider, ValueProvider):
            raise ValueError("window_size_provider must be a ValueProvider")
        self.window_size_provider = window_size_provider

    def assign(self, context):
        """Assigns windows to an element.

        Args:
            context: A WindowFn.AssignContext object.

        Returns:
            A list containing a single IntervalWindow.

        Raises:
            ValueError: If the window size is not positive.
        """

        try:
            window_size = self.window_size_provider.get()
            window_size = int(window_size)
            if window_size <= 0:
                logging.warning(
                    "Window size must be strictly positive. Using default value: %s",
                    self.DEFAULT_WINDOW_SIZE,
                )
                window_size = self.DEFAULT_WINDOW_SIZE
        except Exception as e:
            logging.warning(
                "Failed to get window size: %s. Using default value: %s",
                str(e),
                self.DEFAULT_WINDOW_SIZE,
            )
            window_size = self.DEFAULT_WINDOW_SIZE

        timestamp = context.timestamp
        size = Duration.of(window_size)
        start = timestamp - (timestamp % size)
        return [IntervalWindow(start, start + size)]

    def get_window_coder(self):
        """Returns the coder to use for windows."""
        return coders.IntervalWindowCoder()

    @property
    def size(self):
        """Get the window size."""
        return self.window_size_provider.get()


def parse_json(message: bytes) -> Dict[str, Any]:
    """Parse JSON message from PubSub"""
    import json

    encodings = ["utf-8", "shift-jis", "euc-jp", "iso-2022-jp"]

    for encoding in encodings:
        try:
            return json.loads(message.decode(encoding))
        except UnicodeDecodeError:
            continue
        except json.JSONDecodeError:
            break

    raise ValueError(f"Failed to decode message with any of the encodings: {encodings}")


class DecodeAndParse(beam.DoFn):
    """Decode and parse PubSub message"""

    def process(self, element) -> List[Dict[str, Any]]:
        try:
            result = parse_json(element)
            return [result]
        except Exception as e:
            logging.error(f"Error parsing JSON: {e}")
            return []


class MetricTypeRouter(beam.DoFn):
    def __init__(self, metric_type, field):
        self.metric_type = metric_type
        self.field = field

    def process(self, element) -> Generator[Union[Dict, float], None, None]:
        try:
            if isinstance(self.metric_type, beam.options.value_provider.ValueProvider):
                is_count = self.metric_type.get().upper() == "COUNT"
            else:
                is_count = self.metric_type == MetricType.COUNT

            if is_count:
                yield element
            else:
                yield float(element.get(self.field, 0))
        except Exception as e:
            logging.error(f"Error routing metric type: {str(e)}")
            yield 0.0


class MessagesToMetricsPipeline(beam.PTransform):
    """Transform PubSub messages to Cloud Monitoring metrics"""

    def __init__(
        self,
        filter_conditions: List[FilterCondition],
        metrics_config: GoogleCloudMetricsConfig,
        metric_definition: MetricDefinition,
        window_size: beam.options.value_provider.ValueProvider,
        export_type: Union[str, ValueProvider],
    ):
        """Initialize the pipeline transform

        Args:
            filter_conditions: List of conditions for filtering messages
            metrics_config: Configuration for metrics export
            metric_definition: Definition of the metric to generate
            window_size: Size of the fixed window in seconds (minimum 60)

        Raises:
            ValueError: If window_size is less than 60 seconds
        """

        super().__init__()
        self.filter = MessageFilter(filter_conditions)
        self.metrics_config = metrics_config
        self.metric_definition = metric_definition
        self.window_size = (
            window_size
            if isinstance(window_size, ValueProvider)
            else StaticValueProvider(int, window_size)
        )
        self.export_type = export_type

    def _get_window_transform(self):
        """Get the window transform with configured size"""
        return beam.WindowInto(DynamicFixedWindows(self.window_size))

    def _get_combiner(self):
        """Get appropriate combiner based on metric type"""
        metric_type = self.metric_definition.type

        class DeferredMetricCombiner(beam.CombineFn):
            def __init__(self, metric_type):
                self.metric_type = metric_type

            def create_accumulator(self):
                return 0

            def add_input(self, accumulator, input):
                try:
                    if isinstance(input, dict):
                        return accumulator + 1
                    return accumulator + input
                except Exception as e:
                    logging.error(f"Error adding input: {e}")
                    return accumulator

            def merge_accumulators(self, accumulators):
                try:
                    return sum(accumulators)
                except Exception as e:
                    logging.error(f"Error merging accumulators: {e}")
                    return 0

            def extract_output(self, accumulator):
                try:
                    if isinstance(
                        self.metric_type, beam.options.value_provider.ValueProvider
                    ):
                        type_str = self.metric_type.get().upper()
                    else:
                        type_str = self.metric_type.value.upper()

                    return accumulator if type_str == "COUNT" else accumulator
                except Exception as e:
                    logging.error(f"Error extracting output: {e}")
                    return 0

        return DeferredMetricCombiner(metric_type)

    def expand(self, pcoll):
        filtered = (
            pcoll
            | "DecodeAndParse" >> beam.ParDo(DecodeAndParse())
            | "FilterMessages" >> beam.Filter(self.filter.matches)
        )

        values = filtered | "RouteByMetricType" >> beam.ParDo(
            MetricTypeRouter(self.metric_definition.type, self.metric_definition.field)
        )

        keyed = values | "AddLabels" >> beam.Map(
            lambda msg: (
                tuple(
                    sorted(
                        {
                            **self.metric_definition.metric_labels,
                            **{
                                label_name: str(msg.get(field_name, ""))
                                for label_name, field_name in self.metric_definition.dynamic_labels.items()
                                if msg.get(field_name) is not None
                            },
                        }.items()
                    )
                ),
                msg if isinstance(msg, (int, float)) else 1,
            )
        )

        return (
            keyed
            | "Window" >> self._get_window_transform()
            | "CombinePerKey" >> beam.CombinePerKey(self._get_combiner())
            | "FormatOutput"
            >> beam.Map(lambda kv: {"labels": dict(kv[0]), "value": kv[1]})
            | "ExportMetrics"
            >> beam.ParDo(ExportMetrics(self.metrics_config, self.export_type))
        )


def test_pipeline_with_sum_metric():
    """Test pipeline with SUM metric type"""
    with TestPipeline() as p:
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
            connection_config=GoogleCloudConnectionConfig(project_id="test-project"),
        )

        metric_definition = MetricDefinition(
            name="bytes_total",
            type=MetricType.SUM,
            field="bytes",
            metric_labels={"service": "test"},
            dynamic_labels={"region": "region"},
        )

        test_exporter = TestMetricsExporter()

        result = (
            p
            | beam.Create(input_data)
            | MessagesToMetricsPipeline(
                filter_conditions=[filter_condition],
                metrics_config=metrics_config,
                metric_definition=metric_definition,
                window_size=60,
                export_type="google-cloud-monitoring",
            )
            | beam.ParDo(test_exporter)
        )

        expected_metrics = [
            {
                "value": 300,
                "labels": {"service": "test", "region": "us-east1"},
            },  # 100 + 200
            {"value": 150, "labels": {"service": "test", "region": "us-west1"}},  # 150
        ]

        assert_that(result, equal_to(expected_metrics))
