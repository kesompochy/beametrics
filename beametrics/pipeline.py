import logging
from typing import Any, Dict, Generator, List, Union

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.options.value_provider import StaticValueProvider, ValueProvider
from apache_beam.transforms.window import IntervalWindow, NonMergingWindowFn
from apache_beam.utils.timestamp import Duration

from beametrics.filter import FilterCondition, MessageFilter
from beametrics.metrics import MetricDefinition, MetricType
from beametrics.metrics_exporter import (
    ExportMetrics,
    GoogleCloudMetricsConfig,
    GoogleCloudMetricsExporter,
)


class DynamicFixedWindows(NonMergingWindowFn):
    """A windowing function that assigns each element to one time interval,
    with a window size that can be determined at runtime.

    Args:
        window_size_provider: A ValueProvider that provides the size of the window in seconds.
    """

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
        window_size = self.window_size_provider.get()

        try:
            window_size = int(window_size)
        except (TypeError, ValueError):
            raise ValueError("Window size must be an integer")

        if window_size <= 0:
            raise ValueError("The window size must be strictly positive.")

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

    return json.loads(message.decode("utf-8"))


class DecodeAndParse(beam.DoFn):
    """Decode and parse PubSub message"""

    def process(self, element) -> List[Dict[str, Any]]:
        try:
            result = parse_json(element)
            return [result]
        except Exception as e:
            logging.error(f"Error parsing JSON: {e}")
            return []


class ExtractField(beam.DoFn):
    """Extract field value from message for aggregation"""

    def __init__(self, field: str):
        self.field = field

    def process(self, element) -> Generator[float, None, None]:
        try:
            value = element.get(self.field)
            if value is not None and isinstance(value, (int, float)):
                yield float(value)
        except Exception as e:
            logging.error(f"Error extracting field {self.field}: {e}")


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
                if isinstance(input, dict):
                    return accumulator + 1
                return accumulator + input

            def merge_accumulators(self, accumulators):
                return sum(accumulators)

            def extract_output(self, accumulator):
                if isinstance(
                    self.metric_type, beam.options.value_provider.ValueProvider
                ):
                    type_str = self.metric_type.get().upper()
                else:
                    type_str = self.metric_type.value.upper()

                return (
                    accumulator if type_str == "COUNT" else accumulator
                )  # TODO: Implements for types other than COUNT

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

        return (
            values
            | "Window" >> self._get_window_transform()
            | "AggregateMetrics"
            >> beam.CombineGlobally(self._get_combiner()).without_defaults()
            | "ExportMetrics"
            >> beam.ParDo(ExportMetrics(self.metrics_config, self.export_type))
        )
