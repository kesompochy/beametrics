import json
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

    def _get_metric_type(self) -> bool:
        """Get whether the metric type is COUNT"""
        try:
            if isinstance(
                self.metric_definition.type, beam.options.value_provider.ValueProvider
            ):
                return self.metric_definition.type.get().upper() == "COUNT"
            return self.metric_definition.type == MetricType.COUNT
        except Exception as e:
            logging.error(f"Error getting metric type: {e}")
            return True

    def expand(self, pcoll):
        filtered = (
            pcoll
            | "DecodeAndParse" >> beam.ParDo(DecodeAndParse())
            | "FilterMessages" >> beam.Filter(self.filter.matches)
        )

        keyed = filtered | "AddLabels" >> beam.Map(
            lambda msg: (
                tuple(
                    sorted(
                        {
                            **(
                                json.loads(self.metric_definition.metric_labels.get())
                                if isinstance(
                                    self.metric_definition.metric_labels, ValueProvider
                                )
                                else self.metric_definition.metric_labels
                            ),
                            **{
                                label_name: str(msg.get(field_name, ""))
                                for label_name, field_name in self.metric_definition.get_dynamic_labels().items()
                            },
                        }.items()
                    )
                ),
                (
                    1
                    if self._get_metric_type()
                    else float(msg.get(self.metric_definition.field, 0))
                ),
            )
        )

        return (
            keyed
            | "Window" >> self._get_window_transform()
            | "CombinePerKey" >> beam.CombinePerKey(sum)
            | "FormatOutput"
            >> beam.Map(lambda kv: {"labels": dict(kv[0]), "value": kv[1]})
            | "ExportMetrics"
            >> beam.ParDo(ExportMetrics(self.metrics_config, self.export_type))
        )
