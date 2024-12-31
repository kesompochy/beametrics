import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.options.value_provider import StaticValueProvider, ValueProvider
from apache_beam.transforms.window import IntervalWindow, NonMergingWindowFn
from apache_beam.utils.timestamp import Duration

from beametrics.filter import FilterCondition, MessageFilter
from beametrics.metrics import MetricDefinition, MetricType
from beametrics.metrics_exporter import (
    ExporterConfig,
    MetricsExporter,
    MetricsExporterFactory,
)


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


@dataclass
class MetricConfig:
    """Configuration for a single metric pipeline"""

    filter_conditions: List[FilterCondition]
    exporter_config: ExporterConfig
    metric_definition: MetricDefinition


class MessagesToMetricsPipeline(beam.PTransform):
    """Transform PubSub messages to Cloud Monitoring metrics"""

    def __init__(
        self,
        metrics_configs: List[MetricConfig],
        window_size: beam.options.value_provider.ValueProvider,
    ):
        """Initialize the pipeline transform

        Args:
            filter_conditions: List of conditions for filtering messages
            exporter_config: Configuration for metrics export
            metric_definition: Definition of the metric to generate
            window_size: Size of the fixed window in seconds (minimum 60)

        Raises:
            ValueError: If window_size is less than 60 seconds
        """

        super().__init__()
        self.metrics_configs = metrics_configs
        self.window_size = (
            window_size
            if isinstance(window_size, ValueProvider)
            else StaticValueProvider(int, window_size)
        )

    def _get_window_transform(self):
        """Get the window transform with configured size"""
        return beam.WindowInto(DynamicFixedWindows(self.window_size))

    def _get_metric_type(self, metric_definition: MetricDefinition) -> bool:
        """Get whether the metric type is COUNT"""
        try:
            if isinstance(
                metric_definition.type, beam.options.value_provider.ValueProvider
            ):
                return metric_definition.type.get().upper() == "COUNT"
            return metric_definition.type == MetricType.COUNT
        except Exception as e:
            logging.error(f"Error getting metric type: {e}")
            return True

    def expand(self, pcoll):
        return (
            pcoll
            | "DecodeAndParse" >> beam.ParDo(DecodeAndParse())
            | "FilterAndLabel"
            >> beam.FlatMap(
                lambda msg: [
                    (
                        (i, tuple(sorted(self._get_labels(msg, config).items()))),
                        self._get_value(msg, config),
                    )
                    for i, config in enumerate(self.metrics_configs)
                    if MessageFilter(config.filter_conditions).matches(msg)
                ]
            )
            | "Window" >> beam.WindowInto(DynamicFixedWindows(self.window_size))
            | "CombinePerKey" >> beam.CombinePerKey(sum)
            | "FormatOutput"
            >> beam.Map(
                lambda kv: (kv[0][0], {"labels": dict(kv[0][1]), "value": kv[1]})
            )
            | "Export" >> beam.ParDo(MultiMetricsExporter(self.metrics_configs))
        )

    def _get_labels(self, msg: dict, config: MetricConfig) -> dict:
        if isinstance(config.metric_definition.metric_labels, ValueProvider):
            metric_labels_json = config.metric_definition.metric_labels.get()
            metric_labels = json.loads(metric_labels_json) if metric_labels_json else {}
        else:
            metric_labels = config.metric_definition.metric_labels or {}
        dynamic_labels = {
            label_name: str(msg.get(field_name, ""))
            for label_name, field_name in config.metric_definition.get_dynamic_labels().items()
        }
        return {**metric_labels, **dynamic_labels}

    def _get_value(self, msg: dict, config: MetricConfig) -> float:
        if isinstance(config.metric_definition.type, ValueProvider):
            metric_type = config.metric_definition.type.get().lower()
        else:
            metric_type = config.metric_definition.type

        if metric_type == "count":
            return 1
        return float(msg.get(config.metric_definition.field, 0))


class MultiMetricsExporter(beam.DoFn):
    """Export metrics to multiple destinations based on configuration."""

    def __init__(self, metrics_configs: List[MetricConfig]):
        self.metrics_configs = metrics_configs
        self.exporters: Dict[int, MetricsExporter] = {}

    def setup(self):
        """Initialize exporters for each configuration."""
        for i, config in enumerate(self.metrics_configs):
            self.exporters[i] = MetricsExporterFactory.create_exporter(
                config.exporter_config
            )

    def process(self, element):
        """Export metrics using the appropriate exporter."""
        config_index, metrics = element
        try:
            if config_index in self.exporters:
                self.exporters[config_index].export(metrics["value"], metrics["labels"])
            yield element
        except Exception as e:
            logging.error(f"Error exporting metrics: {e}")
