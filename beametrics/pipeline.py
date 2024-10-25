import apache_beam as beam
from apache_beam.transforms.window import FixedWindows
from typing import Dict, Any, List
from beametrics.filter import FilterCondition, MessageFilter
from beametrics.metrics import MetricType, MetricDefinition
from beametrics.metrics_exporter import (
    GoogleCloudMetricsConfig,
    GoogleCloudMetricsExporter,
)


def parse_json(message: bytes) -> Dict[str, Any]:
    """Parse JSON message from PubSub"""
    import json

    return json.loads(message.decode("utf-8"))


class DecodeAndParse(beam.DoFn):
    """Decode and parse PubSub message"""

    def process(self, element):
        return [parse_json(element)]


class ExportMetricsToCloudMonitoring(beam.DoFn):
    """Export metrics to Cloud Monitoring"""

    def __init__(self, metrics_config: GoogleCloudMetricsConfig):
        self.metrics_config = metrics_config
        self.exporter = None

    def setup(self):
        self.exporter = GoogleCloudMetricsExporter(self.metrics_config)

    def process(self, count):
        self.exporter.export(float(count))
        yield count


class ExtractField(beam.DoFn):
    """Extract field value from message for aggregation"""

    def __init__(self, field: str):
        self.field = field

    def process(self, element):
        value = element.get(self.field)
        if value is not None and isinstance(value, (int, float)):
            yield float(value)


class PubsubToCloudMonitoringPipeline(beam.PTransform):
    """Transform PubSub messages to Cloud Monitoring metrics"""

    def __init__(
        self,
        filter_conditions: List[FilterCondition],
        metrics_config: GoogleCloudMetricsConfig,
        metric_definition: MetricDefinition,
        window_size: int = 60,
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
        if window_size < 60:
            raise ValueError("window_size must be at least 60 seconds")

        super().__init__()
        self.filter = MessageFilter(filter_conditions)
        self.metrics_config = metrics_config
        self.metric_definition = metric_definition
        self.window_size = window_size

    def _get_window_transform(self):
        """Get the window transform with configured size"""
        return beam.WindowInto(FixedWindows(self.window_size))

    def _get_combiner(self):
        """Get appropriate combiner based on metric type"""
        if self.metric_definition.type == MetricType.COUNT:
            return beam.combiners.CountCombineFn()
        elif self.metric_definition.type == MetricType.SUM:
            return beam.combiners.SumInt64Fn()
        else:
            raise ValueError(f"Unsupported metric type: {self.metric_definition.type}")

    def expand(self, pcoll):
        filtered = (
            pcoll
            | "Window" >> self._get_window_transform()
            | "DecodeAndParse" >> beam.ParDo(DecodeAndParse())
            | "FilterMessages" >> beam.Filter(self.filter.matches)
        )

        if self.metric_definition.type == MetricType.COUNT:
            values = filtered
        else:
            values = (
                filtered
                | f"ExtractField_{self.metric_definition.field}"
                >> beam.ParDo(ExtractField(self.metric_definition.field))
            )

        return (
            values
            | "AggregateMetrics"
            >> beam.CombineGlobally(self._get_combiner()).without_defaults()
            | "ExportMetrics"
            >> beam.ParDo(ExportMetricsToCloudMonitoring(self.metrics_config))
        )
