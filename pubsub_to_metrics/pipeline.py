import apache_beam as beam
from apache_beam.transforms.window import FixedWindows
from typing import Dict, Any
from .filter import FilterCondition, MessageFilter
from .metrics import MetricType, MetricDefinition
from .metrics_exporter import (
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
        filter_condition: FilterCondition,
        metrics_config: GoogleCloudMetricsConfig,
        metric_definition: MetricDefinition,
    ):
        super().__init__()
        self.filter = MessageFilter(filter_condition)
        self.metrics_config = metrics_config
        self.metric_definition = metric_definition

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
            | "Window" >> beam.WindowInto(FixedWindows(60))
            | "DecodeAndParse" >> beam.ParDo(DecodeAndParse())
            | "FilterMessages" >> beam.Filter(self.filter.matches)
        )

        if self.metric_definition.type == MetricType.COUNT:
            # Count型の場合は直接カウント
            values = filtered
        else:
            # その他の型の場合はフィールドを抽出して集計
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
