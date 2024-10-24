import apache_beam as beam
from apache_beam.transforms.window import FixedWindows
from typing import Dict, Any
from .filter import FilterCondition, MessageFilter
from .metrics_publisher import (
    GoogleCloudMetricsConfig,
    GoogleCloudMetricsPublisher,
)


def parse_json(message: bytes) -> Dict[str, Any]:
    import json

    return json.loads(message.decode("utf-8"))


class DecodeAndParse(beam.DoFn):
    def process(self, element):
        return [parse_json(element)]


class PublishMetricsToCloudMonitoring(beam.DoFn):
    def __init__(self, metrics_config: GoogleCloudMetricsConfig):
        self.metrics_config = metrics_config
        self.publisher = None

    def setup(self):
        self.publisher = GoogleCloudMetricsPublisher(self.metrics_config)

    def process(self, count):
        self.publisher.publish(float(count))
        yield count


class PubsubToCloudMonitoringPipeline(beam.PTransform):
    def __init__(
        self,
        filter_condition: FilterCondition,
        metrics_config: GoogleCloudMetricsConfig,
    ):
        super().__init__()
        self.filter = MessageFilter(filter_condition)
        self.metrics_config = metrics_config

    def expand(self, pcoll):
        return (
            pcoll
            | "Window" >> beam.WindowInto(FixedWindows(60))
            | "DecodeAndParse" >> beam.ParDo(DecodeAndParse())
            | "FilterMessages" >> beam.Filter(self.filter.matches)
            | "CountMessages"
            >> beam.CombineGlobally(beam.combiners.CountCombineFn()).without_defaults()
            | "PublishMetrics"
            >> beam.ParDo(PublishMetricsToCloudMonitoring(self.metrics_config))
        )
