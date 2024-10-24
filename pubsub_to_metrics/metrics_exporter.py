from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Protocol
from google.cloud import monitoring_v3
import time
import apache_beam as beam


class ConnectionConfig(Protocol):
    """Protocol for connection configuration"""


@dataclass
class GoogleCloudConnectionConfig:
    """Configuration for Google Cloud connection"""

    project_id: str

    @property
    def project_name(self) -> str:
        return f"projects/{self.project_id}"


@dataclass
class MetricsConfig(Protocol):
    """Configuration for metrics exporting"""

    metric_name: str
    labels: dict[str, str]
    connection_config: ConnectionConfig


@dataclass
class GoogleCloudMetricsConfig(MetricsConfig):
    """Configuration for Google Cloud metrics exporting"""

    metric_name: str
    labels: dict[str, str]
    connection_config: GoogleCloudConnectionConfig


class MetricsExporter(ABC):
    """Base class for exporting metrics"""

    def __init__(self, config: MetricsConfig):
        self.config = config

    @abstractmethod
    def export(self, value: float) -> None:
        """Exports a metric value"""
        pass


class GoogleCloudMetricsExporter(MetricsExporter):
    """Exporter for Google Cloud metrics"""

    def __init__(self, config: GoogleCloudMetricsConfig):
        super().__init__(config)
        self.client = monitoring_v3.MetricServiceClient()
        self.config: GoogleCloudMetricsConfig = config

    def export(self, value: float):
        now = time.time()
        seconds = int(now)
        aligned_seconds = seconds - (seconds % 60)

        series = monitoring_v3.TimeSeries()
        series.metric.type = self.config.metric_name
        series.metric.labels.update(self.config.labels)
        series.resource.type = "global"

        point = monitoring_v3.Point()
        point.value.double_value = value

        interval = monitoring_v3.TimeInterval(
            {
                "end_time": {"seconds": aligned_seconds, "nanos": 0},
                "start_time": {
                    "seconds": aligned_seconds,
                    "nanos": 0,
                },
            }
        )
        point.interval = interval
        series.points = [point]

        request = monitoring_v3.CreateTimeSeriesRequest(
            name=self.config.connection_config.project_name,
            time_series=[series],
        )

        self.client.create_time_series(request=request)


class ExportMetricsToCloudMonitoring(beam.DoFn):
    def __init__(self, metrics_config: GoogleCloudMetricsConfig):
        self.metrics_config = metrics_config
        self.exporter = None

    def setup(self):
        self.exporter = GoogleCloudMetricsExporter(self.metrics_config)

    def process(self, count):
        self.exporter.export(float(count))
        yield count
