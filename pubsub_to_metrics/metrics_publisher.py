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
    """Configuration for metrics publishing"""

    metric_name: str
    labels: dict[str, str]
    connection_config: ConnectionConfig


@dataclass
class GoogleCloudMetricsConfig(MetricsConfig):
    """Configuration for Google Cloud metrics publishing"""

    metric_name: str
    labels: dict[str, str]
    connection_config: GoogleCloudConnectionConfig


class MetricsPublisher(ABC):
    """Base class for publishing metrics"""

    def __init__(self, config: MetricsConfig):
        self.config = config

    @abstractmethod
    def publish(self, value: float) -> None:
        """Publishes a metric value"""
        pass


class GoogleCloudMetricsPublisher(MetricsPublisher):
    """Publisher for Google Cloud metrics"""

    def __init__(self, config: GoogleCloudMetricsConfig):
        super().__init__(config)
        self.client = monitoring_v3.MetricServiceClient()
        self.config: GoogleCloudMetricsConfig = config

    def publish(self, value: float):
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


class PublishMetrics(beam.DoFn):
    def __init__(self, metrics_config: GoogleCloudMetricsConfig):
        self.metrics_config = metrics_config
        self.publisher = None

    def setup(self):
        self.publisher = GoogleCloudMetricsPublisher(self.metrics_config)

    def process(self, count):
        self.publisher.publish(float(count))
        yield count
