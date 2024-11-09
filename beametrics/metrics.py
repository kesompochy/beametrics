from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional


class MetricType(Enum):
    """Types of metrics that can be generated"""

    COUNT = "count"
    SUM = "sum"


@dataclass
class MetricDefinition:
    """Definition of a metric to be generated from messages"""

    name: str
    type: MetricType
    field: Optional[str]
    metric_labels: Dict[str, str]
    dynamic_labels: Optional[Dict[str, str]] = None

    def __post_init__(self):
        """Validate metric definition after initialization"""
        if self.type in [MetricType.SUM] and self.field is None:
            raise ValueError(f"field is required for {self.type.value} metric type")
        self.dynamic_labels = self.dynamic_labels or {}
