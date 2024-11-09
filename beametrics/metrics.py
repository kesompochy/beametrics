import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional, Union

from apache_beam.options.value_provider import ValueProvider


class MetricType(Enum):
    """Types of metrics that can be generated"""

    COUNT = "count"
    SUM = "sum"


@dataclass
class MetricDefinition:
    name: str
    type: MetricType
    field: Optional[str]
    metric_labels: Dict[str, str]
    dynamic_labels: Optional[Union[Dict[str, str], ValueProvider]] = (
        None  # ValueProviderを追加
    )

    def __post_init__(self):
        if self.type in [MetricType.SUM] and self.field is None:
            raise ValueError(f"field is required for {self.type.value} metric type")

        if not isinstance(self.dynamic_labels, ValueProvider):
            self.dynamic_labels = self.dynamic_labels or {}

    def get_dynamic_labels(self) -> Dict[str, str]:
        """Get resolved dynamic labels"""
        if isinstance(self.dynamic_labels, ValueProvider):
            try:
                return json.loads(self.dynamic_labels.get())
            except Exception:
                return {}
        return self.dynamic_labels or {}
