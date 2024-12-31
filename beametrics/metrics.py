import json
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional, Union

from apache_beam.options.value_provider import StaticValueProvider, ValueProvider


class MetricType(Enum):
    """Types of metrics that can be generated"""

    COUNT = "count"
    SUM = "sum"


@dataclass
class MetricDefinition:
    name: str
    type: Union[MetricType, ValueProvider]
    field: Optional[str]
    metric_labels: Optional[Dict[str, str]] = None
    dynamic_labels: Optional[Union[Dict[str, str], ValueProvider]] = None

    def __post_init__(self):
        if isinstance(self.type, ValueProvider):
            if isinstance(self.type, StaticValueProvider):
                type_value = self.type.get().lower()
                if type_value == "sum" and self.field is None:
                    raise ValueError(f"field is required for {type_value} metric type")

        elif self.type in [MetricType.SUM] and self.field is None:
            raise ValueError(f"field is required for {self.type.value} metric type")

        if not isinstance(self.dynamic_labels, ValueProvider):
            self.dynamic_labels = self.dynamic_labels or {}

        if not isinstance(self.metric_labels, ValueProvider):
            self.metric_labels = self.metric_labels or {}

    def get_dynamic_labels(self) -> Dict[str, str]:
        """Get resolved dynamic labels"""
        if isinstance(self.dynamic_labels, ValueProvider):
            try:
                # json.load("null") is None
                return json.loads(self.dynamic_labels.get()) or {}
            except Exception:
                return {}
        return self.dynamic_labels or {}
