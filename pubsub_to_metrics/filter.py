from dataclasses import dataclass
from typing import List


@dataclass
class FilterCondition:
    field: str
    value: str
    operator: str


class MessageFilter:
    def __init__(self, conditions: List[FilterCondition]) -> None:
        self.conditions = conditions

    def matches(self, message: dict) -> bool:
        if not self.conditions:
            return True

        return all(
            self._matches_condition(message, condition) for condition in self.conditions
        )

    def _matches_condition(self, message: dict, condition: FilterCondition) -> bool:
        if condition.operator == "equals":
            return message.get(condition.field) == condition.value
        elif condition.operator == "contains":
            value = message.get(condition.field)
            return isinstance(value, str) and condition.value in value
        elif condition.operator == "greater_than":
            value = message.get(condition.field)
            return isinstance(value, (int, float)) and value > float(condition.value)
        elif condition.operator == "less_than":
            value = message.get(condition.field)
            return isinstance(value, (int, float)) and value < float(condition.value)
        return False
