from dataclasses import dataclass


@dataclass
class FilterCondition:
    field: str
    value: str
    operator: str


class MessageFilter:
    def __init__(self, condition: FilterCondition) -> None:
        self.condition = condition

    def matches(self, message: dict) -> bool:
        if self.condition.operator == "equals":
            return message.get(self.condition.field) == self.condition.value
        return False
