from pubsub_to_metrics.filter import FilterCondition, MessageFilter


def test_filter_condition_equals():
    """
    Test FilterCondition with equals operator
    """
    condition = FilterCondition(field="severity", value="ERROR", operator="equals")

    assert condition.field == "severity"
    assert condition.value == "ERROR"
    assert condition.operator == "equals"


def test_message_matcher_with_equals_condition():
    """
    Test MessageMatcher with equals condition
    """
    condition = FilterCondition(field="severity", value="ERROR", operator="equals")
    matcher = MessageFilter(condition)

    assert (
        matcher.matches({"severity": "ERROR", "message": "Database connection failed"})
        is True
    )

    assert (
        matcher.matches({"severity": "INFO", "message": "Process completed"}) is False
    )
