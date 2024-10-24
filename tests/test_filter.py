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
    matcher = MessageFilter([condition])

    assert (
        matcher.matches({"severity": "ERROR", "message": "Database connection failed"})
        is True
    )

    assert (
        matcher.matches({"severity": "INFO", "message": "Process completed"}) is False
    )


def test_message_filter_with_multiple_conditions():
    """Test MessageFilter with multiple conditions"""
    conditions = [
        FilterCondition(field="severity", value="ERROR", operator="equals"),
        FilterCondition(field="code", value="500", operator="equals"),
    ]
    matcher = MessageFilter(conditions)

    # Both conditions match
    assert (
        matcher.matches(
            {"severity": "ERROR", "code": "500", "message": "Internal server error"}
        )
        is True
    )

    # Only one condition matches
    assert (
        matcher.matches({"severity": "ERROR", "code": "404", "message": "Not found"})
        is False
    )

    # No conditions match
    assert (
        matcher.matches({"severity": "INFO", "code": "200", "message": "Success"})
        is False
    )


def test_message_filter_with_different_operators():
    """Test MessageFilter with different operators"""
    conditions = [
        FilterCondition(field="severity", value="ERROR", operator="equals"),
        FilterCondition(field="message", value="database", operator="contains"),
        FilterCondition(field="response_time", value="100", operator="greater_than"),
    ]
    matcher = MessageFilter(conditions)

    # All conditions match
    assert (
        matcher.matches(
            {
                "severity": "ERROR",
                "message": "database connection failed",
                "response_time": 150,
            }
        )
        is True
    )

    # Some conditions don't match
    assert (
        matcher.matches(
            {
                "severity": "ERROR",
                "message": "database connection failed",
                "response_time": 50,
            }
        )
        is False
    )
