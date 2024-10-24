import pytest
from beametrics.metrics import MetricType, MetricDefinition


def test_metric_type_values():
    """Test MetricType enum has expected values"""
    assert MetricType.COUNT.value == "count"
    assert MetricType.SUM.value == "sum"


def test_metric_definition_with_count():
    """Test MetricDefinition creation with COUNT type"""
    definition = MetricDefinition(
        name="error_count",
        type=MetricType.COUNT,
        field=None,
        labels={"service": "test"},
    )

    assert definition.name == "error_count"
    assert definition.type == MetricType.COUNT
    assert definition.field is None
    assert definition.labels == {"service": "test"}


def test_metric_definition_requires_field_for_non_count():
    """Test MetricDefinition requires field for non-COUNT metrics"""
    with pytest.raises(ValueError) as exc_info:
        MetricDefinition(
            name="test_sum", type=MetricType.SUM, field=None, labels={"service": "test"}
        )
    assert "field is required for sum metric type" == str(exc_info.value)
