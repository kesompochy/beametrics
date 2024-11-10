import pytest
from apache_beam.options.value_provider import StaticValueProvider, ValueProvider

from beametrics.metrics import MetricDefinition, MetricType


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
        metric_labels={"service": "test"},
    )

    assert definition.name == "error_count"
    assert definition.type == MetricType.COUNT
    assert definition.field is None
    assert definition.metric_labels == {"service": "test"}


def test_metric_definition_requires_field_for_non_count():
    """Test MetricDefinition requires field for non-COUNT metrics"""
    with pytest.raises(ValueError) as exc_info:
        MetricDefinition(
            name="test_sum",
            type=MetricType.SUM,
            field=None,
            metric_labels={"service": "test"},
        )
    assert "field is required for sum metric type" == str(exc_info.value)
    assert "field is required for sum metric type" == str(exc_info.value)


def test_metric_definition_with_dynamic_labels():
    """Test MetricDefinition with dynamic labels"""
    definition = MetricDefinition(
        name="error_count",
        type=MetricType.COUNT,
        field=None,
        metric_labels={"service": "test"},
        dynamic_labels={"region": "region_field"},
    )

    assert definition.name == "error_count"
    assert definition.type == MetricType.COUNT
    assert definition.field is None
    assert definition.metric_labels == {"service": "test"}
    assert definition.dynamic_labels == {"region": "region_field"}


def test_metric_definition_with_empty_dynamic_labels():
    """Test MetricDefinition initializes empty dynamic_labels to empty dict"""
    definition = MetricDefinition(
        name="error_count",
        type=MetricType.COUNT,
        field=None,
        metric_labels={"service": "test"},
    )

    assert definition.dynamic_labels == {}


def test_metric_definition_with_none_dynamic_labels():
    """Test MetricDefinition handles None dynamic_labels"""
    definition = MetricDefinition(
        name="error_count",
        type=MetricType.COUNT,
        field=None,
        metric_labels={"service": "test"},
        dynamic_labels=None,
    )

    assert definition.dynamic_labels == {}


def test_metric_definition_with_none_metric_labels():
    """Test MetricDefinition handles None metric_labels"""
    definition = MetricDefinition(
        name="error_count", type=MetricType.COUNT, field=None, metric_labels=None
    )

    assert definition.metric_labels == {}


def test_metric_definition_with_none_metric_labels_and_dynamic_labels():
    """Test MetricDefinition handles None metric_labels with dynamic labels"""
    definition = MetricDefinition(
        name="error_count",
        type=MetricType.COUNT,
        field=None,
        metric_labels=None,
        dynamic_labels={"region": "region_field"},
    )

    assert definition.metric_labels == {}
    assert definition.dynamic_labels == {"region": "region_field"}


def test_metric_definition_with_value_provider_type():
    """Test MetricDefinition with ValueProvider type"""
    from apache_beam.options.value_provider import StaticValueProvider

    definition = MetricDefinition(
        name="test_count",
        type=StaticValueProvider(str, "count"),
        field=None,
        metric_labels={"service": "test"},
    )
    assert definition.name == "test_count"
    assert isinstance(definition.type, ValueProvider)
    assert definition.field is None

    definition = MetricDefinition(
        name="test_sum",
        type=StaticValueProvider(str, "sum"),
        field="value",
        metric_labels={"service": "test"},
    )
    assert definition.name == "test_sum"
    assert isinstance(definition.type, ValueProvider)
    assert definition.field == "value"

    with pytest.raises(ValueError) as exc_info:
        MetricDefinition(
            name="test_sum",
            type=StaticValueProvider(str, "sum"),
            field=None,
            metric_labels={"service": "test"},
        )
    assert "field is required for sum metric type" in str(exc_info.value)


def test_metric_definition_with_null_value_provider_dynamic_labels():
    """Test MetricDefinition with ValueProvider that returns 'null' for dynamic_labels"""

    class NullValueProvider(ValueProvider):
        def get(self):
            return "null"

        def is_accessible(self):
            return True

    definition = MetricDefinition(
        name="error_count",
        type=MetricType.COUNT,
        field=None,
        metric_labels={"service": "test"},
        dynamic_labels=NullValueProvider(),
    )

    result = definition.get_dynamic_labels()
    assert result == {}
