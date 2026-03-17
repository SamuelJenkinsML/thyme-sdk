"""Tests for data expectation functions and @expectations decorator."""
from datetime import datetime

import pytest

from thyme.expectations import (
    expect_column_values_to_be_between,
    expect_column_values_to_not_be_null,
    expect_column_values_to_be_in_set,
    expect_column_values_to_be_of_type,
    expectations,
)


def test_expect_between_returns_spec():
    # Given/When: we call expect_column_values_to_be_between
    spec = expect_column_values_to_be_between(column="rating", min_value=1.0, max_value=5.0)
    # Then: it returns the correct spec dict
    assert spec == {
        "type": "column_values_between",
        "column": "rating",
        "min_value": 1.0,
        "max_value": 5.0,
        "mostly": 1.0,
    }


def test_expect_not_null_returns_spec():
    # Given/When: we call expect_column_values_to_not_be_null
    spec = expect_column_values_to_not_be_null(column="rating")
    # Then: it returns the correct spec dict
    assert spec == {
        "type": "column_values_not_null",
        "column": "rating",
        "mostly": 1.0,
    }


def test_expect_in_set_returns_spec():
    # Given/When: we call expect_column_values_to_be_in_set
    spec = expect_column_values_to_be_in_set(column="status", values=["a", "b"])
    # Then: it returns the correct spec dict
    assert spec == {
        "type": "column_values_in_set",
        "column": "status",
        "values": ["a", "b"],
        "mostly": 1.0,
    }


def test_mostly_parameter():
    # Given/When: we pass mostly=0.95
    spec = expect_column_values_to_be_between(
        column="rating", min_value=1.0, max_value=5.0, mostly=0.95,
    )
    # Then: mostly is captured in the spec
    assert spec["mostly"] == 0.95


def test_expectations_decorator_marks_method():
    # Given: a method decorated with @expectations
    @expectations
    def get_expectations(cls):
        return []

    # Then: it has the _is_expectations attribute
    assert getattr(get_expectations, "_is_expectations", False) is True


def test_dataset_discovers_expectations():
    # Given: a dataset class with an @expectations method
    from thyme.dataset import dataset, field, _DATASET_REGISTRY, clear_registry

    clear_registry()

    @dataset(version=1)
    class TestDatasetWithExp:
        id: str = field(key=True)
        rating: float = field()
        timestamp: datetime = field(timestamp=True)

        @expectations
        def get_expectations(cls):
            return [
                expect_column_values_to_be_between(
                    column="rating", min_value=1.0, max_value=5.0, mostly=0.95,
                ),
            ]

    # Then: the dataset registry contains the expectations
    schema = _DATASET_REGISTRY["TestDatasetWithExp"]
    assert len(schema["expectations"]) == 1
    assert schema["expectations"][0]["type"] == "column_values_between"
    assert schema["expectations"][0]["column"] == "rating"
    assert schema["expectations"][0]["mostly"] == 0.95


def test_no_expectations_empty_list():
    # Given: a dataset class without @expectations
    from thyme.dataset import dataset, field, _DATASET_REGISTRY, clear_registry

    clear_registry()

    @dataset(version=1)
    class TestDatasetNoExp:
        id: str = field(key=True)
        timestamp: datetime = field(timestamp=True)

    # Then: the dataset registry has an empty expectations list
    schema = _DATASET_REGISTRY["TestDatasetNoExp"]
    assert schema["expectations"] == []


def test_multiple_expectations_in_list():
    # Given: a dataset class with multiple expectations
    from thyme.dataset import dataset, field, _DATASET_REGISTRY, clear_registry

    clear_registry()

    @dataset(version=1)
    class TestDatasetMultiExp:
        id: str = field(key=True)
        rating: float = field()
        timestamp: datetime = field(timestamp=True)

        @expectations
        def get_expectations(cls):
            return [
                expect_column_values_to_be_between(
                    column="rating", min_value=1.0, max_value=5.0,
                ),
                expect_column_values_to_not_be_null(column="id"),
            ]

    # Then: both expectations are captured
    schema = _DATASET_REGISTRY["TestDatasetMultiExp"]
    assert len(schema["expectations"]) == 2
    assert schema["expectations"][0]["type"] == "column_values_between"
    assert schema["expectations"][1]["type"] == "column_values_not_null"
