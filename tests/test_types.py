"""Tests for thyme.types — type mapping between Thyme, Polars, and Arrow."""

import polars as pl
import pytest

from thyme.types import (
    schema_from_dataset,
    schema_from_featureset,
    thyme_type_to_polars,
)


# ---------------------------------------------------------------------------
# thyme_type_to_polars
# ---------------------------------------------------------------------------


class TestThymeTypeToPolars:
    """Given a Thyme type string, when converting to Polars, then the correct dtype is returned."""

    def test_int_maps_to_int64(self):
        assert thyme_type_to_polars("int") == pl.Int64

    def test_float_maps_to_float64(self):
        assert thyme_type_to_polars("float") == pl.Float64

    def test_str_maps_to_utf8(self):
        assert thyme_type_to_polars("str") == pl.Utf8

    def test_bool_maps_to_boolean(self):
        assert thyme_type_to_polars("bool") == pl.Boolean

    def test_datetime_maps_to_datetime(self):
        result = thyme_type_to_polars("datetime")
        assert result == pl.Datetime("us", "UTC")

    def test_unknown_type_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown Thyme type"):
            thyme_type_to_polars("complex128")

    def test_empty_string_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown Thyme type"):
            thyme_type_to_polars("")


# ---------------------------------------------------------------------------
# schema_from_featureset
# ---------------------------------------------------------------------------


class TestSchemaFromFeatureset:
    """Given featureset registry metadata, when building a Polars schema, then columns match."""

    def test_basic_featureset(self):
        # given
        fs_meta = {
            "name": "UserFeatures",
            "features": [
                {"name": "age", "dtype": "int", "id": 1},
                {"name": "score", "dtype": "float", "id": 2},
                {"name": "name", "dtype": "str", "id": 3},
            ],
            "extractors": [],
        }

        # when
        schema = schema_from_featureset(fs_meta)

        # then
        assert isinstance(schema, pl.Schema)
        assert schema["age"] == pl.Int64
        assert schema["score"] == pl.Float64
        assert schema["name"] == pl.Utf8

    def test_empty_featureset(self):
        fs_meta = {"name": "Empty", "features": [], "extractors": []}
        schema = schema_from_featureset(fs_meta)
        assert len(schema) == 0

    def test_featureset_with_bool_and_datetime(self):
        fs_meta = {
            "name": "Flags",
            "features": [
                {"name": "is_active", "dtype": "bool", "id": 1},
                {"name": "last_seen", "dtype": "datetime", "id": 2},
            ],
            "extractors": [],
        }
        schema = schema_from_featureset(fs_meta)
        assert schema["is_active"] == pl.Boolean
        assert schema["last_seen"] == pl.Datetime("us", "UTC")


# ---------------------------------------------------------------------------
# schema_from_dataset
# ---------------------------------------------------------------------------


class TestSchemaFromDataset:
    """Given dataset registry metadata, when building a Polars schema, then fields match."""

    def test_basic_dataset(self):
        # given
        ds_meta = {
            "name": "Orders",
            "version": 1,
            "index": False,
            "fields": [
                {"name": "order_id", "type": "str", "key": True},
                {"name": "amount", "type": "float"},
                {"name": "timestamp", "type": "datetime", "timestamp": True},
            ],
            "dependencies": [],
            "expectations": [],
        }

        # when
        schema = schema_from_dataset(ds_meta)

        # then
        assert schema["order_id"] == pl.Utf8
        assert schema["amount"] == pl.Float64
        assert schema["timestamp"] == pl.Datetime("us", "UTC")

    def test_dataset_preserves_field_order(self):
        ds_meta = {
            "name": "Test",
            "version": 1,
            "index": False,
            "fields": [
                {"name": "z_field", "type": "int"},
                {"name": "a_field", "type": "str"},
                {"name": "m_field", "type": "float"},
            ],
            "dependencies": [],
            "expectations": [],
        }
        schema = schema_from_dataset(ds_meta)
        assert list(schema.names()) == ["z_field", "a_field", "m_field"]

    def test_empty_dataset(self):
        ds_meta = {
            "name": "Empty",
            "version": 1,
            "index": False,
            "fields": [],
            "dependencies": [],
            "expectations": [],
        }
        schema = schema_from_dataset(ds_meta)
        assert len(schema) == 0
