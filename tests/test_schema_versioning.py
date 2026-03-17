"""Unit tests for schema versioning: optional flag propagation through SDK."""
from dataclasses import fields as dc_fields
from typing import Optional
from datetime import datetime

import pytest

from thyme.dataset import (
    Field,
    _build_schema,
    _is_optional,
    dataset,
    field,
    clear_registry,
)
from thyme.compiler import compile_dataset
from thyme.gen import schema_pb2


# --- _build_schema tests ---


class _StubUserEvents:
    """Minimal stub with Optional and non-Optional fields for schema tests."""
    pass


def _make_dataset_cls():
    """Build a real @dataset-decorated class with an Optional field."""
    from dataclasses import dataclass

    @dataset(version=1)
    class UserEvents:
        user_id: str = field(key=True)
        event_time: datetime = field(timestamp=True)
        amount: float = Field()
        label: Optional[str] = Field()

    return UserEvents


class TestOptionalFieldInSchema:
    def setup_method(self):
        clear_registry()

    def test_optional_field_in_schema(self):
        """Given a dataset with Optional[str] field, _build_schema sets optional: True."""
        cls = _make_dataset_cls()
        schema = cls._dataset_meta

        # Find the 'label' field (Optional[str])
        label_field = next(f for f in schema["fields"] if f["name"] == "label")
        assert label_field["optional"] is True

        # Non-optional fields should not have 'optional' key
        amount_field = next(f for f in schema["fields"] if f["name"] == "amount")
        assert "optional" not in amount_field

    def test_key_field_not_optional(self):
        """Key fields are never optional."""
        cls = _make_dataset_cls()
        schema = cls._dataset_meta

        key_field = next(f for f in schema["fields"] if f["name"] == "user_id")
        assert "optional" not in key_field

    def test_timestamp_field_not_optional(self):
        """Timestamp fields are never optional."""
        cls = _make_dataset_cls()
        schema = cls._dataset_meta

        ts_field = next(f for f in schema["fields"] if f["name"] == "event_time")
        assert "optional" not in ts_field


class TestOptionalCompiledToProto:
    def setup_method(self):
        clear_registry()

    def test_optional_compiled_to_proto(self):
        """Given a dataset meta with optional: True, compiler wraps dtype in OptionalType."""
        ds_meta = {
            "name": "TestDs",
            "version": 1,
            "index": False,
            "fields": [
                {"name": "id", "type": "str", "key": True},
                {"name": "ts", "type": "datetime", "timestamp": True},
                {"name": "value", "type": "float"},
                {"name": "note", "type": "str", "optional": True},
            ],
        }
        proto_ds = compile_dataset(ds_meta)

        # 'note' field should be wrapped in OptionalType
        note_field = proto_ds.schema.fields[3]
        assert note_field.dtype.HasField("optional_type")
        inner = note_field.dtype.optional_type.inner
        assert inner.HasField("string_type")

        # 'value' field should NOT be wrapped
        value_field = proto_ds.schema.fields[2]
        assert value_field.dtype.HasField("float_type")

    def test_non_optional_fields_unchanged(self):
        """Non-optional fields compile to plain DataType, not OptionalType."""
        ds_meta = {
            "name": "Simple",
            "version": 1,
            "index": False,
            "fields": [
                {"name": "id", "type": "int", "key": True},
                {"name": "ts", "type": "datetime", "timestamp": True},
            ],
        }
        proto_ds = compile_dataset(ds_meta)

        id_field = proto_ds.schema.fields[0]
        assert id_field.dtype.HasField("int_type")

        ts_field = proto_ds.schema.fields[1]
        assert ts_field.dtype.HasField("timestamp_type")
