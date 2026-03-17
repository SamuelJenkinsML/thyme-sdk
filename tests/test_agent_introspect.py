"""Tests for thyme.agent.introspect — schema introspection tools."""
import json
import pytest


# ---------------------------------------------------------------------------
# IntrospectedSchema / IntrospectedField dataclass tests
# ---------------------------------------------------------------------------

def test_introspected_schema_has_fields_and_sample_rows():
    # Given / When: constructing an IntrospectedSchema
    from thyme.agent.introspect import IntrospectedSchema, IntrospectedField

    schema = IntrospectedSchema(
        fields=[IntrospectedField(name="user_id", type="str")],
        sample_rows=[{"user_id": "u1"}],
    )

    # Then: attributes are accessible
    assert len(schema.fields) == 1
    assert schema.fields[0].name == "user_id"
    assert schema.fields[0].type == "str"
    assert schema.sample_rows[0]["user_id"] == "u1"


def test_introspected_field_defaults_nullable():
    from thyme.agent.introspect import IntrospectedField

    f = IntrospectedField(name="ts", type="datetime")
    assert f.nullable is True


# ---------------------------------------------------------------------------
# DuckDB type mapping
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("duckdb_type,expected", [
    ("VARCHAR", "str"),
    ("TEXT", "str"),
    ("UUID", "str"),
    ("INTEGER", "int"),
    ("BIGINT", "int"),
    ("HUGEINT", "int"),
    ("FLOAT", "float"),
    ("DOUBLE", "float"),
    ("DECIMAL(10,2)", "float"),
    ("BOOLEAN", "bool"),
    ("TIMESTAMP", "datetime"),
    ("TIMESTAMP WITH TIME ZONE", "datetime"),
    ("DATE", "datetime"),
    ("UNKNOWN_TYPE", "str"),  # fallback
])
def test_duckdb_type_to_python_mapping(duckdb_type, expected):
    # Given: a DuckDB column type string
    from thyme.agent.introspect import _duckdb_type_to_python

    # When: converting to a Python type string
    result = _duckdb_type_to_python(duckdb_type)

    # Then: returns the expected Python type
    assert result == expected


# ---------------------------------------------------------------------------
# introspect_jsonl
# ---------------------------------------------------------------------------

def test_introspect_jsonl_returns_schema(tmp_path):
    # Given: a JSONL file with known field types
    data_file = tmp_path / "events.jsonl"
    data_file.write_text(
        '{"user_id": "u1", "amount": 1.5, "active": true, "created_at": "2024-01-01T00:00:00"}\n'
        '{"user_id": "u2", "amount": 2.0, "active": false, "created_at": "2024-01-02T00:00:00"}\n'
    )
    from thyme.agent.introspect import introspect_jsonl

    # When: introspecting the JSONL file
    schema = introspect_jsonl(str(data_file), sample_n=2)

    # Then: fields are correctly detected
    field_names = {f.name for f in schema.fields}
    assert "user_id" in field_names
    assert "amount" in field_names
    assert "active" in field_names


def test_introspect_jsonl_infers_float_type(tmp_path):
    # Given: a JSONL file with float amounts
    data_file = tmp_path / "orders.jsonl"
    data_file.write_text(
        '{"order_id": "o1", "total": 19.99}\n'
        '{"order_id": "o2", "total": 5.00}\n'
    )
    from thyme.agent.introspect import introspect_jsonl

    # When: introspecting
    schema = introspect_jsonl(str(data_file), sample_n=2)

    # Then: total is float
    type_map = {f.name: f.type for f in schema.fields}
    assert type_map["total"] == "float"
    assert type_map["order_id"] == "str"


def test_introspect_jsonl_returns_sample_rows(tmp_path):
    # Given: a JSONL file with 5 rows
    data_file = tmp_path / "events.jsonl"
    rows = [json.dumps({"id": str(i), "val": i * 1.0}) for i in range(5)]
    data_file.write_text("\n".join(rows) + "\n")
    from thyme.agent.introspect import introspect_jsonl

    # When: introspecting with sample_n=3
    schema = introspect_jsonl(str(data_file), sample_n=3)

    # Then: sample_rows contains at most 3 rows
    assert len(schema.sample_rows) <= 3
    assert all("id" in r for r in schema.sample_rows)


# ---------------------------------------------------------------------------
# introspect_schema dispatcher
# ---------------------------------------------------------------------------

def test_introspect_schema_dispatches_to_jsonl(tmp_path):
    # Given: a JSONL connector dict and a data file
    data_file = tmp_path / "data.jsonl"
    data_file.write_text('{"pk": "a", "score": 0.9}\n')
    connector_dict = {"connector_type": "jsonl", "config": {"path": str(data_file)}}

    from thyme.agent.introspect import introspect_schema

    # When: calling introspect_schema with the dict
    schema = introspect_schema(connector_dict, sample_n=1)

    # Then: schema fields include 'pk' and 'score'
    names = {f.name for f in schema.fields}
    assert "pk" in names
    assert "score" in names


def test_introspect_schema_raises_for_unknown_connector_type():
    # Given: an unknown connector type
    connector_dict = {"connector_type": "unknown_db", "config": {}}

    from thyme.agent.introspect import introspect_schema

    # When / Then: raises ValueError
    with pytest.raises(ValueError, match="Unsupported connector type"):
        introspect_schema(connector_dict)


def test_introspect_schema_accepts_connector_object(tmp_path):
    # Given: a connector object with a to_dict() method
    data_file = tmp_path / "items.jsonl"
    data_file.write_text('{"item_id": "i1", "price": 12.5}\n')

    class FakeConnector:
        def to_dict(self):
            return {"connector_type": "jsonl", "config": {"path": str(data_file)}}

    from thyme.agent.introspect import introspect_schema

    # When: calling introspect_schema with the connector object
    schema = introspect_schema(FakeConnector(), sample_n=1)

    # Then: fields are returned
    assert any(f.name == "item_id" for f in schema.fields)


# ---------------------------------------------------------------------------
# sample_data convenience function
# ---------------------------------------------------------------------------

def test_sample_data_returns_list_of_dicts(tmp_path):
    # Given: a JSONL connector with known rows
    data_file = tmp_path / "s.jsonl"
    data_file.write_text(
        '{"k": "1", "v": 10}\n'
        '{"k": "2", "v": 20}\n'
        '{"k": "3", "v": 30}\n'
    )
    connector_dict = {"connector_type": "jsonl", "config": {"path": str(data_file)}}

    from thyme.agent.introspect import sample_data

    # When: calling sample_data with n=2
    rows = sample_data(connector_dict, n=2)

    # Then: returns a list of dicts with at most n rows
    assert isinstance(rows, list)
    assert len(rows) <= 2
    assert all(isinstance(r, dict) for r in rows)
