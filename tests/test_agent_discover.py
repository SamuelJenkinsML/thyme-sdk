"""Tests for the thyme discover CLI command."""
import json
import pytest
from unittest.mock import MagicMock, patch
from typer.testing import CliRunner

from thyme.cli import app

runner = CliRunner()


# ---------------------------------------------------------------------------
# Basic CLI wiring
# ---------------------------------------------------------------------------

def test_discover_command_is_registered():
    # Given / When: asking for help on the discover command
    result = runner.invoke(app, ["discover", "--help"])

    # Then: command exists (exit code 0) and shows usage
    assert result.exit_code == 0
    assert "discover" in result.output.lower() or "use-case" in result.output.lower()


def test_discover_requires_use_case():
    # Given: invoked without --use-case
    result = runner.invoke(app, ["discover", "--source-type", "jsonl", "--path", "/tmp/x.jsonl"])

    # Then: command fails
    assert result.exit_code != 0


def test_discover_requires_source_info_for_jsonl():
    # Given: jsonl source type without --path
    result = runner.invoke(app, ["discover", "--source-type", "jsonl", "--use-case", "fraud"])

    # Then: command fails with a clear error
    assert result.exit_code != 0
    assert "path" in result.output.lower() or "path" in (result.stderr or "").lower()


# ---------------------------------------------------------------------------
# Full flow with mocked introspect + codegen
# ---------------------------------------------------------------------------

MOCK_GENERATED_CODE = """\
from datetime import datetime
from thyme.dataset import dataset, field

@dataset(index=True, version=1)
class OrderEvent:
    user_id: str = field(key=True)
    amount: float = field()
    event_time: datetime = field(timestamp=True)
"""


def _mock_schema():
    from thyme.agent.introspect import IntrospectedSchema, IntrospectedField
    return IntrospectedSchema(
        fields=[
            IntrospectedField(name="user_id", type="str"),
            IntrospectedField(name="amount", type="float"),
            IntrospectedField(name="event_time", type="datetime"),
        ],
        sample_rows=[{"user_id": "u1", "amount": 9.99, "event_time": "2024-01-01T00:00:00"}],
    )


def test_discover_jsonl_prints_generated_code(tmp_path):
    # Given: a JSONL source file and mocked introspect + codegen
    data_file = tmp_path / "orders.jsonl"
    data_file.write_text('{"user_id": "u1", "amount": 9.99, "event_time": "2024-01-01T00:00:00"}\n')

    with patch("thyme.agent.introspect.introspect_jsonl", return_value=_mock_schema()), \
         patch("thyme.agent.codegen.generate_thyme_code", return_value=MOCK_GENERATED_CODE), \
         patch("thyme.agent.codegen.validate_code", return_value=(True, "")):

        # When: running discover with --source-type jsonl --path <file> --use-case "fraud"
        result = runner.invoke(app, [
            "discover",
            "--source-type", "jsonl",
            "--path", str(data_file),
            "--use-case", "fraud detection",
        ])

    # Then: command succeeds and output contains the generated dataset code
    assert result.exit_code == 0
    assert "OrderEvent" in result.output or "dataset" in result.output


def test_discover_writes_to_output_file(tmp_path):
    # Given: mocked introspect + codegen + an output file path
    data_file = tmp_path / "orders.jsonl"
    data_file.write_text('{"user_id": "u1", "amount": 9.99}\n')
    output_file = tmp_path / "features.py"

    with patch("thyme.agent.introspect.introspect_jsonl", return_value=_mock_schema()), \
         patch("thyme.agent.codegen.generate_thyme_code", return_value=MOCK_GENERATED_CODE), \
         patch("thyme.agent.codegen.validate_code", return_value=(True, "")):

        result = runner.invoke(app, [
            "discover",
            "--source-type", "jsonl",
            "--path", str(data_file),
            "--use-case", "fraud",
            "--output", str(output_file),
        ])

    # Then: file is written with the generated code
    assert result.exit_code == 0
    assert output_file.exists()
    content = output_file.read_text()
    assert "OrderEvent" in content or "@dataset" in content


def test_discover_iceberg_dispatches_correctly():
    # Given: iceberg source options with mocked introspect + codegen
    from thyme.agent.introspect import IntrospectedSchema, IntrospectedField
    mock_schema = IntrospectedSchema(
        fields=[IntrospectedField(name="id", type="str"), IntrospectedField(name="ts", type="datetime")],
        sample_rows=[],
    )

    with patch("thyme.agent.introspect.introspect_iceberg", return_value=mock_schema) as mock_intro, \
         patch("thyme.agent.codegen.generate_thyme_code", return_value=MOCK_GENERATED_CODE), \
         patch("thyme.agent.codegen.validate_code", return_value=(True, "")):

        result = runner.invoke(app, [
            "discover",
            "--source-type", "iceberg",
            "--catalog", "local",
            "--database", "sales",
            "--table", "orders",
            "--use-case", "churn prediction",
        ])

    # Then: iceberg introspection was called with correct args
    assert result.exit_code == 0
    mock_intro.assert_called_once_with("local", "sales", "orders", sample_n=5)


def test_discover_shows_validation_warning_on_invalid_code(tmp_path):
    # Given: codegen returns code that fails validation
    data_file = tmp_path / "bad.jsonl"
    data_file.write_text('{"id": "1"}\n')

    with patch("thyme.agent.introspect.introspect_jsonl", return_value=_mock_schema()), \
         patch("thyme.agent.codegen.generate_thyme_code", return_value="bad code here"), \
         patch("thyme.agent.codegen.validate_code", return_value=(False, "No datasets registered")):

        result = runner.invoke(app, [
            "discover",
            "--source-type", "jsonl",
            "--path", str(data_file),
            "--use-case", "test",
        ])

    # Then: command exits with non-zero or shows warning in output
    # (discover warns but still shows generated code so user can fix it)
    assert "warning" in result.output.lower() or "invalid" in result.output.lower() or result.exit_code != 0
