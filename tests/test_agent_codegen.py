"""Tests for thyme.agent.codegen — LLM-powered code generation."""
import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# _schema_to_prompt_text
# ---------------------------------------------------------------------------

def test_schema_to_prompt_text_includes_field_names():
    # Given: an introspected schema with known fields
    from thyme.agent.introspect import IntrospectedSchema, IntrospectedField
    from thyme.agent.codegen import _schema_to_prompt_text

    schema = IntrospectedSchema(
        fields=[
            IntrospectedField(name="user_id", type="str"),
            IntrospectedField(name="amount", type="float"),
        ],
        sample_rows=[],
    )

    # When: converting to prompt text
    text = _schema_to_prompt_text(schema, "OrderEvent")

    # Then: field names and dataset name are present
    assert "user_id" in text
    assert "amount" in text
    assert "OrderEvent" in text


def test_schema_to_prompt_text_includes_sample_rows():
    # Given: a schema with sample rows
    from thyme.agent.introspect import IntrospectedSchema, IntrospectedField
    from thyme.agent.codegen import _schema_to_prompt_text

    schema = IntrospectedSchema(
        fields=[IntrospectedField(name="id", type="str")],
        sample_rows=[{"id": "abc"}, {"id": "def"}],
    )

    # When: converting to prompt text
    text = _schema_to_prompt_text(schema, "MyDataset")

    # Then: sample data mention is present
    assert "sample" in text.lower() or "abc" in text


# ---------------------------------------------------------------------------
# validate_code
# ---------------------------------------------------------------------------

def test_validate_code_returns_true_for_valid_thyme_code():
    # Given: valid Thyme SDK Python code
    from thyme.agent.codegen import validate_code

    code = """\
from datetime import datetime
from thyme.dataset import dataset, field

@dataset(index=True, version=1)
class OrderEvent:
    user_id: str = field(key=True)
    amount: float = field()
    event_time: datetime = field(timestamp=True)
"""

    # When: validating the code
    is_valid, error = validate_code(code)

    # Then: validation passes
    assert is_valid is True
    assert error == ""


def test_validate_code_returns_false_for_syntax_error():
    # Given: code with a syntax error
    from thyme.agent.codegen import validate_code

    code = "def broken(:\n    pass\n"

    # When: validating
    is_valid, error = validate_code(code)

    # Then: validation fails with an error message
    assert is_valid is False
    assert error != ""


def test_validate_code_returns_false_when_no_datasets_registered():
    # Given: valid Python but no @dataset decorators
    from thyme.agent.codegen import validate_code

    code = "x = 42\n"

    # When: validating
    is_valid, error = validate_code(code)

    # Then: fails because no datasets are registered
    assert is_valid is False
    assert "dataset" in error.lower()


def test_validate_code_returns_false_for_missing_key_field():
    # Given: a dataset missing a key field
    from thyme.agent.codegen import validate_code

    code = """\
from datetime import datetime
from thyme.dataset import dataset, field

@dataset(index=True)
class NoKey:
    amount: float = field()
    event_time: datetime = field(timestamp=True)
"""

    # When: validating
    is_valid, error = validate_code(code)

    # Then: validation fails
    assert is_valid is False


# ---------------------------------------------------------------------------
# generate_thyme_code (mocked anthropic)
# ---------------------------------------------------------------------------

def _make_mock_anthropic(generated_code: str):
    """Helper: build a mock anthropic.Anthropic client that returns generated_code."""
    mock_content = MagicMock()
    mock_content.text = generated_code

    mock_message = MagicMock()
    mock_message.content = [mock_content]

    mock_client = MagicMock()
    mock_client.messages.create.return_value = mock_message
    return mock_client


VALID_GENERATED_CODE = """\
from datetime import datetime
from thyme.dataset import dataset, field

@dataset(index=True, version=1)
class OrderEvent:
    user_id: str = field(key=True)
    amount: float = field()
    event_time: datetime = field(timestamp=True)
"""


def test_generate_thyme_code_calls_anthropic_api():
    # Given: a schema, connector dict, and a mocked Anthropic client
    from thyme.agent.introspect import IntrospectedSchema, IntrospectedField
    from thyme.agent import codegen

    schema = IntrospectedSchema(
        fields=[
            IntrospectedField(name="user_id", type="str"),
            IntrospectedField(name="amount", type="float"),
            IntrospectedField(name="ts", type="datetime"),
        ],
        sample_rows=[],
    )
    connector_dict = {"connector_type": "iceberg", "config": {"catalog": "local", "database": "sales", "table": "orders"}}
    mock_client = _make_mock_anthropic(VALID_GENERATED_CODE)

    # When: generating code with the mock client injected
    with patch("anthropic.Anthropic", return_value=mock_client):
        code = codegen.generate_thyme_code(
            schema=schema,
            connector=connector_dict,
            use_case="fraud detection",
            dataset_name="OrderEvent",
            api_key="test-key",
        )

    # Then: Anthropic was called and code was returned
    mock_client.messages.create.assert_called_once()
    assert "OrderEvent" in code or "dataset" in code


def test_generate_thyme_code_strips_markdown_fences():
    # Given: Anthropic returns code wrapped in markdown fences
    from thyme.agent.introspect import IntrospectedSchema, IntrospectedField
    from thyme.agent import codegen

    wrapped = f"```python\n{VALID_GENERATED_CODE}\n```"
    schema = IntrospectedSchema(
        fields=[IntrospectedField(name="id", type="str")],
        sample_rows=[],
    )
    mock_client = _make_mock_anthropic(wrapped)

    with patch("anthropic.Anthropic", return_value=mock_client):
        code = codegen.generate_thyme_code(
            schema=schema,
            connector={"connector_type": "jsonl", "config": {"path": "/tmp/x.jsonl"}},
            use_case="test",
            api_key="test-key",
        )

    # Then: no markdown fences in the output
    assert "```" not in code


def test_generate_thyme_code_uses_windows_in_prompt():
    # Given: specific windows and a schema
    from thyme.agent.introspect import IntrospectedSchema, IntrospectedField
    from thyme.agent import codegen

    schema = IntrospectedSchema(
        fields=[IntrospectedField(name="id", type="str")],
        sample_rows=[],
    )
    mock_client = _make_mock_anthropic(VALID_GENERATED_CODE)

    with patch("anthropic.Anthropic", return_value=mock_client):
        codegen.generate_thyme_code(
            schema=schema,
            connector={"connector_type": "jsonl", "config": {}},
            use_case="test",
            windows=["6h", "48h"],
            api_key="test-key",
        )

    # Then: the prompt sent to Claude mentions the windows
    call_kwargs = mock_client.messages.create.call_args
    messages = call_kwargs[1].get("messages") or call_kwargs[0][2]
    prompt_text = messages[0]["content"]
    assert "6h" in prompt_text
    assert "48h" in prompt_text
