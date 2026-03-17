"""LLM-powered Thyme SDK code generation.

Uses the Anthropic API (claude-opus-4-6) to generate @dataset / @pipeline /
@featureset Python code from an introspected schema and a plain-English use
case description.
"""
import importlib.util
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

from thyme.agent.introspect import IntrospectedSchema


SYSTEM_PROMPT = """\
You are an expert Thyme feature platform engineer. Your job is to generate
valid, runnable Thyme SDK Python code given a source schema and a use-case
description.

Thyme SDK quick reference:
  from thyme.dataset import dataset, field
  from thyme.pipeline import pipeline, inputs, Avg, Count, Sum, Min, Max
  from thyme.featureset import featureset, feature, extractor, extractor_inputs, extractor_outputs
  from thyme.connectors import source, IcebergSource, PostgresSource, S3JsonSource

Decorators:
  @dataset(index=True, version=1) — applies @dataclass; needs exactly one
      field(key=True) and one field(timestamp=True); other fields use field()
  @source(connector, cursor="<ts_field>", every="5m", cdc="append") — attaches
      a data source to a dataset class
  @pipeline(version=1) / @inputs(InputDatasetClass) — method that returns a
      PipelineNode via .groupby(*keys).aggregate(**kwargs)
      Agg syntax: out_name=Avg("field", window="1h")
  @featureset — class with @feature(id=...) descriptors and @extractor methods
  @extractor_inputs("feature_a", "feature_b") / @extractor_outputs("out_x")

Rules:
  • Each pipeline method must be defined inside an output @dataset class.
  • The output dataset is the class that contains the pipeline method.
  • Extractors reference input feature names from existing featuresets or
    pipeline output datasets.
  • Import datetime if any field uses it.

Generate ONLY the Python code — no explanation, no markdown fences."""


def _schema_to_prompt_text(schema: IntrospectedSchema, dataset_name: str) -> str:
    lines = [f"Schema for dataset '{dataset_name}':"]
    for f in schema.fields:
        lines.append(f"  - {f.name}: {f.type}")
    if schema.sample_rows:
        lines.append(f"\nSample data ({len(schema.sample_rows)} rows):")
        for row in schema.sample_rows[:3]:
            lines.append(f"  {row}")
    return "\n".join(lines)


def generate_thyme_code(
    schema: IntrospectedSchema,
    connector: Any,
    use_case: str,
    entity_key: str | None = None,
    windows: list[str] | None = None,
    dataset_name: str | None = None,
    api_key: str | None = None,
) -> str:
    """Generate Thyme SDK Python code for *schema* and *use_case*.

    Args:
        schema: Introspected source schema.
        connector: Connector object or dict (``connector.to_dict()`` shape).
        use_case: Plain-English description of what features to generate.
        entity_key: Hint for which field is the entity key.
        windows: Aggregation window sizes (default: ["1h", "24h", "7d"]).
        dataset_name: Name for the generated dataset class.
        api_key: Anthropic API key (falls back to ANTHROPIC_API_KEY env var).

    Returns:
        Generated Python source code as a string.
    """
    import anthropic

    if windows is None:
        windows = ["1h", "24h", "7d"]
    if dataset_name is None:
        dataset_name = "SourceEvent"

    connector_dict = connector.to_dict() if hasattr(connector, "to_dict") else connector
    schema_text = _schema_to_prompt_text(schema, dataset_name)

    entity_hint = (
        f"Use '{entity_key}' as the entity key field."
        if entity_key
        else "Infer the entity key from the schema (look for 'user_id', 'customer_id', 'id', etc.)."
    )
    windows_str = ", ".join(f'"{w}"' for w in windows)

    connector_type = connector_dict.get("connector_type", "unknown")
    connector_cfg = connector_dict.get("config", {})

    user_prompt = f"""\
Generate Thyme SDK Python code for the following use case:

Use case: {use_case}

{schema_text}

Connector type: {connector_type}
Connector config: {connector_cfg}

{entity_hint}
Use these aggregation windows: {windows_str}

Requirements:
1. Define a @dataset dataclass matching the schema (field types, key, timestamp).
2. Attach the source connector with @source.
3. Define a second @dataset for aggregated output (pipeline output).
4. Define a @pipeline method inside the output dataset, decorated with @inputs.
5. Define a @featureset with an @extractor reading from the output dataset.
6. Import all Thyme SDK components needed.
7. Include `from datetime import datetime` if datetime fields are used.

Return ONLY the Python code, no explanation."""

    key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    client = anthropic.Anthropic(api_key=key)

    message = client.messages.create(
        model="claude-opus-4-6",
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )

    code = message.content[0].text.strip()

    # Strip markdown code fences if the model wraps its response
    if code.startswith("```python"):
        code = code[len("```python"):].lstrip()
    elif code.startswith("```"):
        code = code[3:].lstrip()
    if code.endswith("```"):
        code = code[:-3].rstrip()

    return code


def validate_code(code: str) -> tuple[bool, str]:
    """Validate generated Thyme SDK code by attempting a dry-run compile.

    Imports the code into a temporary module, then calls ``get_commit_payload``
    to verify that at least one dataset was registered successfully.

    Returns:
        ``(True, "")`` on success, ``(False, error_message)`` on failure.
    """
    from thyme.dataset import clear_registry, get_commit_payload

    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(code)
        tmp_path = Path(f.name)

    try:
        clear_registry()
        spec = importlib.util.spec_from_file_location("_thyme_generated", tmp_path)
        if spec is None or spec.loader is None:
            return False, "Could not create module spec from generated code"
        module = importlib.util.module_from_spec(spec)
        sys.modules["_thyme_generated"] = module
        spec.loader.exec_module(module)  # type: ignore[union-attr]

        payload = get_commit_payload()
        if not payload.get("datasets"):
            return False, "No datasets registered — check @dataset decorators in generated code"
        return True, ""
    except Exception as exc:
        return False, str(exc)
    finally:
        tmp_path.unlink(missing_ok=True)
        clear_registry()
        sys.modules.pop("_thyme_generated", None)
