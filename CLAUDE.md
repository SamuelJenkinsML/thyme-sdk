# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is Thyme

Thyme is a streaming feature platform. Users define features declaratively in Python; a separately-deployed Thyme service compiles and executes them.

This repo is the **Python SDK** only: decorators (`@dataset`, `@pipeline`, `@featureset`, `@source`), CLI (`thyme commit/status/logs/discover`), testing framework (`MockContext`), protobuf bindings, and the AI-powered feature discovery agent. The SDK is versioned and published independently of the runtime service.

## Commands

```bash
# Install dependencies
uv sync

# Run tests
make test       # uv run pytest -v

# Lint
make lint       # uv run ruff check .

# Format
make fmt        # uv run ruff format .
```

## Tests

- Use pytest along with its best practices for implementing tests.
- Use given / when / then style tests.
- When writing code use TDD — ensure tests are written first and fail, then make them pass.
- Test files live in `tests/`.
- Use `MockContext` from `thyme.testing` for unit testing pipelines without infrastructure.

## SDK Role

The SDK is the authoring surface for Thyme. The flow:

1. User defines `@dataset`, `@pipeline`, `@featureset`, `@source` decorated classes.
2. `thyme commit` serializes those definitions into a protobuf `CommitRequest` and sends it to the Thyme service over HTTP.
3. The service compiles the request and runs the resulting streaming jobs; the SDK does no execution itself.
4. Extractor Python source is captured at commit time so the service can run extractors at query time.

## Key Design Decisions

- **Lazy pipeline evaluation** — Pipeline methods return `PipelineNode` objects that form a DAG. No computation happens at import or commit time; the runtime service is responsible for execution.
- **Source code capture for extractors** — Extractor Python source is captured at commit time and stored alongside the definition so the runtime can execute it at query time.
- **Protobuf for wire format** — `CommitRequest` is serialized as protobuf. Python bindings are pre-generated in `thyme/gen/`.
- **Registry pattern** — Decorators register metadata in module-level registries (`_DATASET_REGISTRY`, `_PIPELINE_REGISTRY`, `_FEATURESET_REGISTRY`). `clear_registry()` resets state between commits.
- **Independent versioning** — The SDK ships separately from the runtime so it can be released to PyPI on its own cadence.

## Key Modules

- `thyme/dataset.py` — `@dataset`, `@field`, registry, serialization
- `thyme/pipeline.py` — `@pipeline`, `@inputs`, aggregation operators (`Avg`, `Count`, `Sum`, `Min`, `Max`)
- `thyme/featureset.py` — `@featureset`, `@feature`, `@extractor`, `@extractor_inputs`, `@extractor_outputs`
- `thyme/connectors.py` — `@source`, `IcebergSource`, `PostgresSource`, `S3JsonSource`
- `thyme/cli.py` — Typer CLI app (`commit`, `status`, `logs`, `discover`)
- `thyme/compiler.py` — Protobuf `CommitRequest` compilation
- `thyme/testing/` — `MockContext` in-memory pipeline simulator
- `thyme/agent/` — LLM-powered feature discovery (`introspect.py`, `codegen.py`)
- `thyme/gen/` — Pre-generated protobuf Python bindings

## Tech Stack

- **Python 3.12** (managed via UV)
- **pytest** — testing
- **typer** — CLI framework
- **httpx** — HTTP client for talking to the Thyme service
- **protobuf** — wire format for `CommitRequest`
- **rich** — terminal output formatting
- **anthropic** — LLM-powered feature discovery
- **ruff** — linting and formatting

