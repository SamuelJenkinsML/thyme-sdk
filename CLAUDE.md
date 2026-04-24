# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is Thyme

Thyme is a streaming feature platform with a Python SDK and a Rust data plane. Users define features declaratively in Python; a Rust control plane and streaming engine compile and execute them.

This is the **Python SDK repo**. The main Thyme repo (Rust services, E2E tests, infrastructure) lives at `~/Projects/thyme`.

## Relationship to Main Repo

- **This repo (`thyme-sdk`)** — Python SDK: decorators (`@dataset`, `@pipeline`, `@featureset`, `@source`), CLI (`thyme commit/status/logs/discover`), testing framework (`MockContext`), protobuf bindings, AI-powered feature discovery agent.
- **Main repo (`~/Projects/thyme`)** — Rust monorepo: definition-service, engine, query-server, protobuf schema, E2E tests, infrastructure (Docker, Helm). E2E tests live there because they start Rust binaries.

The SDK was extracted for independent versioning and eventual PyPI publishing.

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

## Architecture

Data flows through four layers:

```
Python SDK (this repo) -> Definition Service (Rust) -> Engine (Rust) -> Query Server (Rust)
                               |                        |
                            Postgres               RocksDB state
                            Kafka topics           (keyed by entity + timestamp)
```

**SDK's role in the data path:**

1. User defines `@dataset`, `@pipeline`, `@featureset`, `@source` decorated classes
2. `thyme commit` serializes definitions into a protobuf `CommitRequest` and POSTs to the definition service
3. The Rust engine executes the compiled pipelines as streaming jobs
4. The query server runs Python extractors (captured via `inspect.getsource()` at commit time) at query time via PyO3

## Key Design Decisions

- **Lazy pipeline evaluation** — Pipeline methods return `PipelineNode` objects that form a DAG. No computation happens until the Rust engine processes them.
- **Source code capture** — Extractor Python source is captured with `inspect.getsource()` at commit time and stored in Postgres for execution by the query server.
- **SDK extracted for independent versioning** — Decoupled from the Rust monorepo for separate release cadence and PyPI publishing.
- **Protobuf for wire format** — `CommitRequest` is serialized as protobuf. Python bindings are pre-generated in `thyme/gen/`.
- **Registry pattern** — Decorators register metadata in module-level registries (`_DATASET_REGISTRY`, `_PIPELINE_REGISTRY`, `_FEATURESET_REGISTRY`). `clear_registry()` resets state between commits.

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
- **httpx** — HTTP client for control plane communication
- **protobuf** — wire format for `CommitRequest`
- **rich** — terminal output formatting
- **anthropic** — LLM-powered feature discovery
- **ruff** — linting and formatting

## Kanban — Cross-Repo Task Tracking

All Thyme task tracking lives in `~/Projects/thyme-docs/09-kanban/`. Tickets are individual markdown files in `09-kanban/tickets/TH-NNN.md` with YAML frontmatter (status, priority, repo, type, etc.).

**When to interact with the board:**
- **After fixing a bug or completing a feature**: Find the matching ticket (grep for the bug ID or feature name in `~/Projects/thyme-docs/09-kanban/tickets/`) and set `status: done`, update `updated:` date. Then run `bash ~/Projects/thyme-docs/scripts/sync-board.sh` to update the Visual Board.
- **When you discover a new bug or gap**: Create a new ticket — read the highest TH-NNN in the tickets folder, write TH-{N+1}.md with frontmatter. Set `repo: thyme-sdk`. Then run `bash ~/Projects/thyme-docs/scripts/sync-board.sh`.
- **When starting work on a ticket**: Set `status: in-progress` and `assignee: claude`. Then run `bash ~/Projects/thyme-docs/scripts/sync-board.sh`.

**Ticket frontmatter fields**: ticket-id, title, type (bug|feature|docs|infra|chore), repo, component, priority (p0-p3), status (backlog|todo|in-progress|review|done|cancelled), assignee, effort (xs-xl), created, updated, due, tags, related.

**Do not** create tickets for trivial fixes (typos, formatting). Only track work that represents meaningful bugs, features, or tasks.
