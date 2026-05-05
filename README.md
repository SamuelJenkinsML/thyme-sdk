# Thyme SDK

Real-time ML features. Defined in Python.

Thyme is a streaming feature platform that eliminates training/serving skew. You define features once in Python; Thyme keeps their values fresh as new events arrive.

## The Problem

You train a model offline on historical data. The offline metrics look great. You deploy it. Within weeks, production accuracy drops — not because the model is wrong, but because the features it sees in production are computed differently than the features it trained on.

This is **training/serving skew** — the most common silent killer of production ML systems. It comes from:

- **Separate batch and streaming pipelines** — Training features are computed in Spark/dbt/SQL, serving features in Flink or custom microservices. These pipelines diverge silently.
- **Hand-rolled feature stores** — Aggregation logic is copy-pasted between batch ETL and streaming jobs. Changes get made in one place, not both.
- **Stale features at serving time** — Batch pipelines run hourly or daily. Your model sees yesterday's data while making today's decisions.

## How Thyme Solves It

### One definition, two modes

You write a feature once in Python. Thyme compiles it to:

- **Streaming aggregation** — a continuously-running process that keeps values fresh within milliseconds of new events arriving
- **Point-in-time lookup** — the same logic applied to historical data at any past timestamp for offline training

There is one source of truth. The online and offline paths are guaranteed consistent.

### Declarative, not operational

You declare *what* a feature is, not *how* to run it. You don't manage stream consumers, state storage, or checkpoint recovery. Thyme handles the infrastructure; you own the feature logic.

## Business Benefits

- **Faster ML iteration** — Define a new feature, commit it, and it's live. No pipeline deployment, no backfill coordination, no ops ticket.
- **Fewer production incidents** — Training/serving parity is structural, not a convention you enforce manually.
- **Smaller infrastructure footprint** — One system replaces the batch ETL, the streaming job, and the custom serving layer.
- **Safe schema evolution** — Features have integer IDs. You can add, rename, and version features without breaking downstream consumers.

## How the SDK Fits In

The Python SDK in this repo is the authoring surface for Thyme. You use it to define datasets, pipelines, featuresets, and sources, and to commit those definitions to a separately-deployed Thyme service that compiles them into continuously-running streaming jobs and serves the resulting feature values.

```
Python SDK (this repo)  ──commit──▶  Thyme service  ──serves──▶  your application
```

The SDK is responsible for:

- A declarative DSL (`@dataset`, `@pipeline`, `@featureset`, `@source`) for describing features
- The `thyme` CLI for committing, inspecting, and discovering features
- A `MockContext` testing framework that mirrors engine semantics in-process so you can unit-test pipelines without infrastructure
- AI-powered feature discovery (`thyme discover`) that introspects a data source and proposes feature definitions

## SDK Features

- **Declarative DSL** — `@dataset`, `@pipeline`, `@featureset`, and `@source` decorators define your feature graph
- **Multiple connectors** — Iceberg, Postgres, S3 JSON, and local JSONL sources
- **Aggregation operators** — `Avg`, `Count`, `Sum`, `Min`, `Max` with configurable time windows
- **LLM-powered discovery** — `thyme discover` introspects a data source and generates feature definitions using AI
- **Testing framework** — `MockContext` for unit testing pipelines without infrastructure
- **CLI tools** — `commit`, `status`, `logs`, `discover` for managing the feature lifecycle

## Core Concepts

### Dataset

A dataset is a named, typed stream of events — the fundamental data container in Thyme.

```python
from datetime import datetime
from thyme import dataset, field

@dataset(index=True, version=1)
class Transaction:
    user_id: str      = field(key=True)
    amount:  float    = field()
    ts:      datetime = field(timestamp=True)
```

Every dataset has a **key field** (the entity identifier, used for grouping) and a **timestamp field** (event time, used for windowed aggregations). Setting `index=True` maintains a fast lookup index for query-time access.

### Pipeline

A pipeline is a windowed aggregation that continuously transforms one dataset into another. Defined as a method on the output dataset class:

```python
from thyme import pipeline, inputs, Avg, Count

@dataset(index=True, version=1)
class UserStats:
    user_id:       str      = field(key=True)
    avg_amount_7d: float    = field()
    txn_count_30d: int      = field()
    ts:            datetime = field(timestamp=True)

    @pipeline(version=1)
    @inputs(Transaction)
    def compute(cls, txns):
        return txns.groupby("user_id").aggregate(
            avg_amount_7d=Avg(of="amount", window="7d"),
            txn_count_30d=Count(of="user_id", window="30d"),
        )
```

Pipeline bodies are **lazy** — they return `PipelineNode` descriptions of the computation. No data is processed at import or commit time. Thyme compiles the DAG and executes it as a streaming job.

**Aggregation operators:**

| Operator | Description |
|----------|-------------|
| `Avg(of, window)` | Rolling mean of a numeric field |
| `Count(of, window)` | Rolling count of events |
| `Sum(of, window)` | Rolling sum of a numeric field |
| `Min(of, window)` | Rolling minimum |
| `Max(of, window)` | Rolling maximum |

Windows are specified as strings: `"7d"` (7 days), `"24h"` (24 hours), `"30m"` (30 minutes). All windows use **event time**, not processing time.

### Featureset

A featureset is a named collection of features that your models and applications consume — the public API of your feature pipeline.

```python
from thyme import featureset, feature, extractor, extractor_inputs, extractor_outputs

@featureset
class UserFeatures:
    uid:           str   = feature(id=1)
    avg_spend_7d:  float = feature(id=2)
    txn_count_30d: int   = feature(id=3)

    @extractor
    @extractor_inputs("uid")
    @extractor_outputs("avg_spend_7d", "txn_count_30d")
    def from_stats(cls, ts, inputs):
        uid = inputs["uid"]
        row = UserStats.lookup(ts, user_id=uid)
        return row["avg_amount_7d"], row["txn_count_30d"]
```

Features have **integer IDs** for stable schema evolution — you can rename features without breaking downstream consumers. Extractors run at serving time, transforming raw aggregates into the values your application consumes.

### Source

A source connects an external data system to a dataset. Thyme polls the source on a schedule and ingests new rows into the streaming pipeline.

```python
from thyme import source, IcebergSource

@source(
    IcebergSource(catalog="prod", database="events", table="transactions"),
    cursor="ts",
    every="1m",
    max_lateness="5m",
    cdc="append",
)
@dataset(index=True, version=1)
class Transaction:
    user_id: str      = field(key=True)
    amount:  float    = field()
    ts:      datetime = field(timestamp=True)
```

**Available connectors:** `IcebergSource`, `PostgresSource`, `S3JsonSource`, and local JSONL.

**CDC modes:** `append` (insert-only event logs), `upsert` (keyed updates), `debezium` (full CDC envelopes).

## Quick Start Example

A complete example showing all four primitives together — computing restaurant rating features from review events:

```python
from datetime import datetime
from thyme import dataset, field, pipeline, inputs, Avg, Count
from thyme import featureset, feature, extractor, extractor_inputs, extractor_outputs
from thyme import source, IcebergSource

# 1. Define the input dataset with a source connector
@source(
    IcebergSource(catalog="prod", database="reviews", table="restaurant_reviews"),
    cursor="timestamp",
    every="1m",
    max_lateness="5m",
    cdc="append",
)
@dataset(index=True, version=1)
class Review:
    restaurant_id: str   = field(key=True)
    rating:        float = field()
    timestamp:     datetime = field(timestamp=True)

# 2. Define an aggregated dataset with a pipeline
@dataset(index=True, version=1)
class RestaurantRatingStats:
    restaurant_id:   str      = field(key=True)
    avg_rating_24h:  float    = field()
    review_count_7d: float    = field()
    timestamp:       datetime = field(timestamp=True)

    @pipeline(version=1)
    @inputs(Review)
    def compute(cls, reviews):
        return reviews.groupby("restaurant_id").aggregate(
            avg_rating_24h=Avg(of="rating", window="24h"),
            review_count_7d=Count(of="rating", window="7d"),
        )

# 3. Define a featureset with an extractor
@featureset
class RestaurantFeatures:
    restaurant_id:   str   = feature(id=1)
    avg_rating_24h:  float = feature(id=2)
    review_count_7d: int   = feature(id=3)

    @extractor(deps=[RestaurantRatingStats])
    @extractor_inputs("restaurant_id")
    @extractor_outputs("avg_rating_24h", "review_count_7d")
    def from_stats(cls, ts, inputs):
        rid = inputs["restaurant_id"]
        row = RestaurantRatingStats.lookup(ts, restaurant_id=rid)
        return row["avg_rating_24h"], row["review_count_7d"]
```

## CLI Reference

The `thyme` CLI manages the feature lifecycle:

### `thyme commit`

Send feature definitions to the Thyme service.

```bash
# Commit from a file
thyme commit features.py

# Commit from a module path
thyme commit -m myproject.features

# Dry-run: print the payload without sending
thyme commit features.py --dry-run

# Write payload to a file
thyme commit features.py --dry-run --output payload.json
```

### `thyme status`

Show system status: committed definitions, running jobs, and service health.

```bash
thyme status
thyme status --json
```

### `thyme logs`

Show recent service events (commits, errors, backfills).

```bash
thyme logs
thyme logs --severity error
thyme logs --type backfill --limit 20
thyme logs --json
```

### `thyme discover`

AI-powered feature discovery. Introspects a data source schema and generates Thyme feature definitions.

```bash
# From an Iceberg table
thyme discover --source-type iceberg \
    --catalog prod --database events --table transactions \
    --use-case "fraud detection" \
    --output features.py

# From a local JSONL file
thyme discover --source-type jsonl \
    --path data/events.jsonl \
    --use-case "user engagement" \
    --output features.py --auto-commit

# From Postgres
thyme discover --source-type postgres \
    --pg-host localhost --pg-database mydb --pg-table orders \
    --pg-user admin --pg-password secret \
    --use-case "demand forecasting"
```

## Testing

The SDK includes `MockContext` — an in-memory pipeline simulator that mirrors the Thyme runtime's semantics without requiring any infrastructure.

```python
from thyme.testing import MockContext

def test_restaurant_ratings():
    ctx = MockContext()

    # Ingest events
    ctx.add_events(Review, [
        {"restaurant_id": "r1", "rating": 4.5, "timestamp": "2026-03-15T10:00:00Z"},
        {"restaurant_id": "r1", "rating": 3.5, "timestamp": "2026-03-15T11:00:00Z"},
        {"restaurant_id": "r1", "rating": 5.0, "timestamp": "2026-03-15T12:00:00Z"},
    ])

    # Check raw aggregates
    aggs = ctx.get_aggregates(RestaurantRatingStats, "r1")
    assert aggs["avg_rating_24h"] == pytest.approx(4.333, abs=0.01)
    assert aggs["review_count_7d"] == 3.0

    # Query through the featureset extractor
    features = ctx.query(RestaurantFeatures, "r1")
    assert features["avg_rating_24h"] == pytest.approx(4.333, abs=0.01)
```

`MockContext` supports:
- `add_events(dataset_class, events)` — ingest events and process through registered pipelines; returns expectation violations
- `get_aggregates(dataset_class, entity_id)` — get raw aggregated values for an entity
- `query(featureset_class, entity_id)` — run the full extractor chain and return feature values

## Installation

```bash
pip install thyme-sdk
```

## Development

```bash
# Install dependencies
uv sync

# Run tests
make test

# Lint
make lint

# Format
make fmt
```

## License

MIT
