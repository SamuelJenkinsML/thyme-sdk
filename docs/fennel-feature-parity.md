# Fennel Feature Parity Roadmap

This document defines the features and connectors to port from the Fennel feature platform into Thyme. Each task is self-contained with enough context for an implementing agent to pick up independently. All work follows TDD: write failing tests first, then implement.

**Testing strategy**: Each task requires two test tiers:
- **Local tests** (`tests/`): Use `MockContext` and unit tests. Run via `make test` (`uv run pytest -v`). No infrastructure needed.
- **E2E tests** (`~/Projects/thyme/sdk/tests/test_e2e.py`): Run against live local infrastructure (Docker Compose: Postgres 5433, Redpanda/Kafka 19092, MinIO 9000) or AWS. These validate the full path: SDK commit -> definition-service -> engine -> query-server.

**Performance note**: Thyme's engine is a Rust streaming system backed by RocksDB with Kafka transactional processing. Every feature must consider: memory footprint per partition, RocksDB key/value size, window eviction efficiency, and Kafka throughput impact. The engine processes events in micro-batches (configurable `batch_size`, 50ms drain timeout) with atomic RocksDB WriteBatch + Kafka transactions.

---

## Table of Contents

1. [P0: Kafka Source Connector](#1-p0-kafka-source-connector)
2. [P0: Kinesis Source Connector](#2-p0-kinesis-source-connector)
3. [P0: Filter Operator (SDK Exposure)](#3-p0-filter-operator-sdk-exposure)
4. [P0: Transform Operator (SDK Exposure)](#4-p0-transform-operator-sdk-exposure)
5. [P1: Snowflake Source Connector](#5-p1-snowflake-source-connector)
6. [P1: BigQuery Source Connector](#6-p1-bigquery-source-connector)
7. [P1: MySQL Source Connector](#7-p1-mysql-source-connector)
8. [P1: Advanced Aggregations](#8-p1-advanced-aggregations)
9. [P1: Sink Connectors (Kafka, S3, Snowflake)](#9-p1-sink-connectors)
10. [P2: Tumbling, Hopping, and Session Windows](#10-p2-tumbling-hopping-and-session-windows)
11. [P2: Expression Engine](#11-p2-expression-engine)
12. [P2: Embedding and Decimal Data Types](#12-p2-embedding-and-decimal-data-types)
13. [P3: Metadata System](#13-p3-metadata-system)
14. [P3: Index Modes (Online/Offline)](#14-p3-index-modes-onlineoffline)
15. [P3: Source-Level Preprocessing, Filtering, and Sampling](#15-p3-source-level-preprocessing-filtering-and-sampling)

---

## 1. P0: Kafka Source Connector

### Problem

Thyme has no streaming source connector. Users can only poll batch sources (Postgres, S3, Iceberg). For real-time feature engineering (fraud detection, recommendations, session features), users need to consume directly from Kafka topics without an intermediary.

### Current State

- **SDK connectors** (`thyme/connectors.py`): Three connector classes (`IcebergSource`, `PostgresSource`, `S3JsonSource`), each with a `to_dict()` method returning `{"connector_type": str, "config": dict}`.
- **`@source` decorator** (`thyme/connectors.py:93-128`): Wraps connector with metadata (`cursor`, `every`, `disorder`, `cdc`). Stores in `_SOURCE_REGISTRY`. Validates `cdc` against `{"append", "debezium", "upsert"}`.
- **Proto schema** (`~/Projects/thyme/proto/thyme/connector.proto:28-40`): `Source` message has a `oneof connector` field (currently: `iceberg`, `postgres`, `s3json`). New connectors require adding to this oneof.
- **Compiler** (`thyme/compiler.py:158-191`): `compile_source()` dispatches on `connector_type` string to populate the proto oneof via `CopyFrom()`.
- **Rust source ingestion** (`~/Projects/thyme/crates/engine/src/source.rs`): `SourceIngestionLoop` polls via Python scripts (DuckDB/psycopg2), produces to Kafka. For Kafka sources, the engine should consume directly from the user's topic instead of going through the poll-and-produce loop.
- **Config** (`thyme/config.py`): `Config` dataclass with factory methods (`postgres_source()`, `s3_source()`, `iceberg_source()`). Env var pattern: `THYME_POSTGRES_*`, `THYME_S3_*`.

### What Fennel Does

Fennel's Kafka connector (`~/Projects/client/fennel/connectors/connectors.py:548-576, 877-890`):

```python
class Kafka(DataSource):
    bootstrap_servers: str
    security_protocol: Literal["PLAINTEXT", "SASL_PLAINTEXT", "SASL_SSL"]
    sasl_mechanism: Literal["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI"]
    sasl_plain_username: Optional[Union[str, Secret]]
    sasl_plain_password: Optional[Union[str, Secret]]

class KafkaConnector(DataConnector):
    topic: str
    format: Optional[str | Avro | Protobuf]  # "json", Avro(...), Protobuf(...)
```

Format classes support schema registry integration (registry URL, username/password/token auth).

### Requirements

**SDK (`thyme-sdk`):**

1. Add `KafkaSource` class to `thyme/connectors.py`:
   - Fields: `bootstrap_servers: str`, `topic: str`, `group_id: str = ""`, `security_protocol: str = "PLAINTEXT"`, `sasl_mechanism: str = ""`, `sasl_username: str = ""`, `sasl_password: str = ""`, `format: str = "json"` (start with JSON only; Avro/Protobuf are follow-ups).
   - `to_dict()` returns `{"connector_type": "kafka", "config": {...}}`.
2. Add `KafkaConfig` dataclass to `thyme/config.py`:
   - Fields: `bootstrap_servers: str = "localhost:19092"`, `group_id: str = ""`, `security_protocol: str = "PLAINTEXT"`.
   - Env vars: `THYME_KAFKA_BOOTSTRAP_SERVERS`, `THYME_KAFKA_GROUP_ID`, `THYME_KAFKA_SECURITY_PROTOCOL`.
   - Factory method: `Config.kafka_source(topic: str) -> KafkaSource`.
3. Update `@source` decorator: Kafka sources should not require `cursor` or `every` (they are streaming, not polling). Validate that `cursor=""` and `every=""` when connector_type is `"kafka"`.
4. Add `KafkaSource` proto message to `connector.proto` and add `kafka` to the `Source` oneof.
5. Update `compile_source()` in `compiler.py` to handle `connector_type="kafka"`.
6. Re-generate Python protobuf bindings in `thyme/gen/`.
7. Export `KafkaSource` and `KafkaConfig` from `thyme/__init__.py`.

**Engine (`~/Projects/thyme`, Rust):**

8. Add `KafkaSourceIngestion` path in `source.rs`: Instead of polling via Python, create a `StreamConsumer` that reads from the user's topic and produces to the internal pipeline topic. This is a direct Kafka-to-Kafka bridge with format deserialization.
9. Consumer config: Use the `group_id` from source spec (or derive `thyme-source-{dataset_name}`), honor `security_protocol` and SASL settings.
10. Handle `disorder` by passing it through to the engine's watermark/late-event logic (already exists in `operators.rs:568-586`).

### Tests

**Local (thyme-sdk):**
- `KafkaSource.__init__()` stores all fields correctly.
- `KafkaSource.to_dict()` returns expected shape.
- `@source` with `KafkaSource` registers in `_SOURCE_REGISTRY` with correct metadata.
- `@source` with `KafkaSource` rejects non-empty `cursor` or `every`.
- `compile_source()` produces valid `connector_pb2.Source` with kafka oneof populated.
- `Config.kafka_source("my-topic")` returns correctly configured `KafkaSource`.
- Env var overrides for `THYME_KAFKA_*` work in `Config.load()`.

**E2E (~/Projects/thyme):**
- Produce JSON messages to a Redpanda topic -> commit a dataset with `KafkaSource` -> engine consumes and aggregates -> query-server returns correct features.
- Test with `cdc="append"` and `cdc="upsert"` modes.
- Measure: end-to-end latency from produce to queryable feature (target: <5s for single partition).

### Performance Considerations

- Kafka-to-Kafka bridge must not buffer entire partitions in memory. Use rdkafka's internal flow control.
- Consumer should use `fetch.min.bytes` / `fetch.wait.max.ms` tuning for latency vs throughput tradeoff.
- For high-throughput topics (>10k msg/s), ensure the bridge doesn't become a bottleneck vs. direct topic assignment to the engine runner.

---

## 2. P0: Kinesis Source Connector

### Problem

AWS Kinesis is a common streaming source in AWS-native environments. Users on AWS who don't use Kafka need a direct Kinesis ingestion path.

### Current State

Same as Kafka connector (see [section 1](#1-p0-kafka-source-connector) for connector pattern details).

### What Fennel Does

Fennel's Kinesis connector (`~/Projects/client/fennel/connectors/connectors.py:656-681, 1022-1050`):

```python
class Kinesis(DataSource):
    role_arn: str

class KinesisConnector(DataConnector):
    stream_arn: str
    init_position: str | at_timestamp  # "latest", "trim_horizon", or at_timestamp(datetime)
    format: str  # "json" only
```

`init_position` supports: `"latest"`, `"trim_horizon"`, or `at_timestamp(datetime|int|float|str)`.

### Requirements

**SDK (`thyme-sdk`):**

1. Add `KinesisSource` class to `thyme/connectors.py`:
   - Fields: `stream_arn: str`, `role_arn: str = ""`, `region: str = "us-east-1"`, `init_position: str = "latest"`, `format: str = "json"`.
   - `init_position` values: `"latest"`, `"trim_horizon"`, or ISO-8601 timestamp string.
   - `to_dict()` returns `{"connector_type": "kinesis", "config": {...}}`.
2. Add `KinesisConfig` dataclass to `thyme/config.py`:
   - Fields: `stream_arn: str = ""`, `role_arn: str = ""`, `region: str = "us-east-1"`.
   - Env vars: `THYME_KINESIS_STREAM_ARN`, `THYME_KINESIS_ROLE_ARN`, `THYME_KINESIS_REGION`.
   - Factory method: `Config.kinesis_source(stream_arn: str) -> KinesisSource`.
3. Same `@source` streaming validation as Kafka: no `cursor` or `every`.
4. Add `KinesisSource` proto message to `connector.proto`, add `kinesis` to Source oneof.
5. Update `compile_source()`, re-generate proto bindings.
6. Export from `thyme/__init__.py`.

**Engine (`~/Projects/thyme`, Rust):**

7. Add Kinesis consumer using `aws-sdk-kinesis` crate (or `rusoto_kinesis`). Shard iterator based on `init_position`. Read records -> deserialize JSON -> produce to internal Kafka topic.
8. Handle shard splits/merges gracefully. Track shard iterators in Postgres (similar to cursor tracking for batch sources).
9. Use IAM role assumption via `role_arn` for cross-account access.

### Tests

**Local (thyme-sdk):**
- `KinesisSource.__init__()` stores fields, validates `init_position` against allowed values.
- `to_dict()` shape is correct.
- `@source` rejects `cursor`/`every` for Kinesis.
- `compile_source()` produces valid proto with kinesis oneof.

**E2E (AWS):**
- Create a Kinesis stream -> put records -> commit dataset with `KinesisSource` -> engine consumes -> query-server returns features.
- Test `init_position="trim_horizon"` (reads all) and `"latest"` (reads only new).
- Test IAM role assumption with `role_arn`.

### Performance Considerations

- Kinesis has per-shard read limits (2 MB/s, 5 reads/s). Use enhanced fan-out (`SubscribeToShard`) for high-throughput streams to avoid throttling.
- Each shard maps to one engine partition. Shard count determines parallelism.
- Checkpoint shard sequence numbers in Postgres alongside cursor tracking.

---

## 3. P0: Filter Operator (SDK Exposure)

### Problem

The proto schema already defines `Filter` (`dataset.proto:61-64`) with `operand_id` and `pycode` fields. The Rust engine has expectation-based filtering but no general Filter operator. The SDK has no `filter()` method on `PipelineNode`. Users cannot filter bad records, null values, or irrelevant events before aggregation.

### Current State

- **Proto** (`dataset.proto:61-64`): `Filter { string operand_id = 1; PyCode pycode = 2; }` exists.
- **Proto Operator oneof** (`dataset.proto:36-46`): `filter` is field 3 in the `Operator.op` oneof.
- **SDK PipelineNode** (`thyme/pipeline.py:53-78`): Has `groupby()` and `aggregate()` only. No `filter()`.
- **Compiler** (`thyme/compiler.py:81-124`): `compile_pipeline()` handles `"aggregate"` and `"temporal_join"` operators. No `"filter"` handling.
- **Rust engine** (`operators.rs`): `OperatorContext::from_spec()` parses `"aggregate"` and `"temporal_join"` from the operators array. No filter operator parsing or execution.
- **MockContext** (`thyme/testing/_engine.py:118-188`): `add_events()` processes aggregate operators only. No filter step.

### What Fennel Does

Fennel has `Filter` as a pipeline node operation: `node.filter(lambda df: df["col"] > 0)`. The filter function receives a DataFrame and returns a boolean mask or filtered DataFrame.

### Requirements

**SDK (`thyme-sdk`):**

1. Add `PipelineNode.filter(func: Callable)` method in `thyme/pipeline.py`:
   - Captures `func` source code via `inspect.getsource(func)`.
   - Stores filter spec: `{"filter": {"pycode": source_code, "entry_point": func_name}}`.
   - Returns a new `PipelineNode` with the filter appended to an internal `_operators` list.
   - The filter function signature: `def my_filter(event: dict) -> bool` — returns `True` to keep, `False` to drop.
2. Refactor `PipelineNode` internals:
   - Currently `_group_keys` and `_agg_specs` are flat attributes. Change to an `_operators: list[dict]` that accumulates operators in order. `groupby().aggregate()` becomes two entries in `_operators`.
   - `get_operators()` on `Pipeline` (line 96-106) returns the full `_operators` list.
3. Update `compile_pipeline()` in `compiler.py` to handle `"filter"` operator:
   - Create `dataset_pb2.Filter(operand_id="", pycode=_make_pycode(source, entry_point))`.
   - Wrap in `dataset_pb2.Operator(id="filter", filter=filter_proto)`.
4. Update `MockContext.add_events()` in `testing/_engine.py`:
   - Before processing aggregate operators, apply filter operators in order.
   - For each event, call the filter function. If it returns `False`, skip the event.
   - Import and exec the filter function from source code for MockContext (use `exec()` with controlled namespace).

**Engine (`~/Projects/thyme`, Rust):**

5. Add filter operator to `OperatorContext::from_spec()`:
   - Parse `"filter"` entries from the operators array.
   - Store filter PyCode in context.
6. Execute filter in `process_record()`:
   - Before aggregation (before line 568 in operators.rs), execute the Python filter function via PyO3.
   - If the function returns `False`, skip the record (return empty `ProcessResult`).
   - Cache the compiled Python function per operator context to avoid re-compilation per record.

### Tests

**Local (thyme-sdk):**
- `PipelineNode.filter(fn)` captures source code and stores operator spec.
- Chaining works: `node.filter(fn1).filter(fn2).groupby("k").aggregate(...)` produces 3 operators in order.
- `compile_pipeline()` produces valid `Operator` with `filter` oneof populated.
- `MockContext`: events rejected by filter are not counted in aggregations.
- `MockContext`: multiple chained filters apply in sequence (AND semantics).
- Edge case: filter that rejects all events -> aggregations return 0.0.

**E2E:**
- Define pipeline with filter -> aggregate. Ingest mixed events. Verify only filtered events contribute to feature values.
- Measure: filter overhead per event (target: <1ms for simple predicates via cached PyO3).

### Performance Considerations

- PyO3 function call overhead per record is the main concern. The engine must compile the Python filter function once and cache the callable.
- For high-throughput pipelines, consider evaluating filters in batches rather than per-record.
- Filter before aggregation reduces downstream state size — this is a net performance win for selective filters.

---

## 4. P0: Transform Operator (SDK Exposure)

### Problem

Users cannot derive new fields, rename columns, cast types, or compute expressions before aggregation. The proto defines `Transform` (`dataset.proto:66-69`) but the SDK doesn't expose it.

### Current State

Same as Filter (see [section 3](#3-p0-filter-operator-sdk-exposure)). `Transform` has `operand_id` and `pycode` in the proto. Not implemented in SDK, compiler, MockContext, or Rust engine.

### What Fennel Does

Fennel has `Transform` as a pipeline node: `node.transform(lambda df: df.assign(new_col=df["a"] + df["b"]))`. The function receives a dict/row and returns a modified dict/row.

### Requirements

**SDK (`thyme-sdk`):**

1. Add `PipelineNode.transform(func: Callable)` method in `thyme/pipeline.py`:
   - Captures `func` source code via `inspect.getsource()`.
   - Stores: `{"transform": {"pycode": source_code, "entry_point": func_name}}`.
   - Returns new `PipelineNode`.
   - Function signature: `def my_transform(event: dict) -> dict` — returns a modified event dict (can add/remove/rename fields).
2. Update `compile_pipeline()` to handle `"transform"` operator (same pattern as filter).
3. Update `MockContext.add_events()`:
   - After filters, apply transforms in order before aggregation.
   - Each transform replaces the event dict with the function's return value.
   - Validate the returned dict has required key and timestamp fields.

**Engine (`~/Projects/thyme`, Rust):**

4. Parse `"transform"` in `OperatorContext::from_spec()`.
5. Execute transform via PyO3 before aggregation in `process_record()`. The Python function receives the parsed JSON record as a dict and returns a modified dict.
6. Cache compiled Python function per operator context.

### Tests

**Local (thyme-sdk):**
- `PipelineNode.transform(fn)` captures source and stores spec.
- Transform that adds a field: downstream aggregate can reference the new field.
- Transform that renames a field: downstream aggregate uses new name.
- Chaining: `filter -> transform -> groupby -> aggregate` works end-to-end in MockContext.
- `compile_pipeline()` produces valid `Operator` with transform oneof.

**E2E:**
- Pipeline: transform (compute derived field) -> aggregate on derived field -> query returns correct value.
- Measure: transform overhead per event.

### Performance Considerations

- Same PyO3 caching strategy as filter. Compile once, call many.
- Transforms that increase record size (adding many fields) increase RocksDB write amplification for materialize-mode pipelines. Document this tradeoff.

---

## 5. P1: Snowflake Source Connector

### Problem

Snowflake is the most common data warehouse for ML teams. Without a Snowflake connector, users must export data to S3 or Postgres before ingesting into Thyme, adding latency and operational overhead.

### Current State

Batch source pattern established by `PostgresSource` (see [section 1](#1-p0-kafka-source-connector) for pattern details). The Rust engine executes Python poll scripts via PyO3 (`source.rs:346-388` for Postgres pattern).

### What Fennel Does

Fennel's Snowflake connector (`~/Projects/client/fennel/connectors/connectors.py:621-653`):

```python
class Snowflake(DataSource):
    account: str
    db_name: str
    username: Union[str, Secret]
    password: Union[str, Secret]
    warehouse: str
    src_schema: str  # aliased from "schema"
    role: str
```

Uses `TableConnector` with `table_name` and optional `cursor` for incremental reads.

### Requirements

**SDK (`thyme-sdk`):**

1. Add `SnowflakeSource` class to `thyme/connectors.py`:
   - Fields: `account: str`, `database: str`, `schema: str = "PUBLIC"`, `warehouse: str`, `role: str = ""`, `table: str`, `user: str`, `password: str`.
   - `to_dict()` returns `{"connector_type": "snowflake", "config": {...}}`.
2. Add `SnowflakeConfig` dataclass to `thyme/config.py`:
   - Fields: `account`, `database`, `schema`, `warehouse`, `role`, `user`, `password`.
   - Env vars: `THYME_SNOWFLAKE_ACCOUNT`, `THYME_SNOWFLAKE_DATABASE`, `THYME_SNOWFLAKE_WAREHOUSE`, `THYME_SNOWFLAKE_USER`, `THYME_SNOWFLAKE_PASSWORD`, `THYME_SNOWFLAKE_SCHEMA`, `THYME_SNOWFLAKE_ROLE`.
   - Factory: `Config.snowflake_source(table: str) -> SnowflakeSource`.
3. Add `SnowflakeSource` proto message to `connector.proto`, add to Source oneof.
4. Update `compile_source()`, re-generate bindings.
5. Export from `__init__.py`.

**Engine (`~/Projects/thyme`, Rust):**

6. Add Snowflake poll script in `source.rs` (follow Postgres pattern at lines 346-388):
   - Use `snowflake-connector-python` via PyO3.
   - Query: `SELECT * FROM {schema}.{table} WHERE CAST({cursor} AS VARCHAR) > '{cursor_value}' ORDER BY {cursor} LIMIT 50000`.
   - Connection: `snowflake.connector.connect(account=..., user=..., password=..., warehouse=..., database=..., schema=..., role=...)`.
7. Cursor tracking: same Postgres-based cursor table as other batch sources.

### Tests

**Local (thyme-sdk):**
- `SnowflakeSource.__init__()` and `to_dict()` produce correct shape.
- `compile_source()` produces valid proto with snowflake oneof.
- `Config.snowflake_source()` uses config defaults, env vars override.

**E2E (requires Snowflake account):**
- Seed a Snowflake table -> commit dataset with `SnowflakeSource` + cursor -> engine polls and ingests -> features queryable.
- Test incremental polling: add rows after initial ingest, verify cursor advances.
- Measure: poll latency for 10k rows (target: <30s including Snowflake query time).

### Performance Considerations

- Snowflake queries have cold-start latency (warehouse resume). Set `every` to at least `"5m"` to amortize.
- Use `LIMIT` in poll queries to bound memory per poll cycle.
- For large tables, the initial backfill can be slow. Consider documenting a pattern for pre-seeding via S3 export + `S3JsonSource` for the historical load, then switching to `SnowflakeSource` for incremental.

---

## 6. P1: BigQuery Source Connector

### Problem

BigQuery is Google Cloud's primary warehouse. GCP-native teams need direct BigQuery ingestion.

### Current State

Same batch source pattern as Snowflake (see [section 5](#5-p1-snowflake-source-connector)).

### What Fennel Does

Fennel's BigQuery connector (`~/Projects/client/fennel/connectors/connectors.py:480-502`):

```python
class BigQuery(DataSource):
    project_id: str
    dataset_id: str
    service_account_key: Union[dict[str, str], Secret]
```

Requires `cursor` field for incremental reads. Uses `TableConnector`.

### Requirements

**SDK (`thyme-sdk`):**

1. Add `BigQuerySource` class to `thyme/connectors.py`:
   - Fields: `project_id: str`, `dataset_id: str`, `table: str`, `credentials_json: str = ""` (path to service account JSON or inline JSON string).
   - `to_dict()` returns `{"connector_type": "bigquery", "config": {...}}`.
2. Add `BigQueryConfig` to `thyme/config.py`:
   - Fields: `project_id`, `dataset_id`, `credentials_json`.
   - Env vars: `THYME_BIGQUERY_PROJECT`, `THYME_BIGQUERY_DATASET`, `THYME_BIGQUERY_CREDENTIALS`.
   - Factory: `Config.bigquery_source(table: str) -> BigQuerySource`.
3. Proto, compiler, bindings, exports (same pattern).

**Engine (Rust):**

4. Add BigQuery poll script using `google-cloud-bigquery` Python library via PyO3:
   - `client = bigquery.Client.from_service_account_json(credentials_path)`.
   - Query: `SELECT * FROM \`{project}.{dataset}.{table}\` WHERE CAST({cursor} AS STRING) > @cursor_value ORDER BY {cursor} LIMIT 50000`.
   - Use parameterized queries to prevent injection.

### Tests

**Local (thyme-sdk):**
- Unit tests for class, to_dict, compile_source, config factory.

**E2E (requires GCP project):**
- Seed BigQuery table -> ingest -> verify features.
- Test service account auth via `GOOGLE_APPLICATION_CREDENTIALS` env var fallback.

### Performance Considerations

- BigQuery has slot-based pricing. Large polls consume slots. Use `LIMIT` and reasonable `every` intervals.
- BigQuery queries are async by nature. The poll script should handle job completion polling.

---

## 7. P1: MySQL Source Connector

### Problem

MySQL is widely used for transactional data. Many feature engineering use cases need to ingest from MySQL (user profiles, transaction history, product catalogs).

### Current State

Thyme already has `PostgresSource`. MySQL follows the same pattern with minor SQL dialect differences.

### What Fennel Does

Fennel's MySQL connector (`~/Projects/client/fennel/connectors/connectors.py:600-618`): Inherits from `SQLSource` with `host`, `db_name`, `username`, `password`, `port=3306`, optional `jdbc_params`.

### Requirements

**SDK (`thyme-sdk`):**

1. Add `MySQLSource` class to `thyme/connectors.py`:
   - Fields: `host: str`, `port: int = 3306`, `database: str`, `table: str`, `user: str`, `password: str`.
   - `to_dict()` returns `{"connector_type": "mysql", "config": {...}}`.
2. Add `MySQLConfig` to `thyme/config.py`:
   - Fields mirror PostgresConfig pattern. Env vars: `THYME_MYSQL_HOST`, etc.
   - Factory: `Config.mysql_source(table: str) -> MySQLSource`.
3. Proto, compiler, bindings, exports.

**Engine (Rust):**

4. Add MySQL poll script using `pymysql` or `mysql-connector-python` via PyO3:
   - Follow Postgres pattern (`source.rs:346-388`) but use MySQL connection and syntax.
   - Query: `SELECT * FROM {table} WHERE CAST({cursor} AS CHAR) > %s ORDER BY {cursor} LIMIT 50000`.
   - Use parameterized queries (not string formatting) for SQL injection prevention.

### Tests

**Local (thyme-sdk):**
- Unit tests for class, to_dict, compile_source, config.

**E2E (Docker: add mysql:8 to docker-compose.yaml):**
- Seed MySQL table -> ingest -> verify features.
- Test incremental cursor advancement.
- Measure: poll latency for 50k rows (target: <10s).

### Performance Considerations

- MySQL `LIMIT` + `ORDER BY` on non-indexed cursor columns is slow. Document that the cursor column should be indexed.
- Connection pooling: use `itersize` equivalent (MySQL cursor doesn't have this natively; use `fetchmany(5000)` batching).

---

## 8. P1: Advanced Aggregations

### Problem

Thyme supports only 5 aggregations (Avg, Count, Sum, Min, Max). ML feature engineering commonly needs standard deviation, percentiles, last-N values, distinct counts, and exponential decay. Without these, users must compute features outside Thyme.

### Current State

- **SDK** (`thyme/pipeline.py:5-50`): `AggOp` base class with `of: str` and `window: str`. Subclasses: `Avg`, `Count`, `Sum`, `Min`, `Max`, each with `agg_type` string.
- **Proto** (`dataset.proto:54-59`): `AggSpec { string agg_type, string field, string window, string output_field }`. The `agg_type` is a free-form string — no enum, so adding new types requires no proto change.
- **Rust engine** (`operators.rs:83-103`): `AggType` enum with `Avg, Count, Sum, Min, Max`. `is_invertible()` returns true for `Sum, Count, Avg`. Aggregation computed in `compute_aggregate()` (lines 262-369) using accumulators (Sum/Count/Avg) or min-max B-tree scan (Min/Max).
- **RocksDB state** (`state.rs`): `acc:` prefix for invertible aggregations (AccumulatorValue: sum + count). `mm:` prefix for min/max (sorted f64 keys with live/tombstone markers). `dq:` prefix for delete-queue (window eviction scheduling).
- **MockContext** (`testing/_engine.py:33-67`): `_GroupState.compute_aggregate()` handles count/sum/avg/min/max with in-memory event lists and window filtering.

### Requirements

Each aggregation below follows the same implementation pattern. For each:

**SDK (`thyme-sdk`):**

Add a new class in `thyme/pipeline.py` extending `AggOp`. Export from `__init__.py`.

**Engine (`~/Projects/thyme`, Rust):**

Add variant to `AggType` enum. Implement computation in `process_record()`. Define state storage strategy.

**MockContext (`thyme-sdk`):**

Add case to `_GroupState.compute_aggregate()`.

### 8a. Stddev (Standard Deviation)

**SDK:** `Stddev(of: str, window: str)` with `agg_type = "stddev"`.

**Engine state:** Invertible. Store in `acc:` as extended `AccumulatorValue` with `sum`, `sum_sq` (sum of squares), and `count`. Stddev = sqrt((sum_sq/count) - (sum/count)^2). On window eviction, subtract the evicted value from both `sum` and `sum_sq`.

**MockContext:** Compute using `statistics.pstdev()` or manual formula on in-window values.

### 8b. Quantile (Approximate Percentile)

**SDK:** `Quantile(of: str, window: str, p: float)` with `agg_type = "quantile"`. `p` is in [0.0, 1.0]. Add `p` to `AggOp` base class as optional field (default None), and include it in the agg_spec dict as `"p"`.

**Proto:** Add optional `double p = 5` to `AggSpec` message.

**Engine state:** Use t-digest or DDSketch algorithm for approximate quantile computation. Store digest state in a dedicated `qt:` RocksDB prefix (serialized as bytes). Not invertible — on window eviction, rebuild from remaining events (or use a time-bucketed approach).

**MockContext:** Use `statistics.quantiles()` or sorted list + index lookup on in-window values.

**Performance note:** This is the most expensive new aggregation. T-digest merge operations are O(compression_param). Use compression=100 as default. Consider time-bucketed eviction where the digest is rebuilt periodically rather than per-event.

### 8c. Distinct

**SDK:** `Distinct(of: str, window: str)` with `agg_type = "distinct"`. Returns the count of distinct values (float).

**Engine state:** Store in a dedicated `ds:` RocksDB prefix as a set/HyperLogLog. For exact: use a sorted set with value keys (similar to mm: pattern). For approximate: use HyperLogLog (fixed 12KB memory per group).

**MockContext:** `len(set(values_in_window))`.

### 8d. LastK

**SDK:** `LastK(of: str, window: str, k: int, dedup: bool = False)` with `agg_type = "lastk"`. Add `k` and `dedup` as optional fields on `AggOp`. Returns a list of the last K values.

**Proto:** Add `optional int32 k = 6` and `optional bool dedup = 7` to `AggSpec`.

**Engine state:** Store in `lk:` prefix. Key: `lk:{group}:{agg_idx}:{timestamp_be8}:{seq_be8}`. Value: the field value (bytes). On read, reverse-scan the prefix and take first K entries. On window eviction, delete expired entries.

**Data type note:** LastK returns a list, not a float. This requires extending the state value to support list-typed features. The `StateValue.features` map is `map<string, double>` — add a `map<string, string> list_features = 7` for JSON-encoded list values.

**MockContext:** Sort in-window events by timestamp descending, take first K, optionally dedup.

### 8e. FirstK

**SDK:** `FirstK(of: str, window: str, k: int, dedup: bool = False)` with `agg_type = "firstk"`. Same pattern as LastK but forward-scan.

### 8f. ExpDecaySum (Exponential Decay)

**SDK:** `ExpDecaySum(of: str, half_life: str)` with `agg_type = "exp_decay_sum"`. `half_life` is a duration string (e.g., `"1h"`, `"7d"`). No window — decay is implicit. Add `half_life` as optional field on `AggOp`.

**Proto:** Add `optional string half_life = 8` to `AggSpec`.

**Engine state:** Invertible with modification. Store in `acc:` with `sum` (decayed running sum) and `last_ts` (timestamp of last update). On new event: `decayed_sum = old_sum * 2^(-(t_new - last_ts) / half_life) + new_value`. No window eviction needed — values naturally decay to zero.

**MockContext:** For each event in history, compute `value * 2^(-(t_query - t_event) / half_life)` and sum.

**Performance note:** This is the cheapest advanced aggregation — no window eviction, no auxiliary state, O(1) per event. Excellent for recency-weighted features.

### Tests (all aggregations)

**Local (thyme-sdk):**
- Each new class: `__init__`, `agg_type` string, inheritance from `AggOp`.
- `PipelineNode.aggregate()` accepts new types.
- `compile_pipeline()` produces correct `AggSpec` proto with new fields.
- `MockContext` computes correct values for each aggregation with known inputs.
- Window boundary tests: values outside window excluded.
- Empty group returns sensible default (0.0 for Stddev, 0.0 for Quantile, 0 for Distinct, [] for LastK/FirstK, 0.0 for ExpDecaySum).

**E2E:**
- For each aggregation: ingest events -> query features -> verify values.
- Stddev: 5 events with known values, verify against manual stddev calculation.
- Quantile: 100 events, verify p50 within 5% of actual median.
- ExpDecaySum: events at known timestamps, verify decay calculation.
- Performance: 100k events per partition, measure aggregation throughput (target: >50k events/s per partition for invertible aggs).

---

## 9. P1: Sink Connectors

### Problem

Thyme computed features only live in RocksDB (online serving) and Kafka output topics (internal). Users need features to flow back to warehouses for batch training, to Kafka for downstream services, and to HTTP endpoints for real-time integrations.

### Current State

- **Engine output** (`operators.rs:105-114`): `ProcessResult` contains `entity_outputs: Vec<OutputRecord>` that are produced to an output Kafka topic. The output topic is `{entity_type}_topic`.
- **No sink abstraction** in SDK or proto. No `@sink` decorator.

### What Fennel Does

Fennel's sink decorator (`~/Projects/client/fennel/connectors/connectors.py:247-369`):

```python
@sink(
    conn=KafkaConnector | S3Connector | SnowflakeConnector | HTTPConnector,
    cdc="debezium" | None,
    every=Duration | None,
    how="incremental" | "recreate",
    renames={},
    since=datetime | None,
    until=datetime | None,
)
```

Supports Kafka (debezium CDC), S3 (Delta format), Snowflake (incremental), HTTP (debezium CDC).

### Requirements

**SDK (`thyme-sdk`):**

1. Create `thyme/sinks.py`:
   - `KafkaSink(bootstrap_servers: str, topic: str, format: str = "json", security_protocol: str = "PLAINTEXT")`.
   - `S3Sink(bucket: str, prefix: str = "", region: str = "us-east-1", format: str = "json")` — write computed features as JSON/Parquet files to S3.
   - `HttpSink(url: str, method: str = "POST", headers: dict = {})` — POST feature updates to a webhook.
   - Each has `to_dict()` returning `{"sink_type": str, "config": dict}`.
2. Add `@sink` decorator to `thyme/sinks.py`:
   - Signature: `@sink(connector: KafkaSink | S3Sink | HttpSink, every: str = "", cdc: str = "")`.
   - Stores in `_SINK_REGISTRY` keyed by dataset class name.
   - Attaches `_sink_meta` to decorated class.
3. Add `Sink` proto message to a new `sink.proto` (or extend `connector.proto`):
   - `Sink { string dataset, KafkaSink kafka, S3Sink s3, HttpSink http }` with oneof.
4. Add `repeated Sink sinks` to `CommitRequest` in `services.proto`.
5. Update `compiler.py` with `compile_sink()`.
6. Export `sink`, `KafkaSink`, `S3Sink`, `HttpSink` from `__init__.py`.

**Engine (`~/Projects/thyme`, Rust):**

7. After `process_record()` produces to the output topic, add a sink step:
   - **KafkaSink:** Produce the output record to the user's target topic (in addition to or instead of the internal output topic). Use rdkafka producer with the sink's broker config.
   - **S3Sink:** Buffer output records and flush to S3 as JSON/Parquet files on a timer (`every` interval). Use `object_store` crate (already a dependency).
   - **HttpSink:** POST output records to the configured URL. Buffer and batch to avoid per-record HTTP calls. Retry with exponential backoff on failure.
8. Sink execution should be async and not block the main processing pipeline.

### Tests

**Local (thyme-sdk):**
- `KafkaSink/S3Sink/HttpSink` classes: `to_dict()` shape.
- `@sink` decorator registers in `_SINK_REGISTRY`.
- `compile_sink()` produces valid proto.
- `CommitRequest` includes sinks.

**E2E:**
- **KafkaSink:** Pipeline produces features -> sink writes to target Redpanda topic -> consume and verify messages.
- **S3Sink:** Pipeline produces features -> sink writes to MinIO bucket -> list objects and verify content.
- **HttpSink:** Start a simple HTTP server -> pipeline produces -> sink POSTs -> verify received payloads.
- Measure: sink latency and throughput. KafkaSink target: <100ms added latency. S3Sink: batch writes every `every` interval.

### Performance Considerations

- Sinks must not back-pressure the main processing loop. Use async channels to decouple.
- S3 writes should be batched (e.g., 1000 records or 1MB, whichever comes first) to avoid excessive small-file writes.
- HTTP sinks need circuit breaker logic to avoid blocking when the target is down.

---

## 10. P2: Tumbling, Hopping, and Session Windows

### Problem

Thyme only supports sliding (continuous) windows. Tumbling windows are needed for "per-hour" or "per-day" aligned aggregations. Hopping windows enable overlapping time buckets. Session windows are critical for user behavior analysis.

### Current State

- **SDK** (`thyme/pipeline.py:5-10`): `AggOp.__init__(of, window)` where `window` is a duration string like `"30d"`.
- **Rust engine** (`operators.rs:8-43`): `parse_window_duration()` returns `u64` seconds. Window eviction logic (`operators.rs:591-632`) uses `fire_at = event_ts + window_secs` — this is a sliding window semantic.
- **MockContext** (`testing/_window.py:4-30`): `parse_window_duration()` returns seconds. `_GroupState.compute_aggregate()` (lines 43-67) filters events where `ts >= reference_time - window_secs`.
- **RocksDB delete queue** (`state.rs`): `dq:` entries with `fire_at` for sliding window eviction.

### What Fennel Does

Fennel supports four window types as first-class objects (`~/Projects/client/fennel/dtypes/dtypes.py:352-516`):

```python
Continuous(duration="1h")           # Sliding window
Tumbling(duration="1h", lookback="0s")  # Fixed non-overlapping buckets
Hopping(duration="1h", stride="15m", lookback="0s")  # Overlapping with stride
Session(gap="30m")                  # Gap-based sessions
```

These are passed as the `window` parameter to aggregation operators.

### Requirements

**SDK (`thyme-sdk`):**

1. Create `thyme/windows.py`:
   - `SlidingWindow(duration: str)` — current behavior, renamed for clarity.
   - `TumblingWindow(duration: str)` — fixed non-overlapping time buckets aligned to epoch (e.g., `"1h"` = hourly buckets aligned to :00).
   - `SlidingWindow(duration: str)` — alias for current behavior.
   - `HoppingWindow(duration: str, stride: str)` — overlapping windows. `stride` <= `duration`. Each event belongs to `ceil(duration / stride)` windows.
   - `SessionWindow(gap: str)` — events within `gap` of each other belong to the same session. A new session starts when `gap` elapses with no events.
   - All have a `to_dict() -> dict` method for serialization.
2. Update `AggOp` to accept `window: str | SlidingWindow | TumblingWindow | HoppingWindow | SessionWindow`.
   - If `window` is a plain string, treat as `SlidingWindow(duration=window)` for backward compatibility.
3. Update `AggSpec` dict to include `"window_type": str` and `"window_params": dict` alongside the existing `"window"` duration string.
4. **Proto changes:**
   - Add `optional string window_type = 5` to `AggSpec` (values: `"sliding"`, `"tumbling"`, `"hopping"`, `"session"`; default `"sliding"`).
   - Add `optional string window_stride = 6` (for hopping).
   - Add `optional string window_gap = 7` (for session).
5. Update `compile_pipeline()` to include new AggSpec fields.
6. Export window classes from `__init__.py`.

**Engine (`~/Projects/thyme`, Rust):**

7. **Tumbling windows:** Align events to bucket boundaries. Bucket start = `event_ts - (event_ts % duration_secs)`. Store aggregations per bucket. On query, return the current (or most recent completed) bucket's value.
8. **Hopping windows:** Each event belongs to multiple buckets. Bucket starts at `floor(event_ts / stride) * stride - duration + stride` through `floor(event_ts / stride) * stride`. Maintain separate accumulators per bucket.
9. **Session windows:** Track session boundaries per group key. New event within `gap` of the last event extends the current session. Otherwise, close the current session and start a new one. Store session state in `sess:` RocksDB prefix.

**MockContext:**

10. Update `_GroupState.compute_aggregate()`:
    - Sliding: current behavior (events in `[ref_time - duration, ref_time]`).
    - Tumbling: bucket events into fixed intervals, return latest completed bucket's aggregation.
    - Hopping: compute aggregation for each overlapping window, return the latest window's value.
    - Session: group events into sessions based on gap, aggregate the latest session.

### Tests

**Local (thyme-sdk):**
- Window class construction and validation (`HoppingWindow` rejects stride > duration).
- Backward compatibility: `Avg(of="x", window="30d")` still works.
- Tumbling MockContext: events at :00, :15, :30, :45 in a `"1h"` tumbling window all aggregate together.
- Hopping MockContext: events in overlapping windows contribute to multiple buckets.
- Session MockContext: events with >gap between them start new sessions.
- `compile_pipeline()` includes window_type and params in AggSpec proto.

**E2E:**
- Tumbling: hourly aggregation, verify bucket boundaries align to clock hours.
- Hopping: 1h window with 15m stride, verify 4 overlapping window values.
- Session: events with varying gaps, verify session boundaries.
- Performance: tumbling/hopping should not significantly degrade throughput vs sliding (target: <20% overhead).

### Performance Considerations

- **Tumbling** is actually simpler than sliding — no eviction needed, just bucket rotation. Should be faster.
- **Hopping** multiplies state by `ceil(duration / stride)`. A 1h window with 1m stride = 60x state. Document maximum practical overlap ratio.
- **Session** windows require maintaining per-group session state. Unbounded session growth (no max duration) can cause memory issues. Consider adding an optional `max_duration` parameter.

---

## 11. P2: Expression Engine

### Problem

Users need to compute derived fields, apply math/string/datetime operations, and build complex predicates without writing full Python functions. An expression engine enables optimized, serializable, push-down-capable transformations.

### Current State

- Filter and Transform operators (tasks 3-4) accept Python callables. This works but has limitations: Python functions are opaque to the optimizer, can't be pushed down to sources, and require PyO3 per-record overhead.
- Fennel has a comprehensive expression engine (`~/Projects/client/fennel/expr/expr.py`) with 100+ operations, type inference, and proto serialization.

### What Fennel Does

Fennel's expression engine supports:
- **Math:** `abs`, `ceil`, `floor`, `round`, `sqrt`, `pow`, `log`, trig functions.
- **String:** `lower`, `upper`, `contains`, `startswith`, `endswith`, `split`, `concat`, `len`, `json_extract`.
- **DateTime:** `year`, `month`, `day`, `hour`, `minute`, `second`, `timestamp`, `strftime`.
- **Dict/Struct:** `get`, `contains`, `len`, field access.
- **Control flow:** `when(cond).then(val).otherwise(default)`.
- **Null handling:** `isnull`, `fillnull`.
- **Column references:** `col("name")`.

### Requirements

This is a large task. Implement in phases:

**Phase 1: Core expression types and column references.**

1. Create `thyme/expr.py`:
   - `col(name: str) -> Expr` — column reference.
   - `Expr` base class with operator overloading: `+`, `-`, `*`, `/`, `==`, `!=`, `<`, `>`, `<=`, `>=`, `&` (and), `|` (or), `~` (not).
   - `Expr.isnull() -> Expr`, `Expr.fillnull(value) -> Expr`.
   - `literal(value) -> Expr` — constant value.
2. Add `to_dict()` serialization on all Expr nodes (tree of dicts).
3. Add `evaluate(expr: Expr, record: dict) -> Any` function for MockContext evaluation.
4. Integrate with `PipelineNode.filter()` and `.transform()`:
   - `node.filter(col("amount") > 0)` as alternative to `node.filter(lambda e: e["amount"] > 0)`.
   - `node.transform({"derived_field": col("a") + col("b")})` for dict-based transforms.

**Phase 2: Math, string, datetime operations.**

5. `Expr.num` property returning `NumExpr` with: `abs()`, `round(n)`, `ceil()`, `floor()`, `sqrt()`.
6. `Expr.str` property returning `StrExpr` with: `lower()`, `upper()`, `contains(s)`, `startswith(s)`, `endswith(s)`, `len()`.
7. `Expr.dt` property returning `DateTimeExpr` with: `year`, `month`, `day`, `hour`, `minute`, `second`.

**Phase 3: Proto serialization and Rust-native evaluation.**

8. Define `Expr` proto message with recursive structure.
9. Implement Rust-native expression evaluation (no PyO3 overhead).

### Tests

**Local (thyme-sdk):**
- Phase 1: `col("x") + col("y")` produces correct expression tree. `evaluate()` on a record returns correct result. Filter with expression works in MockContext.
- Phase 2: String/math/datetime operations produce correct results.
- Edge cases: null handling, type mismatches, division by zero.

**E2E:**
- Pipeline with expression-based filter -> aggregate -> verify features.
- Measure: expression evaluation vs Python lambda evaluation overhead (target: Rust-native expressions 10x faster than PyO3 lambda).

### Performance Considerations

- Phase 1-2 still uses Python evaluation (in MockContext and via PyO3 in engine). Acceptable for correctness.
- Phase 3 (Rust-native) is the performance payoff. Eliminates PyO3 GIL contention and function call overhead. Target: <1us per expression evaluation per record.
- Expression trees should be compiled to a flat bytecode or closure chain at pipeline init, not interpreted per record.

---

## 12. P2: Embedding and Decimal Data Types

### Problem

ML features increasingly involve vector embeddings (recommendation models, semantic search, RAG). Financial features require precise decimal arithmetic. Thyme's type system (`int`, `float`, `str`, `bool`, `datetime`, `Optional`, `List`, `Map`) lacks both.

### Current State

- **SDK type mapping** (`thyme/compiler.py:14-24`): `TYPE_MAP` maps Python type name strings to proto `DataType`. Only basic types.
- **Proto schema** (`schema.proto:5-35`): `DataType` oneof with 8 variants (bool, int, float, string, timestamp, optional, list, map).
- **Dataset field processing** (`thyme/dataset.py:20-43`): `_type_to_string()` converts Python annotations to strings. `_is_optional()` detects `Optional[T]`.

### What Fennel Does

Fennel's type system (`~/Projects/client/fennel/dtypes/dtypes.py`):

```python
Embedding[256]     # Vector of floats with fixed dimension
Decimal[2]         # Decimal with 2 decimal places (scale 0-28)
```

Plus constrained types: `between(int, 0, 100)`, `oneof(str, ["a", "b"])`, `regex(r"^\d+$")`.

### Requirements

**Embedding:**

1. Add `Embedding` class to a new `thyme/dtypes.py`:
   - `Embedding[dim]` syntax via `__class_getitem__`: returns `_Embedding(dim=dim)`.
   - Stored as `List[float]` internally but with dimension validation.
   - Type string: `"embedding[256]"` (parseable by compiler).
2. Add `EmbeddingType { int32 dim = 1; }` to `schema.proto` DataType oneof.
3. Update `TYPE_MAP` in `compiler.py` to handle `"embedding[N]"` pattern.
4. Update `_type_to_string()` in `dataset.py` to recognize `_Embedding` instances.
5. RocksDB storage: embeddings stored as JSON-encoded float arrays in `StateValue`. Add `map<string, bytes> embeddings = 8` to `StateValue` for efficient binary storage (packed f32 arrays).

**Decimal:**

6. Add `Decimal` class to `thyme/dtypes.py`:
   - `Decimal[scale]` via `__class_getitem__`: returns `_Decimal(scale=scale)`.
   - Scale 0 is invalid (use int). Max scale: 28.
   - Type string: `"decimal[2]"`.
7. Add `DecimalType { int32 scale = 1; }` to `schema.proto` DataType oneof.
8. Update compiler type mapping.
9. Engine: store as string-encoded fixed-point in RocksDB to avoid floating-point precision loss. Aggregations (Sum, Avg) on Decimal fields must use exact arithmetic.

### Tests

**Local (thyme-sdk):**
- `Embedding[256]` creates correct type object. Dimension must be positive int.
- `Decimal[2]` creates correct type object. `Decimal[0]` raises error. `Decimal[29]` raises error.
- Dataset with `embedding: Embedding[128] = field()` compiles to correct proto DataType.
- Dataset with `price: Decimal[2] = field()` compiles correctly.
- MockContext handles embedding and decimal fields in events.

**E2E:**
- Ingest events with embedding fields -> materialize -> query returns correct vectors.
- Ingest events with decimal fields -> aggregate (Sum) -> verify no precision loss vs float.
- Measure: embedding storage overhead for 256-dim vectors (target: <1.5KB per vector in RocksDB).

### Performance Considerations

- Embeddings are large (256 floats = 1KB). Avoid storing embeddings in `features` map (which is double-valued). Use dedicated binary storage.
- Decimal aggregations with exact arithmetic are slower than f64. For Sum, use integer arithmetic on scaled values (e.g., $12.34 stored as 1234 with scale 2).

---

## 13. P3: Metadata System

### Problem

As feature count grows, teams need governance: who owns a feature, what does it do, is it deprecated. Without metadata, the platform becomes a black box of unnamed aggregations.

### Current State

- No metadata system in Thyme SDK or proto.
- Fennel's `@meta` decorator (`~/Projects/client/fennel/lib/metadata/metadata.py:30-83`) supports `owner` (email-validated), `description`, `tags`, `deprecated`, `deleted` on datasets, featuresets, features, pipelines.

### Requirements

**SDK (`thyme-sdk`):**

1. Create `thyme/metadata.py`:
   - `@meta(owner: str = "", description: str = "", tags: list[str] = [], deprecated: bool = False)` decorator.
   - Validate `owner` is a valid email if non-empty.
   - Can be applied to `@dataset` classes, `@featureset` classes, and individual `@pipeline` methods.
   - Stores `_meta` attribute on the decorated object.
2. Add `Metadata` proto message (new `metadata.proto` or extend existing):
   - `string owner`, `string description`, `repeated string tags`, `bool deprecated`.
3. Include metadata in `CommitRequest`:
   - Add `optional Metadata metadata` field to `Dataset`, `Pipeline`, and `Featureset` proto messages.
4. Update `compile_dataset()`, `compile_pipeline()`, `compile_featureset()` to include metadata.
5. Update `_build_schema()` in `dataset.py` to extract and store metadata.

**Engine/definition-service:**

6. Store metadata in Postgres alongside entity definitions.
7. Expose metadata in `GET /features` response (optional `include_metadata=true` query param).

### Tests

**Local (thyme-sdk):**
- `@meta(owner="alice@co.com")` sets `_meta` attribute.
- Invalid email raises `ValueError`.
- Metadata included in compiled proto.
- `deprecated=True` sets flag in proto. Definition service can warn or reject deprecated entities.
- Stacking: `@meta` + `@dataset` + `@source` all work together.

**E2E:**
- Commit dataset with metadata -> query definition-service -> verify metadata stored.
- Deprecated dataset: commit succeeds but logs a warning.

### Performance Considerations

- Metadata is commit-time only — zero runtime overhead. No impact on event processing, aggregation, or query latency.

---

## 14. P3: Index Modes (Online/Offline)

### Problem

Thyme has a basic `indexed: bool` on datasets. It doesn't distinguish between online serving (low-latency, latest value) and offline serving (point-in-time historical lookups for training data). Different access patterns need different storage and query optimization.

### Current State

- **SDK** (`thyme/dataset.py:169-183`): `@dataset(index=True)` sets `indexed` bool in schema.
- **Proto** (`dataset.proto:8-15`): `bool indexed = 4` on Dataset message.
- **Query server** (`~/Projects/thyme/crates/query-server/src/api.rs:65-148`): `GET /features` already supports both latest (online) and timestamp-based (offline) queries. Response includes `"mode": "online" | "offline"`.
- **RocksDB reader** (`reader.rs:227-249`): `get_latest()` scans all partitions for highest key. `get_at_timestamp()` seeks to specific timestamp.

### What Fennel Does

Fennel's `@index` decorator (`~/Projects/client/fennel/datasets/datasets.py:2124-2194`):

```python
@index(type="primary", online=True, offline="forever")
```

- `online: bool` — enable low-latency latest-value serving.
- `offline: str` — `"forever"` or duration string for historical retention.

### Requirements

**SDK (`thyme-sdk`):**

1. Replace `index: bool` parameter on `@dataset` with `@index` decorator in `thyme/dataset.py` or new `thyme/index.py`:
   - `@index(online: bool = True, offline: str = "forever")`.
   - `offline="none"` disables offline. `offline="forever"` retains all history. `offline="30d"` retains 30 days.
   - Backward compatible: `@dataset(index=True)` maps to `@index(online=True, offline="forever")`.
2. Update Dataset proto:
   - Replace `bool indexed` with `IndexConfig { bool online, string offline_retention }`.
3. Compile and serialize index config.

**Engine (`~/Projects/thyme`, Rust):**

4. **Online optimization:** When `online=True`, maintain a separate `latest:` RocksDB prefix with only the most recent value per entity. This avoids the full prefix scan in `get_latest()` (which currently scans all timestamped keys).
5. **Offline TTL:** When `offline` has a duration, configure RocksDB TTL or add a background compaction filter that drops keys older than the retention period.
6. **Query server:** Use `latest:` prefix for online queries (single-key lookup instead of prefix scan). Use timestamped keys for offline queries.

### Tests

**Local (thyme-sdk):**
- `@index(online=True, offline="30d")` stores correct config.
- `@dataset(index=True)` backward compatible with default index config.
- Proto includes IndexConfig.

**E2E:**
- Online query: single lookup latency (target: <5ms p99 for `latest:` prefix).
- Offline query: point-in-time lookup at historical timestamp.
- Offline TTL: ingest old events, verify they're cleaned up after retention period.
- Measure: online query latency improvement from `latest:` prefix vs prefix scan.

### Performance Considerations

- `latest:` prefix is the key optimization. It converts online queries from O(n_timestamps) prefix scan to O(1) point lookup.
- Offline TTL compaction reduces disk usage for long-running pipelines. Without it, RocksDB grows unbounded.
- The `latest:` key must be updated atomically with the timestamped key in the same `WriteBatch`.

---

## 15. P3: Source-Level Preprocessing, Filtering, and Sampling

### Problem

Currently, all data from a source is ingested and processed. For large sources, users waste compute processing irrelevant records. Source-level filtering and sampling reduce data volume before it enters the pipeline.

### Current State

- **`@source` decorator** (`thyme/connectors.py:93-128`): Parameters: `connector`, `cursor`, `every`, `disorder`, `cdc`. No `preproc`, `where`, or `sample`.
- **Rust source ingestion** (`source.rs`): Poll scripts execute SQL queries (Postgres) or DuckDB scans (S3/Iceberg). Filtering could be pushed into these queries.

### What Fennel Does

Fennel's `@source` decorator includes (`~/Projects/client/fennel/connectors/connectors.py:113-244`):

```python
@source(
    conn=...,
    preproc={"new_col": ref("data[nested][field]"), "const": "value"},
    where=lambda df: df[df.status == "active"],
    sample=Sample(rate=0.1, using=["user_id"]),
)
```

- **preproc:** Column renames, nested JSON extraction (`ref("a[b][c]")`), constant injection.
- **where:** Row-level predicate pushed to source query.
- **sample:** Probabilistic or deterministic (hash-based by column) sampling.

### Requirements

**SDK (`thyme-sdk`):**

1. Add `where` parameter to `@source` decorator:
   - `where: str = ""` — SQL predicate string (e.g., `"status = 'active' AND amount > 0"`).
   - Stored in source metadata and passed to the poll script.
   - For Postgres/MySQL: appended as `AND ({where})` to the poll query.
   - For S3/Iceberg: appended to DuckDB query as `WHERE` clause.
2. Add `sample` parameter:
   - `sample: float = 1.0` — sampling rate in [0.0, 1.0]. 1.0 = no sampling.
   - For Postgres/MySQL: `TABLESAMPLE BERNOULLI({rate * 100})` or `WHERE random() < {rate}`.
   - For S3/Iceberg: DuckDB `USING SAMPLE {rate * 100}%`.
3. Add `preproc` parameter:
   - `preproc: dict[str, str] = {}` — column mapping. Keys are output column names, values are source column expressions (SQL expressions or `"source_col"` references).
   - Translates to `SELECT {val} AS {key}, ...` in the poll query.
4. Store all three in source metadata dict and `_SOURCE_REGISTRY`.
5. Add fields to `Source` proto message: `string where_clause`, `double sample_rate`, `map<string, string> preproc`.
6. Update `compile_source()`.

**Engine (`~/Projects/thyme`, Rust):**

7. Update poll scripts in `source.rs` to incorporate:
   - `WHERE` clause injection (append to existing cursor-based WHERE).
   - Sampling (add to SQL query or DuckDB scan).
   - Preproc column mapping (modify SELECT clause).
8. Use parameterized queries where possible. For `where` clauses that are user-provided SQL, validate against a safe subset (no subqueries, no DDL keywords).

### Tests

**Local (thyme-sdk):**
- `@source` with `where="amount > 0"` stores in metadata.
- `@source` with `sample=0.5` stores rate.
- `@source` with `preproc={"total": "price * quantity"}` stores mapping.
- `compile_source()` includes new fields in proto.
- Validation: `sample` must be in [0.0, 1.0]. `sample=0.0` rejects all. `sample=1.5` raises ValueError.

**E2E:**
- **where:** Seed Postgres with 100 rows (50 matching filter). Verify only 50 ingested.
- **sample:** Seed with 10000 rows, sample=0.1. Verify ~1000 ingested (within statistical bounds).
- **preproc:** Seed with columns A, B. Preproc: `{"total": "A + B"}`. Verify derived column available in pipeline.
- Measure: filtering at source vs pipeline-level filter. Source-level should reduce Kafka message volume proportionally.

### Performance Considerations

- Source-level filtering is always a win — it reduces Kafka writes, network transfer, and engine processing.
- SQL injection risk with `where` parameter. Implement a SQL validation/sanitization layer. At minimum, reject semicolons, `DROP`, `DELETE`, `INSERT`, `UPDATE`, `CREATE`, `ALTER` keywords.
- Sampling at source is approximate. Hash-based deterministic sampling (by entity key) is preferable for reproducibility but requires the entity key to be known at source level.

---

## Implementation Order

```
Phase 1 (P0 — Critical Path):
  3. Filter Operator          ─┐
  4. Transform Operator        ├─ Can be done in parallel
  1. Kafka Source Connector   ─┘
  2. Kinesis Source Connector  ─── After Kafka (similar pattern)

Phase 2 (P1 — Production Readiness):
  5. Snowflake Connector      ─┐
  6. BigQuery Connector        ├─ Independent, parallel
  7. MySQL Connector          ─┘
  8. Advanced Aggregations     ─── Independent (large task, split by sub-type)
  9. Sink Connectors           ─── After Kafka connector (shared infra)

Phase 3 (P2 — Competitive Features):
  10. Window Types             ─── After advanced aggregations
  11. Expression Engine        ─── After filter/transform operators
  12. Embedding/Decimal Types  ─── Independent

Phase 4 (P3 — Governance & Optimization):
  13. Metadata System          ─┐
  14. Index Modes              ├─ Independent, parallel
  15. Source Preprocessing     ─┘
```

Each task can be picked up independently. Dependencies are noted where they exist. For maximum velocity, assign Phase 1 tasks to parallel agents.
