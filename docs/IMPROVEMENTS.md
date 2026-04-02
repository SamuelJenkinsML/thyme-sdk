# Thyme SDK: Structural Improvements

Findings from a full code review of the thyme-sdk, cross-referenced with patterns from the Fennel feature platform client. Each task is self-contained with enough context for an agent to implement independently.

---

## 1. Introduce a Base Connector Class (ABC)

**Problem:** `IcebergSource`, `PostgresSource`, and `S3JsonSource` are duck-typed — they all implement `to_dict()` but share no base class. There's no enforced contract, no way to do `isinstance` checks, and IDE support for connectors is limited.

**What Fennel does:** All connectors extend `DataSource(BaseModel)` with enforced methods (`type()`, `identifier()`, `required_fields()`), giving a clear extension contract.

**Task:** Create an abstract base class `Connector` in `thyme/connectors.py`.

```python
from abc import ABC, abstractmethod

class Connector(ABC):
    """Base class for all source connectors."""

    @abstractmethod
    def to_dict(self) -> dict:
        """Return a dict with 'connector_type' and 'config' keys."""
        ...

    @property
    @abstractmethod
    def connector_type(self) -> str:
        """Return the connector type string (e.g. 'postgres', 'iceberg')."""
        ...
```

**Files to modify:**
- `thyme/connectors.py` — Add `Connector` ABC, make `IcebergSource`, `PostgresSource`, `S3JsonSource` extend it. Extract `connector_type` from each `to_dict()` into a property. Update `source()` type annotation from `Any` to `Connector`.
- `thyme/config.py` — Update return type annotations on factory methods to `Connector`.
- `tests/test_connectors.py` — Add test that custom connector subclass works, that non-Connector raises `TypeError` in `@source`.

**Acceptance criteria:**
- `@source(non_connector_object, ...)` raises `TypeError` at decoration time.
- All three existing connectors pass `isinstance(x, Connector)`.
- `connector_type` property returns correct string for each.

---

## 2. Separate DataSource from DataConnector (Two-Layer Connector Model)

**Problem:** Currently each connector class (e.g. `PostgresSource`) bundles connection config *and* table selection into one object. This means you can't reuse a single Postgres connection across multiple datasets — you must duplicate host/port/user/password for each table.

**What Fennel does:** Separates `DataSource` (connection credentials) from `DataConnector` (what to read). A `Postgres` data source creates a `TableConnector` via `.table("users", cursor="updated_at")`. One connection config, many tables.

**Task:** Refactor the connector model into two layers.

**New API:**
```python
# Define the connection once
pg = PostgresSource(host="db.prod", port=5432, database="app", user="...", password="...")

# Reference specific tables
@source(pg.table("orders"), cursor="updated_at", every="5m", disorder="1h")
@dataset(version=1)
class Orders:
    ...

@source(pg.table("users"), cursor="modified_at", every="10m", disorder="2h")
@dataset(version=1)
class Users:
    ...
```

**Files to modify:**
- `thyme/connectors.py`:
  - Rename current classes to represent the connection layer: `PostgresSource` stays but loses `table` from `__init__`. Add `PostgresSource.table(table: str, schema: str = "public") -> PostgresTableConnector`.
  - Create `PostgresTableConnector(Connector)` that holds a reference to the parent `PostgresSource` + table name. Its `to_dict()` merges parent config + table.
  - Same pattern for `IcebergSource.table(table: str)` and `S3JsonSource.prefix(prefix: str)` (or keep S3 as-is since prefix is already in constructor).
  - Keep backward compatibility: if `table` is passed to `PostgresSource.__init__`, it still works (emit deprecation warning).
- `thyme/compiler.py` — `compile_source()` unchanged (it works on dicts, so if `to_dict()` output is the same, no changes needed).
- `thyme/config.py` — `postgres_source()` returns the connection-only `PostgresSource` (no table). Add `Config.postgres_table(table, schema)` convenience method.
- `tests/` — Add test for multi-table reuse from single source. Test backward compat.

**Acceptance criteria:**
- Single `PostgresSource` can be reused across multiple `@source` decorators with different tables.
- `to_dict()` output is identical to current format (no compiler changes needed).
- Old API (`PostgresSource(... table="orders")`) still works with deprecation warning.

---

## 3. Add Secret/Credential Reference Support

**Problem:** Credentials (Postgres password, S3 keys, API tokens) are passed as plain strings. Users must either hardcode them, use env vars manually, or rely on the Config layer. There's no way to reference a secret manager from within a connector definition, meaning feature definitions can't be committed to version control safely.

**What Fennel does:** Has a `Secret` class that references AWS Secrets Manager by ARN + JSON path. Fields accept `Union[str, Secret]`. The backend resolves secrets at runtime, so the SDK never sees plaintext.

**Task:** Add a `SecretRef` class that represents a reference to an external secret, resolved by the engine at runtime.

```python
class SecretRef:
    """Reference to a secret stored in an external secret manager."""
    def __init__(self, provider: str, key: str, field: str = ""):
        self.provider = provider  # "aws_secrets_manager", "env", "vault"
        self.key = key            # ARN, env var name, or Vault path
        self.field = field        # JSON field within the secret

    def __getitem__(self, field: str) -> "SecretRef":
        """Chain field access: secret["db"]["password"]"""
        return SecretRef(self.provider, self.key, field=field)
```

**Files to modify:**
- `thyme/connectors.py`:
  - Add `SecretRef` class.
  - Add convenience constructors: `aws_secret(arn: str) -> SecretRef`, `env_secret(var_name: str) -> SecretRef`.
  - Update `PostgresSource.__init__` type hints: `user: str | SecretRef`, `password: str | SecretRef`.
  - Update `to_dict()` to serialize `SecretRef` as `{"__secret__": {"provider": ..., "key": ..., "field": ...}}` instead of the plain string value.
- `thyme/compiler.py` — Update `compile_source()` to handle secret refs in config dict. Add proto serialization if `connector_pb2` has a `SecretRef` message; otherwise serialize as the dict marker (engine parses it).
- `proto/thyme/connector.proto` (in main thyme repo) — Add `SecretRef` message type and optional secret fields to `PostgresSource`. *(Note: proto changes are cross-repo; SDK task is to support the new format.)*
- `tests/test_connectors.py` — Test `SecretRef` serialization, chained field access, mixed plain/secret credentials.

**Acceptance criteria:**
- `PostgresSource(... password=aws_secret("arn:aws:sm:us-east-1:123:secret/db"))` serializes to a dict with `__secret__` marker instead of plaintext.
- `SecretRef["field"]` chaining works.
- Plain string credentials still work unchanged.

---

## 4. Add Sink/Egress Support

**Problem:** The SDK only supports sources (ingestion). There's no way to define where processed features should be written to (Kafka topics, S3, Postgres tables, HTTP endpoints). Users who need to push features downstream have no SDK-level abstraction.

**What Fennel does:** Has a `@sink` decorator that mirrors `@source` with target-specific constraints (Kafka sinks require `cdc="debezium"`, S3 sinks require Delta format, etc.).

**Task:** Add `@sink` decorator and sink connector classes.

**New API:**
```python
kafka_sink = KafkaSink(brokers="kafka:9092", topic="features-out", format="json")

@sink(kafka_sink, cdc="debezium", every="1m")
@dataset(version=1, index=True)
class ProcessedFeatures:
    ...
```

**Files to modify:**
- `thyme/connectors.py`:
  - Add `KafkaSink`, `S3Sink`, `PostgresSink` classes (each extends `Connector` ABC from Task 1, or a new `SinkConnector` subclass).
  - Add `@sink` decorator that validates sink-specific constraints and stores metadata in `_SINK_REGISTRY`.
  - Add `get_registered_sinks()`, `clear_sink_registry()`.
  - Validation rules:
    - `KafkaSink`: requires `cdc="debezium"`, format must be `"json"`.
    - `S3Sink`: format must be `"delta"` or `"parquet"`, no CDC.
    - `PostgresSink`: no CDC, must specify table.
- `thyme/compiler.py` — Add `compile_sink()` function. Add sinks to `compile_commit_request()`.
- `thyme/dataset.py` — Update `clear_registry()` to also clear sink registry. Update `get_commit_payload()` to include sinks.
- `thyme/commit_payload.py` — Add sink entries to API payload.
- `proto/thyme/connector.proto` (cross-repo) — Add `Sink` message type.
- `tests/` — Test sink registration, validation rules, serialization.

**Acceptance criteria:**
- `@sink` stores metadata in registry, included in commit payload.
- Invalid sink configs (e.g. `KafkaSink` with `cdc="append"`) raise `ValueError` at decoration time.
- Sinks serialize correctly in both JSON and protobuf commit payloads.

---

## 5. Add Connector Validation at Decoration Time

**Problem:** The `@source` decorator performs minimal validation — it only checks CDC mode. It doesn't verify that the cursor field exists in the dataset schema, that `every`/`disorder` are valid duration strings, or that required connector fields are non-empty. Errors surface late (at commit time on the server, or at runtime).

**What Fennel does:** Validates at multiple stages: decoration time (syntax), serialization time (cross-entity references), and commit time (server-side schema checks). Duration parsing, preproc field existence, and CDC compatibility are all checked early.

**Task:** Add comprehensive validation to the `@source` decorator and connector constructors.

**Files to modify:**
- `thyme/connectors.py`:
  - In `source()` wrapper function (after the dataset class is available):
    - Validate `cursor` field exists in dataset fields (access `cls._dataset_meta["fields"]`). Note: `@source` runs before `@dataset` in decorator order (bottom-up), so the class may not have `_dataset_meta` yet. Solution: defer validation to a `_validate_source()` call triggered during `_discover_pipelines()` in `dataset.py`, or restructure decorator ordering.
    - Validate `every` and `disorder` are parseable duration strings. Add a `parse_duration(s: str) -> int` function that handles `"5m"`, `"1h"`, `"7d"`, `"30s"` and raises `ValueError` on bad input.
    - Validate required connector fields: `PostgresSource.host` not empty, `IcebergSource.catalog` not empty, etc.
  - Add `validate()` method to each connector class that checks required fields.
- `thyme/dataset.py`:
  - In `dataset()` decorator, after building schema, call `_validate_sources(cls)` which cross-references source metadata against schema fields.
- `thyme/pipeline.py`:
  - In `Pipeline.get_operators()`, validate that aggregation `of` fields exist in the input dataset schema.
- `tests/` — Test each validation: missing cursor field, bad duration string, empty host, aggregation on nonexistent field.

**Acceptance criteria:**
- `@source(pg.table("orders"), cursor="nonexistent_field")` raises `ValueError` with clear message after `@dataset` processes the class.
- `@source(..., every="banana")` raises `ValueError`.
- `PostgresSource(host="", ...)` raises `ValueError`.
- All existing valid definitions continue to work.

---

## 6. Add Environment Selector Support

**Problem:** There's no way to have different source configurations per environment (dev/staging/prod). Users who deploy to multiple environments must maintain separate feature definition files or use conditional logic.

**What Fennel does:** Has an `EnvSelector` that supports `env="prod"`, `env=["prod", "staging"]`, and negation `env="~dev"`. At serialization time, connectors are filtered by the active environment.

**Task:** Add environment-aware source selection.

**New API:**
```python
@source(pg_prod.table("orders"), cursor="updated_at", every="5m", env="prod")
@source(pg_dev.table("orders"), cursor="updated_at", every="1h", env="dev")
@dataset(version=1)
class Orders:
    ...
```

**Files to modify:**
- `thyme/connectors.py`:
  - Add `env: str | list[str] | None = None` parameter to `@source` decorator.
  - Store `env` in source metadata dict.
  - Allow multiple `@source` decorators on the same dataset (currently `_SOURCE_REGISTRY[cls.__name__]` overwrites). Change to `_SOURCE_REGISTRY[cls.__name__] = list[dict]`.
  - Add `_validate_env_selector(env)` — check no mixing of negated/non-negated, no empty strings.
- `thyme/compiler.py`:
  - `compile_source()` — Add `env` field to proto `Source` message (or pass through as metadata).
  - Handle list of sources per dataset.
- `thyme/dataset.py`:
  - `get_commit_payload()` — Filter sources by active environment (from `Config` or env var `THYME_ENV`).
- `thyme/config.py`:
  - Add `env: str = ""` field to `Config`. Load from `THYME_ENV` env var.
- `tests/` — Test multi-env source registration, filtering by env, negation syntax.

**Acceptance criteria:**
- Multiple `@source` decorators on one dataset don't overwrite each other.
- `THYME_ENV=prod thyme commit features.py` only includes prod sources.
- When no env filter is active, all sources are included.

---

## 7. Add Preprocessing/Transform Support on Sources

**Problem:** There's no way to rename columns, extract nested fields, or apply simple transforms during ingestion. Users must create intermediate datasets or handle this in pipelines, adding complexity.

**What Fennel does:** Has `preproc` parameter on `@source` with three value types: `ref("column_name")` for renames, `ref("nested[field]")` for nested extraction, and literal values for constants.

**Task:** Add a `preproc` parameter to `@source` for column-level transforms.

**New API:**
```python
@source(
    pg.table("raw_orders"),
    cursor="updated_at",
    preproc={
        "user_id": ref("uid"),           # rename uid -> user_id
        "country": "US",                  # constant value
        "amount": ref("data[amount]"),    # extract nested field
    }
)
@dataset(version=1)
class Orders:
    user_id: str = field(key=True)
    country: str
    amount: float
    timestamp: datetime = field(timestamp=True)
```

**Files to modify:**
- `thyme/connectors.py`:
  - Add `ref(field_name: str)` class/function that represents a field reference.
  - Add `preproc: dict[str, str | ref] | None = None` parameter to `@source`.
  - Store preproc in source metadata. Serialize `ref` objects as `{"__ref__": "field_name"}`.
  - Validate preproc keys exist in dataset fields (deferred validation, same pattern as Task 5).
- `thyme/compiler.py` — Serialize preproc into source proto. Add `PreProc` message handling.
- `proto/thyme/connector.proto` (cross-repo) — Add `PreProc` field to `Source` message.
- Engine `source.rs` (cross-repo) — Apply preproc transforms during ingestion (rename/extract/constant).
- `tests/` — Test ref serialization, constant values, nested field syntax, validation.

**Acceptance criteria:**
- `ref("uid")` serializes correctly in commit payload.
- Preproc keys validated against dataset field names.
- Literal values validated against field types.

---

## 8. Strengthen the Type System

**Problem:** The SDK's type system is limited to 5 base types (`int`, `float`, `str`, `bool`, `datetime`) plus `Optional`. There's no support for `List`, `Dict`, `Decimal`, or user-defined struct types. The `_type_to_string()` function falls back to `str()` for unknown types, losing semantic information.

**What Fennel does:** Supports `List[T]`, `Dict[K, V]`, `Optional[T]`, `Decimal`, `Embedding[N]`, `oneof(str, [...])`, `between(int, min, max)`, `regex(str, pattern)`, and user-defined structs (dataclasses).

**Task:** Extend the type system to support compound types.

**Files to modify:**
- `thyme/dataset.py`:
  - Update `_type_to_string()` to handle `list[T]`, `dict[K, V]` by recursively converting inner types.
  - Handle `List`, `Dict`, `Set` from `typing` module (check `get_origin()` for `list`, `dict`, `set`).
  - Return structured type strings: `"list[int]"`, `"dict[str, float]"`.
- `thyme/compiler.py`:
  - Update `TYPE_MAP` and `_type_str_to_proto()` to handle compound types.
  - Add `ListType`, `MapType` proto conversions.
  - If proto schema doesn't support these yet, serialize as JSON metadata.
- `proto/thyme/schema.proto` (cross-repo) — Extend `DataType` with `ListType`, `MapType` oneofs. *(Already exists in the proto — `repeated_type` and `map_type` are defined but unused by the SDK.)*
- `tests/` — Test `list[int]`, `dict[str, float]`, `Optional[list[str]]` type annotations on dataset fields.

**Acceptance criteria:**
- `list[int]` field annotation compiles to `DataType(repeated_type=RepeatedType(inner=DataType(int_type=...)))`.
- `dict[str, float]` compiles correctly.
- Nested optionals (`Optional[list[int]]`) work.
- Unknown types raise `ValueError` instead of silently falling back to string.

---

## 9. Fix Silent Exception Swallowing in CLI Commit

**Problem:** In `cli.py:commit()`, the protobuf compilation is wrapped in a bare `except Exception: pass` (line 131). If proto compilation fails for a real reason (schema mismatch, missing field, proto version incompatibility), it silently falls back to JSON. The user gets no indication that their carefully-defined protobuf schema was discarded.

**Current code:**
```python
try:
    from thyme.compiler import compile_commit_request
    proto_msg = compile_commit_request(...)
    proto_bytes = proto_msg.SerializeToString()
except Exception:
    pass  # Silent fallback to JSON
```

**Task:** Log the proto compilation failure and inform the user which format was used.

**Files to modify:**
- `thyme/cli.py`:
  - Replace bare `except Exception: pass` with `except Exception as e:` that logs via `typer.echo(f"Warning: protobuf compilation failed ({e}), falling back to JSON", err=True)`.
  - After successful POST, indicate which format was used: `typer.echo(f"Committed ... via {'protobuf' if proto_bytes else 'JSON'} to {url}")`.
- `tests/test_cli.py` — Add test that proto failure produces a warning on stderr.

**Acceptance criteria:**
- When proto compilation fails, user sees a warning on stderr.
- Commit still succeeds via JSON fallback.
- Commit success message indicates format used.

---

## 10. Add Duration Parsing and Validation Utility

**Problem:** Duration strings (`"5m"`, `"1h"`, `"7d"`) are passed as raw strings with no validation in the SDK. Invalid values like `"banana"` or `"5x"` pass through silently and only fail on the server. The `MockContext` in `thyme/mock.py` has its own `parse_window_duration()` but it's not used for source config validation.

**Task:** Extract duration parsing into a shared utility and validate at decoration time.

**Files to modify:**
- `thyme/duration.py` (new file):
  ```python
  import re

  _DURATION_RE = re.compile(r"^(\d+)(s|m|h|d)$")
  _MULTIPLIERS = {"s": 1, "m": 60, "h": 3600, "d": 86400}

  def parse_duration(s: str) -> int:
      """Parse a duration string to seconds. Raises ValueError on invalid input."""
      if not s:
          return 0
      m = _DURATION_RE.match(s.strip())
      if not m:
          raise ValueError(f"Invalid duration '{s}'. Expected format: '30s', '5m', '1h', '7d'.")
      return int(m.group(1)) * _MULTIPLIERS[m.group(2)]
  ```
- `thyme/connectors.py` — Import `parse_duration`, call it on `every` and `disorder` in `source()`.
- `thyme/mock.py` — Replace inline `parse_window_duration()` with import from `thyme/duration.py`.
- `tests/test_duration.py` (new file) — Test valid durations, invalid durations, empty string, edge cases.

**Acceptance criteria:**
- `parse_duration("5m")` returns 300. `parse_duration("banana")` raises `ValueError`.
- `@source(..., every="invalid")` raises `ValueError` at decoration time.
- `MockContext` uses the same parsing function (no divergence risk).

---

## 11. Add Multiple Key Fields Support to Datasets

**Problem:** Datasets are limited to exactly one key field. The validation in `_validate_dataset_fields()` raises `ValueError` if there are zero or more than one key. Many real-world use cases require composite keys (e.g., `(user_id, merchant_id)` for user-merchant interaction features).

**What Fennel does:** Supports multiple key fields per dataset. The key fields together form the composite entity key.

**Task:** Allow multiple key fields in dataset definitions.

**Files to modify:**
- `thyme/dataset.py`:
  - In `_validate_dataset_fields()`, change the "exactly one key field" constraint to "at least one key field".
  - Remove the error for `len(key_fields) > 1`.
  - Keep the "key field cannot be Optional" check for all key fields.
- `thyme/compiler.py` — No changes needed (`is_key` is already per-field, multiple keys just means multiple fields with `is_key=True`).
- Engine `operators.rs` / `state.rs` (cross-repo) — RocksDB key format needs to support composite keys: `entity_type/key1_val|key2_val/timestamp_bytes`. Verify engine supports this before making SDK change.
- `tests/test_dataset.py` — Add test for composite key dataset. Remove/update test that asserts error on multiple keys.

**Acceptance criteria:**
- `field(key=True)` on multiple fields is accepted.
- All key fields are serialized with `is_key=True` in proto.
- Still requires at least one key field and exactly one timestamp field.

---

## 12. Add `where` Filtering on Sources

**Problem:** There's no way to filter rows during ingestion. If you only want active users or orders from a specific region, you must ingest everything and filter in a pipeline. This wastes resources on the engine side.

**What Fennel does:** Has a `where` parameter on `@source` that pushes filters down to the ingestion query.

**Task:** Add a `where` parameter to `@source` for server-side row filtering.

**New API:**
```python
@source(
    pg.table("orders"),
    cursor="updated_at",
    every="5m",
    where=lambda df: df["status"] == "completed"
)
```

**Files to modify:**
- `thyme/connectors.py`:
  - Add `where: Callable | str | None = None` parameter to `@source`.
  - If `where` is a callable, capture its source code via `inspect.getsource()`.
  - If `where` is a string, treat as a SQL WHERE clause fragment.
  - Store in source metadata as `{"where": {"type": "python"|"sql", "source": "..."}}`.
- `thyme/compiler.py` — Serialize `where` into source proto (as `PyCode` for Python, or as string for SQL).
- Engine `source.rs` (cross-repo) — Apply where filter after polling source data.
- `tests/` — Test where with lambda, with SQL string, verify serialization.

**Acceptance criteria:**
- Where clause captured and included in commit payload.
- Both Python lambda and SQL string forms supported.
- None (no filter) is the default and works as before.

---

## 13. Add Sampling Support for Sources

**Problem:** During development and testing, users often want to work with a subset of data. There's no way to sample source data without modifying the source query or creating a separate test source.

**What Fennel does:** Has a `sample` parameter on `@source` that can be a float (fraction) or a `Sample` object with rate and column-based filtering.

**Task:** Add a `sample` parameter to `@source`.

**New API:**
```python
@source(pg.table("events"), cursor="ts", every="5m", sample=0.1)  # 10% sample
```

**Files to modify:**
- `thyme/connectors.py`:
  - Add `sample: float | None = None` parameter to `@source`.
  - Validate `0 < sample <= 1` if provided.
  - Store in source metadata.
- `thyme/compiler.py` — Add sample rate to source proto.
- `tests/` — Test sample validation, serialization, default None.

**Acceptance criteria:**
- `sample=0.1` serializes in commit payload.
- `sample=1.5` raises `ValueError`.
- Default (None) means no sampling.

---

## 14. Add Feature ID Uniqueness Validation

**Problem:** Feature IDs in a featureset are integers assigned manually (`feature(id=1)`, `feature(id=2)`). There's no validation that IDs are unique within a featureset. Duplicate IDs would cause silent data corruption in the query server.

**Task:** Add ID collision detection in the `@featureset` decorator.

**Files to modify:**
- `thyme/featureset.py`:
  - In `featureset()`, after collecting features, check for duplicate IDs:
    ```python
    ids = [f["id"] for f in features]
    dupes = [id for id in ids if ids.count(id) > 1]
    if dupes:
        raise ValueError(f"Duplicate feature IDs in {cls.__name__}: {set(dupes)}")
    ```
  - Also validate IDs are positive integers.
- `tests/test_featureset.py` — Test duplicate ID detection, zero/negative ID rejection.

**Acceptance criteria:**
- Two features with `feature(id=1)` in the same featureset raise `ValueError`.
- `feature(id=0)` and `feature(id=-1)` raise `ValueError`.

---

## 15. Add Extractor Input/Output Validation

**Problem:** `@extractor_inputs` and `@extractor_outputs` accept string feature names but there's no validation that these names correspond to actual features declared in the featureset. Typos in feature names silently propagate to the query server and fail at runtime.

**Task:** Validate extractor input/output names against declared features at registration time.

**Files to modify:**
- `thyme/featureset.py`:
  - In `featureset()`, after collecting both features and extractors, cross-validate:
    ```python
    feature_names = {f["name"] for f in features}
    for ext in extractors:
        for inp in ext["inputs"]:
            if inp not in feature_names:
                raise ValueError(f"Extractor '{ext['name']}' references unknown input feature '{inp}' in {cls.__name__}")
        for out in ext["outputs"]:
            if out not in feature_names:
                raise ValueError(f"Extractor '{ext['name']}' references unknown output feature '{out}' in {cls.__name__}")
    ```
- `tests/test_featureset.py` — Test with valid names, test with typo'd name, test with empty inputs/outputs.

**Acceptance criteria:**
- `@extractor_inputs("nonexistent_feature")` raises `ValueError` with clear message.
- Valid feature names pass through.
- Empty input/output lists are allowed (for extractors that use deps only).

---

## 16. Add Bounded Source / Backfill Support

**Problem:** There's no way to define a finite/bounded source for backfilling historical data. Sources are assumed to be infinite streams. Users who need to ingest a one-time batch (e.g., historical events from S3) have no SDK-level way to express this.

**What Fennel does:** Has `bounded=True` parameter on `@source` with an `idleness` timeout. When the source produces no records for `idleness` duration, it's marked complete.

**Task:** Add bounded source support.

**New API:**
```python
@source(
    s3.prefix("historical/"),
    cursor="event_time",
    every="5m",
    bounded=True,
    idleness="10m"  # mark complete after 10 min with no new records
)
@dataset(version=1)
class HistoricalEvents:
    ...
```

**Files to modify:**
- `thyme/connectors.py`:
  - Add `bounded: bool = False` and `idleness: str = ""` parameters to `@source`.
  - Validate: if `bounded=True`, `idleness` must be non-empty. If `idleness` is set, `bounded` must be True.
  - Store in source metadata.
- `thyme/compiler.py` — Add bounded/idleness fields to source proto.
- `tests/` — Test bounded source serialization, validation rules.

**Acceptance criteria:**
- `bounded=True, idleness="10m"` serializes correctly.
- `bounded=True` without `idleness` raises `ValueError`.
- `idleness="10m"` without `bounded=True` raises `ValueError`.

---

## 17. Add `since` / `until` Time Boundaries on Sources

**Problem:** There's no way to specify a time range for source ingestion. Users can't say "only ingest data from 2024-01-01 onwards" or "stop ingesting after 2024-12-31". This is important for backfills, migrations, and cost control.

**What Fennel does:** Has `since` and `until` parameters on `@source` as `datetime` objects, with validation that `since < until`.

**Task:** Add time boundary support.

**New API:**
```python
from datetime import datetime

@source(
    pg.table("orders"),
    cursor="created_at",
    every="5m",
    since=datetime(2024, 1, 1),
    until=datetime(2025, 1, 1),
)
```

**Files to modify:**
- `thyme/connectors.py`:
  - Add `since: datetime | None = None` and `until: datetime | None = None` parameters to `@source`.
  - Validate: if both are set, `since < until`.
  - Serialize as ISO 8601 strings in source metadata.
- `thyme/compiler.py` — Add `since`/`until` to source proto (as `google.protobuf.Timestamp` or ISO string).
- `tests/` — Test with both boundaries, one boundary, invalid range.

**Acceptance criteria:**
- `since` and `until` serialize as ISO 8601 in commit payload.
- `since > until` raises `ValueError`.
- Omitting either or both works (None means unbounded).

---

## 18. Improve Config YAML Parser Robustness

**Problem:** The fallback YAML parser (`_parse_simple_yaml`) only handles flat and one-level nested configs. Deeper nesting silently breaks. The parser is used when PyYAML is not installed, which can happen in minimal environments.

**Options:**
1. Make `pyyaml` a required dependency (simplest).
2. Improve the fallback parser to handle arbitrary nesting.

**Task:** Make `pyyaml` a required dependency and remove the fragile fallback parser.

**Files to modify:**
- `pyproject.toml` — Add `pyyaml>=6.0` to `dependencies`.
- `thyme/config.py`:
  - Remove `_parse_simple_yaml()` and `_coerce()` functions.
  - In `_load_yaml()`, remove the `try/except ImportError` fallback. Just use `yaml.safe_load()`.
  - If `import yaml` fails, raise a clear error: `ImportError("pyyaml is required: pip install pyyaml")`.
- `tests/test_config.py` — Remove test for fallback parser. Add test for deeply nested YAML config.

**Acceptance criteria:**
- `_load_yaml()` always uses PyYAML.
- Missing PyYAML gives a clear error message.
- No silent parse failures on nested YAML.

---

## 19. Add Kafka Source Connector

**Problem:** The SDK only supports three source types (Iceberg, Postgres, S3Json). Kafka is a critical data source for real-time streaming use cases but has no SDK connector. Users must work around this.

**What Fennel does:** Has a full `Kafka` data source with `bootstrap_servers`, `security_protocol` (PLAINTEXT/SASL_PLAINTEXT/SASL_SSL), SASL credentials, and topic-level configuration with format options (JSON, Avro, Protobuf).

**Task:** Add a `KafkaSource` connector.

**New API:**
```python
kafka = KafkaSource(
    brokers="kafka:9092",
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_username=env_secret("KAFKA_USER"),
    sasl_password=env_secret("KAFKA_PASS"),
)

@source(kafka.topic("raw-events", format="json"), cdc="append", disorder="5m")
@dataset(version=1)
class RawEvents:
    ...
```

**Files to modify:**
- `thyme/connectors.py`:
  - Add `KafkaSource(Connector)` class with fields: `brokers`, `security_protocol`, `sasl_mechanism`, `sasl_username`, `sasl_password`.
  - Add `KafkaSource.topic(topic: str, format: str = "json") -> KafkaTopicConnector`.
  - Add `KafkaTopicConnector(Connector)` with `to_dict()` returning `connector_type="kafka"`.
- `thyme/compiler.py` — Add `elif connector_type == "kafka":` branch in `compile_source()`.
- `proto/thyme/connector.proto` (cross-repo) — Add `KafkaSource` message.
- Engine `source.rs` (cross-repo) — Add Kafka consumer implementation for source ingestion loop.
- `thyme/agent/introspect.py` — Add `introspect_kafka()` for topic schema discovery.
- `thyme/cli.py` — Add `--kafka-*` options to `discover` command.
- `tests/` — Test KafkaSource construction, serialization, topic method.

**Acceptance criteria:**
- `KafkaSource` serializes correctly in commit payload.
- Topic-level format specification works.
- Security protocol and SASL credentials serialize correctly.

---

## 20. Performance: Lazy Registry Deep Copy

**Problem:** `get_registered_datasets()`, `get_registered_pipelines()`, `get_registered_sources()`, and `get_registered_featuresets()` all return `deepcopy()` of the entire registry. This is called multiple times during commit (in `get_commit_payload()`, `compile_commit_request()`, etc.), creating redundant copies of all metadata.

**Task:** Reduce unnecessary deep copies.

**Files to modify:**
- `thyme/dataset.py`:
  - Add `get_registered_datasets_ref() -> dict` that returns the registry directly (no copy) for internal use.
  - Keep `get_registered_datasets()` with deepcopy for public API.
  - Update `get_commit_payload()` to use the ref version internally.
- `thyme/connectors.py` — Same pattern: add `get_registered_sources_ref()`.
- `thyme/featureset.py` — Same pattern: add `get_registered_featuresets_ref()`.
- `thyme/commit_payload.py` — Use ref versions.

**Acceptance criteria:**
- `get_commit_payload()` does at most one deep copy of each registry (at the boundary), not multiple.
- Public API (`get_registered_datasets()`) still returns copies (safe for external callers).
- All existing tests pass.

---

## Priority Order

| Priority | Task | Impact | Effort |
|----------|------|--------|--------|
| P0 | 9. Fix silent exception swallowing | Bug fix, immediate | 30 min |
| P0 | 14. Feature ID uniqueness validation | Data integrity | 30 min |
| P0 | 15. Extractor input/output validation | Data integrity | 1 hr |
| P1 | 1. Base Connector ABC | Architecture | 1 hr |
| P1 | 10. Duration parsing utility | Validation | 1 hr |
| P1 | 5. Connector validation at decoration time | DX, reliability | 2 hr |
| P1 | 18. YAML parser robustness | Reliability | 30 min |
| P2 | 2. Two-layer connector model | DX, architecture | 3 hr |
| P2 | 3. Secret/credential references | Security | 3 hr |
| P2 | 8. Strengthen type system | Correctness | 2 hr |
| P2 | 11. Multiple key fields | Feature gap | 1 hr |
| P2 | 17. Since/until time boundaries | Feature gap | 1 hr |
| P3 | 6. Environment selectors | Multi-env DX | 3 hr |
| P3 | 7. Preprocessing transforms | Feature gap | 3 hr |
| P3 | 4. Sink/egress support | Feature gap | 4 hr |
| P3 | 19. Kafka source connector | Feature gap | 4 hr |
| P3 | 12. Where filtering | Optimization | 2 hr |
| P4 | 16. Bounded source / backfill | Feature gap | 2 hr |
| P4 | 13. Sampling support | DX | 1 hr |
| P4 | 20. Lazy registry deep copy | Performance | 1 hr |
