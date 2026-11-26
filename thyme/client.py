"""ThymeClient — lifecycle SDK for querying and managing features.

Connects to the Thyme query-server to fetch features online,
extract training data offline, log data, and inspect state.
"""

from __future__ import annotations

import io
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable, Union

import httpx
import polars as pl

from thyme.config import Config
from thyme.offline_catalog import CatalogConfig, connect
from thyme.offline_extractors import apply_extractors, runnable_extractors
from thyme.offline_iceberg import (
    DEFAULT_MAX_LOOKBACK,
    resolve_featureset,
)
from thyme.result import ThymeResult
from thyme.types import schema_from_featureset

#: Environment that indicates the offline store is reachable. Presence of any of
#: these, not their value, is what routes `query_offline` at Iceberg — the
#: defaults in `CatalogConfig` are always populated, so "is it set" has to be
#: asked of the environment rather than of the resolved config.
_ICEBERG_ENV = ("THYME_ICEBERG_CATALOG", "THYME_ICEBERG_URI", "THYME_ICEBERG_DATABASE")


def _iceberg_configured() -> bool:
    """Whether the environment points at an Iceberg catalog.

    Deliberately explicit rather than probing for a reachable catalog: a network
    call to decide which code path to take would make the choice depend on
    transient conditions, and a training pull silently falling back to the
    per-row API path is exactly the failure this work exists to remove.
    """
    return any(os.environ.get(name) for name in _ICEBERG_ENV)

_ARROW_IPC_CONTENT_TYPE = "application/vnd.apache.arrow.stream"
_ARROW_ACCEPT = f"{_ARROW_IPC_CONTENT_TYPE}, application/json;q=0.9"
_QUERY_RUN_HEADER = "X-Query-Run-Id"


def _run_id(response: httpx.Response) -> str | None:
    """Extract the query-run id from a response, if the server set one."""
    value = response.headers.get(_QUERY_RUN_HEADER)
    return value or None


def _normalize_to_polars(
    data: Union[pl.DataFrame, list[dict], Any],
) -> pl.DataFrame:
    """Normalize input data to a Polars DataFrame.

    Accepts pl.DataFrame, pd.DataFrame, or list[dict].
    """
    if isinstance(data, pl.DataFrame):
        return data
    if isinstance(data, list):
        return pl.DataFrame(data)
    # Try pandas DataFrame
    if hasattr(data, "to_dict"):
        return pl.DataFrame(data.to_dict(orient="list"))
    raise TypeError(
        f"Expected pl.DataFrame, pd.DataFrame, or list[dict], got {type(data).__name__}"
    )


def _get_featureset_meta(featureset: type) -> dict:
    """Extract featureset metadata from a decorated class."""
    meta = getattr(featureset, "_featureset_meta", None)
    if meta is None:
        raise ValueError(
            f"'{featureset.__name__}' is not a registered featureset. "
            f"Did you decorate it with @featureset?"
        )
    return meta


def _is_arrow_ipc_response(response: httpx.Response) -> bool:
    """Check if the response contains Arrow IPC data."""
    content_type = response.headers.get("content-type", "")
    return _ARROW_IPC_CONTENT_TYPE in content_type


def _read_arrow_ipc(response: httpx.Response) -> pl.DataFrame:
    """Read Arrow IPC stream bytes from a response into a Polars DataFrame."""
    return pl.read_ipc_stream(io.BytesIO(response.content))


def _features_to_row(features: dict[str, Any], schema: pl.Schema) -> dict[str, Any]:
    """Cast feature values to match the expected Polars schema types."""
    row: dict[str, Any] = {}
    for name, dtype in schema.items():
        val = features.get(name)
        if val is None:
            row[name] = None
        elif dtype == pl.Int64:
            row[name] = int(val)
        elif dtype == pl.Float64:
            row[name] = float(val)
        elif dtype == pl.Boolean:
            row[name] = bool(val)
        else:
            row[name] = val
    return row


def _positional_to_polars(data: dict[str, Any]) -> pl.DataFrame:
    """Build a DataFrame from the positional batch shape.

    The server's default (and `?format=positional`) response is a 2D matrix:
    ``{"features": [col_names], "values": [[row], ...]}`` — one wide row per
    entity, ``entity_id`` leading. This is the JSON fallback for when the server
    did not return Arrow IPC.
    """
    cols: list[str] = data.get("features", [])
    values: list[list[Any]] = data.get("values", [])
    if not values:
        return pl.DataFrame({c: [] for c in cols}) if cols else pl.DataFrame()
    return pl.DataFrame(values, schema=cols, orient="row")


def _apply_featureset_schema(df: pl.DataFrame, schema: pl.Schema) -> pl.DataFrame:
    """Cast a wide batch frame's feature columns to the declared featureset types.

    The batch response is a wide table (``entity_id`` + one column per feature).
    Only columns named in the featureset schema are cast; non-feature columns
    such as ``entity_id`` pass through untouched.
    """
    casts = [pl.col(name).cast(dtype) for name, dtype in schema.items() if name in df.columns]
    return df.with_columns(casts) if casts else df


class ThymeClient:
    """Client for querying and managing Thyme features.

    Args:
        config: Connection configuration. If None, loads from
                environment / config files via Config.load().
        _transport: For testing — inject an httpx transport to
                    mock HTTP responses.
    """

    def __init__(
        self,
        config: Config | None = None,
        _transport: httpx.BaseTransport | None = None,
    ):
        self._config = config or Config.load()
        shared: dict[str, Any] = {
            "headers": self._config.auth_headers(),
            "timeout": 30.0,
        }
        if _transport is not None:
            shared["transport"] = _transport
        self._http = httpx.Client(base_url=self._config.query_url, **shared)
        self._def_http = httpx.Client(base_url=self._config.api_base, **shared)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the underlying HTTP connections."""
        self._http.close()
        self._def_http.close()

    def __enter__(self) -> "ThymeClient":
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()

    def query(
        self,
        featureset: type,
        entity_id: str,
        inputs: dict | None = None,
        *,
        timestamp: datetime | str | None = None,
        idempotency_key: str | None = None,
    ) -> ThymeResult:
        """Online feature query for a single entity.

        Args:
            featureset: A @featureset-decorated class.
            entity_id: The entity to query features for.
            inputs: Values for the featureset's ``request=True`` features,
                keyed by feature name. Sent over ``POST /features``.
            timestamp: Optional RFC-3339 string or datetime for a historical read.
            idempotency_key: Optional ``Idempotency-Key`` header so retries of a
                deciding featureset return the original decision without
                calling the model a second time.

        Returns:
            ThymeResult with a single row of feature values.
        """
        meta = _get_featureset_meta(featureset)
        fs_name = meta["name"]
        schema = schema_from_featureset(meta)
        ts_str = (
            timestamp
            if isinstance(timestamp, str) or timestamp is None
            else timestamp.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        )
        headers = {"Idempotency-Key": idempotency_key} if idempotency_key else None

        if inputs is not None:
            payload: dict[str, Any] = {
                "featureset": fs_name,
                "entity_id": entity_id,
                "inputs": inputs,
            }
            if ts_str is not None:
                payload["timestamp"] = ts_str
            response = self._http.post("/features", json=payload, headers=headers)
        else:
            params: dict[str, Any] = {"featureset": fs_name, "entity_id": entity_id}
            if ts_str is not None:
                params["timestamp"] = ts_str
            response = self._http.get("/features", params=params, headers=headers)
        response.raise_for_status()
        data = response.json()

        raw_features = data.get("features")
        if raw_features is None:
            df = pl.DataFrame(schema=schema)
        else:
            row = _features_to_row(raw_features, schema)
            df = pl.DataFrame([row], schema=schema)

        metadata: dict[str, Any] = {
            "entity_id": data.get("entity_id", entity_id),
            "entity_type": data.get("entity_type", fs_name),
            "mode": data.get("mode", "online"),
        }
        for key in ("decision_id", "recorded", "judgments", "policies"):
            if key in data:
                metadata[key] = data[key]

        return ThymeResult(
            df,
            metadata=metadata,
            query_run_id=_run_id(response),
        )

    def query_batch(
        self,
        featureset: type,
        entity_ids: list[str],
    ) -> ThymeResult:
        """Batch online query for multiple entities.

        Args:
            featureset: A @featureset-decorated class.
            entity_ids: List of entity IDs to query.

        Returns:
            ThymeResult with one row per entity.

        Raises:
            httpx.HTTPStatusError: On non-2xx response from query-server.
        """
        meta = _get_featureset_meta(featureset)
        fs_name = meta["name"]
        schema = schema_from_featureset(meta)

        response = self._http.post(
            "/features/batch",
            json={"featureset": fs_name, "entity_ids": entity_ids},
            headers={"accept": _ARROW_ACCEPT},
        )
        response.raise_for_status()

        # Both transports return a wide table: an `entity_id` column plus one
        # column per feature (bare names for a single featureset). Arrow IPC is
        # read zero-copy; the positional-JSON fallback is reshaped to match.
        if _is_arrow_ipc_response(response):
            df = _read_arrow_ipc(response)
        else:
            df = _positional_to_polars(response.json())
        df = _apply_featureset_schema(df, schema)

        return ThymeResult(
            df,
            metadata={"entity_type": fs_name, "mode": "batch"},
            query_run_id=_run_id(response),
        )

    def query_offline(
        self,
        featureset: type,
        entities: pl.DataFrame | list[dict] | Any,
        *,
        entity_column: str,
        timestamp_column: str,
        batch_size: int = 5000,
        catalog: CatalogConfig | None = None,
        max_lookback: str | None = DEFAULT_MAX_LOOKBACK,
        vectorized: bool = False,
    ) -> ThymeResult:
        """Point-in-time correct batch feature extraction for training data.

        Reads the Iceberg offline store **directly** when a catalog is
        configured — either passed here or via `THYME_ICEBERG_*` in the
        environment. The whole spine is then resolved by one as-of join rather
        than by `POST /features/offline`, which loops per row inside the serving
        process and is a point-in-time API rather than a training path.

        With no catalog configured the API path is used, unchanged.

        Args:
            featureset: A @featureset-decorated class.
            entities: Input data with entity keys and timestamps.
                      Accepts pl.DataFrame, pd.DataFrame, or list[dict].
            entity_column: Column name for entity IDs.
            timestamp_column: Column name for timestamps.
            batch_size: Number of rows per request. API path only.
            catalog: Where the Iceberg tables live. Defaults to the environment.
            max_lookback: How far back history is read, as a DuckDB interval.
                ``None`` reads all history. Iceberg path only.

        Returns:
            ThymeResult with original entity/timestamp columns plus feature
            columns. The Iceberg path returns a **lazy** result, so a large pull
            can be streamed with ``.sink_parquet()`` or ``.iter_batches()``
            instead of being materialised.

        Raises:
            httpx.HTTPStatusError: On non-2xx response from query-server.
            ValueError: On the Iceberg path, when a table or column is missing,
                or when no pipeline in this process declares a table's windows.
        """
        if catalog is not None or _iceberg_configured():
            return self._query_offline_iceberg(
                featureset,
                entities,
                entity_column=entity_column,
                timestamp_column=timestamp_column,
                catalog=catalog or CatalogConfig.from_env(),
                max_lookback=max_lookback,
                vectorized=vectorized,
            )

        meta = _get_featureset_meta(featureset)
        fs_name = meta["name"]
        schema = schema_from_featureset(meta)

        entities_df = _normalize_to_polars(entities)

        all_rows: list[dict[str, Any]] = []
        all_dfs: list[pl.DataFrame] = []
        last_run_id: str | None = None

        # Process in batches
        for offset in range(0, len(entities_df), batch_size):
            batch = entities_df.slice(offset, batch_size)
            queries = [
                {
                    "entity_id": str(row[entity_column]),
                    "timestamp": str(row[timestamp_column]),
                }
                for row in batch.to_dicts()
            ]

            response = self._http.post(
                "/features/offline",
                json={"featureset": fs_name, "queries": queries},
                headers={"accept": _ARROW_ACCEPT},
            )
            response.raise_for_status()
            last_run_id = _run_id(response) or last_run_id

            if _is_arrow_ipc_response(response):
                batch_df = _read_arrow_ipc(response)
                all_dfs.append(batch_df)
            else:
                data = response.json()
                for i, result_item in enumerate(data.get("results", [])):
                    entity_row = batch.row(i, named=True)
                    row = {
                        entity_column: entity_row[entity_column],
                        timestamp_column: entity_row[timestamp_column],
                    }
                    row.update(_features_to_row(result_item["features"], schema))
                    all_rows.append(row)

        # Merge Arrow IPC DataFrames if any
        if all_dfs:
            df = pl.concat(all_dfs)
        elif not all_rows:
            # Build empty DataFrame with entity columns + feature columns
            combined_schema = {
                entity_column: entities_df.schema.get(entity_column, pl.Utf8),
                timestamp_column: entities_df.schema.get(timestamp_column, pl.Utf8),
            }
            combined_schema.update(schema)
            df = pl.DataFrame(schema=combined_schema)
        else:
            df = pl.DataFrame(all_rows)

        return ThymeResult(
            df,
            metadata={"entity_type": fs_name, "mode": "offline"},
            query_run_id=last_run_id,
        )

    def _query_offline_iceberg(
        self,
        featureset: type,
        entities: Any,
        *,
        entity_column: str,
        timestamp_column: str,
        catalog: CatalogConfig,
        max_lookback: str | None,
        vectorized: bool = False,
    ) -> ThymeResult:
        """Resolve a spine against the Iceberg store in one as-of join."""
        meta = _get_featureset_meta(featureset)
        fs_name = meta["name"]

        spine = _normalize_to_polars(entities).select(
            [entity_column, timestamp_column]
        )

        con = connect(catalog)
        relation = resolve_featureset(
            con,
            featureset_meta=meta,
            catalog=catalog,
            spine=spine.to_arrow(),
            entity_column=entity_column,
            timestamp_column=timestamp_column,
            max_lookback=max_lookback,
        )
        metadata = {
            "entity_type": fs_name,
            "mode": "offline",
            "source": "iceberg",
            "catalog": catalog.type,
        }

        if not runnable_extractors(meta):
            # `connection` is load-bearing: a DuckDB relation is only valid
            # while its connection lives, and this one is local to the call.
            return ThymeResult(relation, metadata=metadata, connection=con)

        # Derived features need Python, and Python needs rows — so this is the
        # one path that materialises. Row-wise by default because that is what
        # query-server does and therefore the only way to guarantee the training
        # set matches the serving path; `vectorized=True` trades that guarantee
        # for one call per frame. See thyme/offline_extractors.py.
        frame = apply_extractors(relation.pl(), meta, vectorized=vectorized)
        metadata["extractors"] = "vectorized" if vectorized else "row_wise"
        return ThymeResult(frame, metadata=metadata)

    def lookup(
        self,
        dataset: type,
        entity_id: str,
        *,
        timestamp: str | None = None,
    ) -> ThymeResult:
        """Direct dataset point lookup (no extractors).

        Uses the query-server's raw path to read directly from RocksDB.

        Args:
            dataset: A @dataset-decorated class.
            entity_id: The entity to look up.
            timestamp: Optional timestamp for point-in-time lookup.

        Returns:
            ThymeResult with a single row, or empty if entity not found.
        """
        ds_meta = getattr(dataset, "_dataset_meta", None)
        if ds_meta is None:
            raise ValueError(
                f"'{dataset.__name__}' is not a registered dataset. "
                f"Did you decorate it with @dataset?"
            )
        ds_name = ds_meta["name"]

        params: dict[str, str] = {
            "entity_type": ds_name,
            "entity_id": entity_id,
        }
        if timestamp is not None:
            params["timestamp"] = timestamp

        response = self._http.get("/features", params=params)
        response.raise_for_status()
        data = response.json()

        features = data.get("features")
        mode = data.get("mode", "online")

        if features is None:
            df = pl.DataFrame()
        else:
            df = pl.DataFrame([features])

        return ThymeResult(
            df,
            metadata={
                "entity_id": entity_id,
                "entity_type": ds_name,
                "mode": mode,
            },
            query_run_id=_run_id(response),
        )

    def inspect(
        self,
        featureset: type | None = None,
    ) -> dict:
        """Inspect system metadata from the definition service.

        Args:
            featureset: If provided, return metadata for this specific featureset.
                        If None, return the full system status.

        Returns:
            Dict with system metadata (datasets, pipelines, featuresets, sources, jobs)
            or a single featureset's metadata if featureset is specified.

        Raises:
            ValueError: If the specified featureset is not found.
        """
        # inspect() calls the definition-service, not the query-server
        response = self._def_http.get("/api/v1/status")
        response.raise_for_status()
        data = response.json()

        if featureset is None:
            return data

        meta = _get_featureset_meta(featureset)
        fs_name = meta["name"]

        for fs in data.get("featuresets", []):
            if fs.get("name") == fs_name:
                return fs

        raise ValueError(
            f"Featureset '{fs_name}' not found in system status. "
            f"Has it been committed?"
        )

    def log(
        self,
        dataset: type,
        data: pl.DataFrame | list[dict] | Any,
    ) -> None:
        """Push data into a dataset via the definition-service.

        Args:
            dataset: A @dataset-decorated class.
            data: Events to log. Accepts pl.DataFrame, pd.DataFrame, or list[dict].

        Raises:
            httpx.HTTPStatusError: On non-2xx response.
        """
        ds_meta = getattr(dataset, "_dataset_meta", None)
        if ds_meta is None:
            raise ValueError(
                f"'{dataset.__name__}' is not a registered dataset. "
                f"Did you decorate it with @dataset?"
            )
        ds_name = ds_meta["name"]

        df = _normalize_to_polars(data)
        events = df.to_dicts()

        response = self._def_http.post(
            "/api/v1/log",
            json={"dataset": ds_name, "events": events},
        )
        response.raise_for_status()

    # ------------------------------------------------------------------
    # Backfill (TH-156)
    # ------------------------------------------------------------------

    def backfill(
        self,
        job_name: str,
        *,
        target_start: "datetime | str | None" = None,
        reset: bool = False,
    ) -> dict[str, Any]:
        """Ask the control plane to backfill a job, and return the new row.

        A backfill replays the job's input topic in event-time order and
        re-publishes nothing. Committing a new pipeline already asks for its own
        backfill. This is for running one again, or from a chosen date. History
        the topic no longer holds comes from bumping the source dataset's
        version, which re-reads the source into new storage.

        Args:
            job_name: The job to backfill, e.g. ``"count_orders_job"``.
            target_start: First event time whose rows the replay emits. Earlier
                events still build state. ``None`` emits every row the topic
                holds.
            reset: Remove each partition's state first. Needed to run a backfill
                again for a job that already has state, because a runner
                replays only onto an empty store. This discards the job's
                current state and rebuilds it from the topic.

        Returns:
            The service's response: ``backfill_id``, ``job_name``, ``reset``.

        Raises:
            httpx.HTTPStatusError: 404 for an unknown job, 409 when a backfill
                for it is already pending or running, 400 for a bad combination.
        """
        body: dict[str, Any] = {"job_name": job_name, "reset": reset}
        if target_start is not None:
            body["target_start"] = (
                target_start
                if isinstance(target_start, str)
                else target_start.isoformat().replace("+00:00", "Z")
            )
        response = self._def_http.post("/api/v1/backfills", json=body)
        response.raise_for_status()
        return response.json()

    def list_backfills(self, job_name: str | None = None) -> list[dict[str, Any]]:
        """Every backfill the control plane knows about, newest first.

        Args:
            job_name: Return only this job's backfills.
        """
        response = self._def_http.get("/api/v1/backfills")
        response.raise_for_status()
        rows = response.json()
        if job_name is not None:
            rows = [r for r in rows if r.get("job_name") == job_name]
        return rows

    def wait_for_backfill(
        self,
        backfill_id: str,
        *,
        timeout: float = 1800.0,
        poll_interval: float = 2.0,
        on_progress: "Callable[[dict[str, Any]], None] | None" = None,
    ) -> dict[str, Any]:
        """Poll until a backfill completes, fails, or the timeout runs out.

        A replay is complete when every partition of the job has handed over to
        live processing, which is what the service reports in
        ``completed_partitions``.

        Args:
            backfill_id: The id ``backfill`` returned.
            timeout: Seconds to wait before giving up.
            poll_interval: Seconds between polls.
            on_progress: Called with each row read, for progress output.

        Returns:
            The final row.

        Raises:
            RuntimeError: The backfill failed, or the id does not exist.
            TimeoutError: It had not finished within ``timeout``.
        """
        deadline = time.monotonic() + timeout
        row: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            rows = self.list_backfills()
            row = next((r for r in rows if r.get("id") == backfill_id), None)
            if row is None:
                raise RuntimeError(f"No backfill {backfill_id}")
            if on_progress is not None:
                on_progress(row)
            status = row.get("status")
            if status == "completed":
                return row
            if status == "failed":
                raise RuntimeError(
                    f"Backfill {backfill_id} failed: {row.get('error_message') or 'no reason given'}"
                )
            time.sleep(poll_interval)
        raise TimeoutError(
            f"Backfill {backfill_id} was still {row.get('status') if row else 'unknown'} "
            f"after {timeout:.0f}s"
        )

    def record_decision(
        self,
        *,
        featureset: str | type,
        entity_id: str,
        state: dict[str, Any] | str,
        state_hash: str,
        renderer_version: int,
        feature_versions: dict[str, Any] | str,
        questions_hash: str,
        model_alias: str,
        model_version: str,
        judgments: dict[str, Any] | str,
        policy_version: int,
        rule_fired: str,
        action: str,
        latency_ms: int,
        replica_id: str = "external",
        decided_at: datetime | str | None = None,
        decision_id: str | None = None,
        cost_usd: float | None = None,
        idempotency_key: str | None = None,
    ) -> str:
        """Record a decision made by an external model (XGBoost, LLM agent, etc.).

        Sends ``POST /api/v1/decisions`` to the definition service and returns
        the durable ``decision_id``. Dict arguments for ``state``,
        ``feature_versions`` and ``judgments`` are serialized to JSON strings.
        """
        fs_name = featureset.__name__ if isinstance(featureset, type) else str(featureset)
        ts = _iso_utc(decided_at) if decided_at is not None else _iso_utc(datetime.now(timezone.utc))
        payload: dict[str, Any] = {
            "featureset": fs_name,
            "entity_id": entity_id,
            "decided_at": ts,
            "replica_id": replica_id,
            "state": state if isinstance(state, str) else json.dumps(state, sort_keys=True),
            "state_hash": state_hash,
            "renderer_version": int(renderer_version),
            "feature_versions": (
                feature_versions
                if isinstance(feature_versions, str)
                else json.dumps(feature_versions, sort_keys=True)
            ),
            "questions_hash": questions_hash,
            "model_alias": model_alias,
            "model_version": model_version,
            "judgments": (
                judgments if isinstance(judgments, str) else json.dumps(judgments, sort_keys=True)
            ),
            "policy_version": int(policy_version),
            "rule_fired": rule_fired,
            "action": action,
            "latency_ms": int(latency_ms),
        }
        if decision_id is not None:
            payload["decision_id"] = decision_id
        if cost_usd is not None:
            payload["cost_usd"] = float(cost_usd)
        if idempotency_key is not None:
            payload["idempotency_key"] = idempotency_key

        response = self._def_http.post("/api/v1/decisions", json=payload)
        response.raise_for_status()
        return response.json()["decision_id"]

    def record_outcome(
        self,
        outcome: str,
        *,
        featureset: str | type,
        entity_id: str,
        decision_id: str | None = None,
        value: float | None = None,
        occurred_at: datetime | str | None = None,
        outcome_id: str | None = None,
    ) -> str:
        """Record an outcome (e.g. chargeback, conversion) for a decision or entity.

        Sends ``POST /api/v1/outcomes`` to the definition service and returns
        the durable ``outcome_id``. Omit ``decision_id`` when the outcome
        attaches by ``(featureset, entity_id)`` inside a declared window.
        """
        fs_name = featureset.__name__ if isinstance(featureset, type) else str(featureset)
        ts = _iso_utc(occurred_at) if occurred_at is not None else _iso_utc(datetime.now(timezone.utc))
        payload: dict[str, Any] = {
            "featureset": fs_name,
            "entity_id": entity_id,
            "outcome": outcome,
            "occurred_at": ts,
        }
        if decision_id is not None:
            payload["decision_id"] = decision_id
        if value is not None:
            payload["value"] = float(value)
        if outcome_id is not None:
            payload["outcome_id"] = outcome_id

        response = self._def_http.post("/api/v1/outcomes", json=payload)
        response.raise_for_status()
        return response.json()["outcome_id"]

    def get_decision(self, decision_id: str) -> dict[str, Any]:
        """Fetch one recorded decision by ID from the query server."""
        response = self._http.get(f"/decisions/{decision_id}")
        response.raise_for_status()
        return response.json()

    def list_decisions(
        self,
        *,
        featureset: str | type | None = None,
        entity_id: str | None = None,
        start: datetime | str | None = None,
        end: datetime | str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """List recorded decisions from the query server (tail + Iceberg, deduplicated)."""
        params: dict[str, Any] = {"limit": limit}
        if featureset is not None:
            params["featureset"] = (
                featureset.__name__ if isinstance(featureset, type) else str(featureset)
            )
        if entity_id is not None:
            params["entity_id"] = entity_id
        if start is not None:
            params["from"] = _iso_utc(start)
        if end is not None:
            params["to"] = _iso_utc(end)
        response = self._http.get("/decisions", params=params)
        response.raise_for_status()
        return response.json().get("decisions", [])

    def backtest_policy(
        self,
        featureset: str | type,
        *,
        policy: Any = None,
        records: Any = None,
        start: datetime | str | None = None,
        end: datetime | str | None = None,
        entity_ids: list[str] | None = None,
        catalog: CatalogConfig | None = None,
        judgment_columns: dict[str, str] | None = None,
        policy_output: str = "action",
        chunk_size: int = 100,
    ) -> Any:
        """Replay a policy over stored decision records in Rust with zero model calls."""
        from thyme.backtest import backtest_policy as _backtest_policy

        return _backtest_policy(
            self,
            featureset,
            policy=policy,
            records=records,
            start=start,
            end=end,
            entity_ids=entity_ids,
            catalog=catalog,
            judgment_columns=judgment_columns,
            policy_output=policy_output,
            chunk_size=chunk_size,
        )

    def erase(
        self,
        dataset_or_entity: type | str | list[str],
        entity_ids: list[str] | None = None,
        *,
        include_decisions: bool = False,
    ) -> None:
        """Erase entities for GDPR deletion.

        Two forms:
        - ``client.erase(DatasetClass, ["card_4417"])`` — erases from that
          dataset (and optionally decision records/outcomes via
          ``include_decisions=True``).
        - ``client.erase("card_4417")`` or ``client.erase(["card_4417"])`` —
          entity-wide erase across datasets, decision records, and outcomes.
        """
        payload: dict[str, Any]
        if isinstance(dataset_or_entity, str):
            payload = {"entity_ids": [dataset_or_entity]}
        elif isinstance(dataset_or_entity, list):
            payload = {"entity_ids": list(dataset_or_entity)}
        else:
            ds_meta = getattr(dataset_or_entity, "_dataset_meta", None)
            if ds_meta is None:
                raise ValueError(
                    f"'{dataset_or_entity.__name__}' is not a registered dataset. "
                    f"Did you decorate it with @dataset?"
                )
            if entity_ids is None:
                raise ValueError("entity_ids is required when erasing a specific dataset")
            payload = {"dataset": ds_meta["name"], "entity_ids": list(entity_ids)}
            if include_decisions:
                payload["include_decisions"] = True

        response = self._def_http.post("/api/v1/erase", json=payload)
        response.raise_for_status()


def _iso_utc(value: datetime | str) -> str:
    if isinstance(value, str):
        return value
    dt = value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")
