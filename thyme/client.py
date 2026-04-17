"""ThymeClient — lifecycle SDK for querying and managing features.

Connects to the Thyme query-server to fetch features online,
extract training data offline, log data, and inspect state.
"""

from __future__ import annotations

import io
from typing import Any, Union

import httpx
import polars as pl

from thyme.config import Config
from thyme.result import ThymeResult
from thyme.types import schema_from_featureset

_ARROW_IPC_CONTENT_TYPE = "application/vnd.apache.arrow.stream"
_ARROW_ACCEPT = f"{_ARROW_IPC_CONTENT_TYPE}, application/json;q=0.9"


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

    def query(
        self,
        featureset: type,
        entity_id: str,
    ) -> ThymeResult:
        """Online feature query for a single entity.

        Args:
            featureset: A @featureset-decorated class.
            entity_id: The entity to query features for.

        Returns:
            ThymeResult with a single row of feature values.

        Raises:
            httpx.HTTPStatusError: On non-2xx response from query-server.
        """
        meta = _get_featureset_meta(featureset)
        fs_name = meta["name"]
        schema = schema_from_featureset(meta)

        response = self._http.get(
            "/features",
            params={"featureset": fs_name, "entity_id": entity_id},
        )
        response.raise_for_status()
        data = response.json()

        row = _features_to_row(data["features"], schema)
        df = pl.DataFrame([row], schema=schema)

        return ThymeResult(df, metadata={
            "entity_id": data.get("entity_id", entity_id),
            "entity_type": data.get("entity_type", fs_name),
            "mode": data.get("mode", "online"),
        })

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

        if _is_arrow_ipc_response(response):
            df = _read_arrow_ipc(response)
        else:
            data = response.json()
            results = data.get("results", [])
            rows = [_features_to_row(r["features"], schema) for r in results]
            if not rows:
                df = pl.DataFrame(schema=schema)
            else:
                df = pl.DataFrame(rows, schema=schema)

        return ThymeResult(df, metadata={
            "entity_type": fs_name,
            "mode": "batch",
        })

    def query_offline(
        self,
        featureset: type,
        entities: pl.DataFrame | list[dict] | Any,
        *,
        entity_column: str,
        timestamp_column: str,
        batch_size: int = 5000,
    ) -> ThymeResult:
        """Point-in-time correct batch feature extraction for training data.

        Args:
            featureset: A @featureset-decorated class.
            entities: Input data with entity keys and timestamps.
                      Accepts pl.DataFrame, pd.DataFrame, or list[dict].
            entity_column: Column name for entity IDs.
            timestamp_column: Column name for timestamps.
            batch_size: Number of rows per request (default 5000).

        Returns:
            ThymeResult with original entity/timestamp columns plus feature columns.

        Raises:
            httpx.HTTPStatusError: On non-2xx response from query-server.
        """
        meta = _get_featureset_meta(featureset)
        fs_name = meta["name"]
        schema = schema_from_featureset(meta)

        entities_df = _normalize_to_polars(entities)

        all_rows: list[dict[str, Any]] = []
        all_dfs: list[pl.DataFrame] = []

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

        return ThymeResult(df, metadata={
            "entity_type": fs_name,
            "mode": "offline",
        })

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

        return ThymeResult(df, metadata={
            "entity_id": entity_id,
            "entity_type": ds_name,
            "mode": mode,
        })

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

    def erase(
        self,
        dataset: type,
        entity_ids: list[str],
    ) -> None:
        """Erase entities from a dataset (GDPR deletion).

        Produces CDC delete events to the dataset's Kafka topic.
        The engine processes these as tombstones in RocksDB.

        Args:
            dataset: A @dataset-decorated class.
            entity_ids: Entity IDs to erase.

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

        response = self._def_http.post(
            "/api/v1/erase",
            json={"dataset": ds_name, "entity_ids": entity_ids},
        )
        response.raise_for_status()
