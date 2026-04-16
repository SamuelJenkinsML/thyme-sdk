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
        client_kwargs: dict[str, Any] = {
            "base_url": self._config.query_url,
            "headers": self._config.auth_headers(),
            "timeout": 30.0,
        }
        if _transport is not None:
            client_kwargs["transport"] = _transport
        self._http = httpx.Client(**client_kwargs)

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
