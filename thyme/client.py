"""ThymeClient — lifecycle SDK for querying and managing features.

Connects to the Thyme query-server to fetch features online,
extract training data offline, log data, and inspect state.
"""

from __future__ import annotations

from typing import Any

import httpx
import polars as pl

from thyme.config import Config
from thyme.result import ThymeResult
from thyme.types import schema_from_featureset


def _get_featureset_meta(featureset: type) -> dict:
    """Extract featureset metadata from a decorated class."""
    meta = getattr(featureset, "_featureset_meta", None)
    if meta is None:
        raise ValueError(
            f"'{featureset.__name__}' is not a registered featureset. "
            f"Did you decorate it with @featureset?"
        )
    return meta


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
        )
        response.raise_for_status()
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
