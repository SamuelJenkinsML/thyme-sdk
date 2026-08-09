"""`client.query_offline` reading Iceberg directly (TH-312).

The API path resolves a spine with a serial per-row loop inside the serving
process. That is a correct point-in-time API and not a training path, so when a
catalog is configured the SDK reads the tables itself.

Existing callers with no Iceberg configuration keep the API path exactly as it
was — this is additive.
"""

import duckdb
import polars as pl
import pytest

from thyme.client import ThymeClient, _ICEBERG_ENV, _iceberg_configured
from thyme.offline_catalog import CatalogConfig
from thyme.offline_iceberg import derived_features, resolve_spine
from thyme.result import ThymeResult


STORED_ONLY = {
    "name": "UserFeatures",
    "features": [
        {"name": "order_count_24h", "dtype": "int"},
        {"name": "total_spend_24h", "dtype": "float"},
    ],
}

WITH_DERIVED = {
    "name": "UserFeatures",
    "features": [
        {"name": "order_count_24h", "dtype": "int"},
        {"name": "is_suspicious", "dtype": "bool", "deps": ["order_count_24h"]},
    ],
}


@pytest.fixture
def con():
    connection = duckdb.connect()
    connection.execute(
        "CREATE TABLE user_order_stats("
        "  entity_id VARCHAR, ts VARCHAR, event_time TIMESTAMP,"
        "  state_value BLOB, order_count_24h DOUBLE, total_spend_24h DOUBLE)"
    )
    connection.executemany(
        "INSERT INTO user_order_stats VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("c123", "2026-08-04T09:00:00Z", "2026-08-04 09:00:00", b"x", 3.0, 30.0),
            ("c123", "2026-08-04T10:00:00Z", "2026-08-04 10:00:00", b"x", 7.0, 70.0),
            ("c456", "2026-08-04T09:30:00Z", "2026-08-04 09:30:00", b"x", 5.0, 50.0),
        ],
    )
    yield connection
    connection.close()


class TestResolveSpine:
    def test_resolves_each_spine_row_as_of_its_own_timestamp(self, con):
        # given a spine with per-row timestamps, as a training set has
        spine = pl.DataFrame(
            {
                "entity_id": ["c123", "c123", "c456"],
                "timestamp": [
                    "2026-08-04T09:30:00Z",
                    "2026-08-04T11:00:00Z",
                    "2026-08-04T11:00:00Z",
                ],
            }
        ).to_arrow()

        # when resolved in one scan
        rel = resolve_spine(
            con,
            table="user_order_stats",
            feature_columns=["order_count_24h", "total_spend_24h"],
            spine=spine,
            entity_column="entity_id",
            timestamp_column="timestamp",
        )
        rows = rel.pl().sort(["entity_id", "timestamp"]).rows()

        # then each row sees only what was known at its own timestamp
        assert [r[-2] for r in rows] == [3.0, 7.0, 5.0]

    def test_returns_a_relation_that_is_not_yet_evaluated(self, con):
        spine = pl.DataFrame(
            {"entity_id": ["c123"], "timestamp": ["2026-08-04T11:00:00Z"]}
        ).to_arrow()

        rel = resolve_spine(
            con,
            table="user_order_stats",
            feature_columns=["order_count_24h"],
            spine=spine,
            entity_column="entity_id",
            timestamp_column="timestamp",
        )

        # a relation, not a frame -- wrapping it in ThymeResult keeps it lazy
        result = ThymeResult(rel, connection=con)
        assert result.is_lazy is True
        assert result.columns == ["entity_id", "timestamp", "order_count_24h"]

    def test_the_blob_column_is_never_read(self, con):
        # given a table carrying state_value beside the feature columns
        spine = pl.DataFrame(
            {"entity_id": ["c123"], "timestamp": ["2026-08-04T11:00:00Z"]}
        ).to_arrow()

        rel = resolve_spine(
            con,
            table="user_order_stats",
            feature_columns=["order_count_24h"],
            spine=spine,
            entity_column="entity_id",
            timestamp_column="timestamp",
        )

        # then it is absent from the result entirely
        assert "state_value" not in rel.columns


class TestRouting:
    """Which path a pull takes must be predictable, not probed."""

    def test_no_iceberg_env_means_the_api_path(self, monkeypatch):
        for var in _ICEBERG_ENV:
            monkeypatch.delenv(var, raising=False)

        assert _iceberg_configured() is False

    @pytest.mark.parametrize("var", _ICEBERG_ENV)
    def test_any_iceberg_env_routes_at_the_store(self, monkeypatch, var):
        for name in _ICEBERG_ENV:
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv(var, "something")

        assert _iceberg_configured() is True

    def test_a_pull_with_derived_features_computes_them(self, monkeypatch, con):
        # given a featureset with a stored feature and one derived from it
        class UserFeatures:
            _featureset_meta = {
                "name": "UserOrderStats",
                "features": [
                    {"name": "order_count_24h", "dtype": "float"},
                    {
                        "name": "is_busy",
                        "dtype": "bool",
                        "deps": ["order_count_24h"],
                    },
                ],
                "extractors": [
                    {
                        "name": "compute_busy",
                        "kind": "PY_FUNC",
                        "inputs": ["order_count_24h"],
                        "outputs": ["is_busy"],
                        "source_code": (
                            "def compute_busy(cls, ts, count):\n"
                            "    if count is None:\n"
                            "        return False\n"
                            "    return count > 5\n"
                        ),
                    }
                ],
            }

        # and a catalog whose connection is the local fixture
        monkeypatch.setattr("thyme.client.connect", lambda cfg: con)

        spine = pl.DataFrame(
            {
                "entity_id": ["c123", "c456"],
                "timestamp": ["2026-08-04T11:00:00Z", "2026-08-04T11:00:00Z"],
            }
        )

        result = ThymeClient().query_offline(
            UserFeatures,
            spine,
            entity_column="entity_id",
            timestamp_column="timestamp",
            # A bare DuckDB connection is `memory.main`; the config has to
            # name both parts or the qualified table will not resolve.
            catalog=CatalogConfig(alias="memory", database="main"),
        )

        # then the stored feature is read and the derived one computed
        frame = result.to_polars().sort("entity_id")
        assert frame["order_count_24h"].to_list() == [7.0, 5.0]
        assert frame["is_busy"].to_list() == [True, False]
        assert result.metadata["extractors"] == "row_wise"


class TestDerivedFeatures:
    """Derived features are not table columns, so the SQL path cannot produce
    them. Returning a frame silently missing those columns would be a
    correctness bug, so the gap has to be visible."""

    def test_stored_only_featureset_has_no_derived_features(self):
        assert derived_features(STORED_ONLY) == []

    def test_derived_features_are_identified(self):
        assert derived_features(WITH_DERIVED) == ["is_suspicious"]

    def test_request_features_are_not_counted_as_derived(self):
        meta = {
            "features": [
                {"name": "live", "dtype": "float", "request": True, "deps": ["x"]},
            ]
        }
        assert derived_features(meta) == []
