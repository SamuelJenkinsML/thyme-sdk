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
from thyme.offline_iceberg import derived_features, resolve_featureset, resolve_spine
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


def lookup(feature: str, dataset: str, field: str) -> dict:
    return {
        "kind": "LOOKUP",
        "outputs": [feature],
        "lookup_info": {"dataset_name": dataset, "field_name": field},
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
                "name": "UserFeatures",
                "features": [
                    {"name": "order_count_24h", "dtype": "float"},
                    {
                        "name": "is_busy",
                        "dtype": "bool",
                        "deps": ["order_count_24h"],
                    },
                ],
                "extractors": [
                    lookup("order_count_24h", "UserOrderStats", "order_count_24h"),
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


class TestResolveFeatureset:
    """A pull reads the tables the sink wrote, which are named after output
    datasets. A featureset's own name is not a table."""

    CATALOG = CatalogConfig(alias="memory", database="main")
    SPINE = pl.DataFrame(
        {
            "entity_id": ["c123", "c456"],
            "timestamp": ["2026-08-04T11:00:00Z", "2026-08-04T11:00:00Z"],
        }
    )

    def resolve(self, con, meta):
        return resolve_featureset(
            con,
            featureset_meta=meta,
            catalog=self.CATALOG,
            spine=self.SPINE.to_arrow(),
            entity_column="entity_id",
            timestamp_column="timestamp",
            max_lookback=None,
        )

    def test_a_featureset_named_apart_from_its_dataset_reads_the_dataset(self, con):
        # given a featureset whose name is no table, reading under its own name
        meta = {
            "name": "FraudSignals",
            "features": [{"name": "orders", "dtype": "float"}],
            "extractors": [lookup("orders", "UserOrderStats", "order_count_24h")],
        }

        # when it is resolved
        frame = self.resolve(con, meta).pl().sort("entity_id")

        # then the dataset's table answers, under the feature's name
        assert frame.columns == ["entity_id", "timestamp", "orders"]
        assert frame["orders"].to_list() == [7.0, 5.0]

    def test_two_datasets_are_joined_on_the_spine(self, con):
        # given a second dataset's table, and a spine row asked for twice
        con.execute(
            "CREATE TABLE card_risk(entity_id VARCHAR, ts VARCHAR, event_time TIMESTAMP, risk DOUBLE)"
        )
        con.execute(
            "INSERT INTO card_risk VALUES ('c123', '2026-08-04T08:00:00Z', '2026-08-04 08:00:00', 0.5)"
        )
        meta = {
            "name": "FraudSignals",
            "features": [
                {"name": "order_count_24h", "dtype": "float"},
                {"name": "risk", "dtype": "float"},
            ],
            "extractors": [
                lookup("order_count_24h", "UserOrderStats", "order_count_24h"),
                lookup("risk", "CardRisk", "risk"),
            ],
        }
        spine = pl.concat([self.SPINE, self.SPINE.head(1)])

        # when it is resolved
        frame = (
            resolve_featureset(
                con,
                featureset_meta=meta,
                catalog=self.CATALOG,
                spine=spine.to_arrow(),
                entity_column="entity_id",
                timestamp_column="timestamp",
                max_lookback=None,
            )
            .pl()
            .sort("entity_id")
        )

        # then each spine row keeps one row, with both datasets' values
        assert frame["entity_id"].to_list() == ["c123", "c123", "c456"]
        assert frame["order_count_24h"].to_list() == [7.0, 7.0, 5.0]
        assert frame["risk"].to_list() == [0.5, 0.5, None]

    def test_a_feature_the_store_has_no_column_for_is_left_out(self, con):
        # given a list feature, which the sink keeps in json_features
        meta = {
            "name": "FraudSignals",
            "features": [
                {"name": "order_count_24h", "dtype": "float"},
                {"name": "recent", "dtype": "list"},
            ],
            "extractors": [
                lookup("order_count_24h", "UserOrderStats", "order_count_24h"),
                lookup("recent", "UserOrderStats", "recent"),
            ],
        }

        # then it is not asked of the table
        assert "recent" not in self.resolve(con, meta).columns

    def test_a_missing_column_names_the_table_and_the_column(self, con):
        meta = {
            "name": "FraudSignals",
            "features": [{"name": "orders", "dtype": "float"}],
            "extractors": [lookup("orders", "UserOrderStats", "no_such_column")],
        }

        with pytest.raises(ValueError, match=r"user_order_stats.*no_such_column"):
            self.resolve(con, meta)

    def test_a_missing_table_names_the_dataset(self, con):
        meta = {
            "name": "FraudSignals",
            "features": [{"name": "orders", "dtype": "float"}],
            "extractors": [lookup("orders", "NeverWritten", "orders")],
        }

        with pytest.raises(ValueError, match=r"NeverWritten.*never_written"):
            self.resolve(con, meta)

    def test_shares_with_no_known_window_refuse_to_read_stored_totals(self, con):
        # given a table the sink wrote with shares, where a late row has no
        # total, and no pipeline in this process to say what the window is
        con.execute("ALTER TABLE user_order_stats ADD COLUMN order_count_24h__share_count DOUBLE")
        meta = {
            "name": "FraudSignals",
            "features": [{"name": "orders", "dtype": "float"}],
            "extractors": [lookup("orders", "UserOrderStats", "order_count_24h")],
        }

        # when it is resolved, then it says so rather than lose the late row
        with pytest.raises(ValueError, match=r"order_count_24h.*pipeline"):
            self.resolve(con, meta)

    def test_no_stored_feature_is_refused(self, con):
        meta = {"name": "FraudSignals", "features": [], "extractors": []}

        with pytest.raises(ValueError, match="no stored features"):
            self.resolve(con, meta)


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
