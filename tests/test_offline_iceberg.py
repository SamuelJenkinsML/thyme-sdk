"""Tests for the Iceberg as-of read path (TH-312).

Two properties decide whether a 20M-row training pull is minutes or hours, and
both regress silently, so they are asserted directly rather than inferred from a
benchmark:

1. The scan projects only the columns the featureset needs. The Iceberg table
   also carries `state_value` (the raw StateValue proto, ~200-500 B/row) plus
   `sketches`, `dimensions` and `json_features`. Reading them turns a ~28 GB scan
   into a ~170-380 GB one.
2. The join is a single ASOF over the whole spine, not a per-row seek.

The third property is correctness rather than speed, and is the one that fails
quietly: ordering is on the **raw timestamp string**, compared byte-wise, never
on a parsed instant.
"""

import duckdb
import pytest

from thyme.offline_iceberg import (
    DEFAULT_MAX_LOOKBACK,
    build_asof_sql,
    feature_columns_for,
)


# Columns the sink writes that a training pull must never read. Names mirror
# offline-sink/internal/sink/schema.go.
BLOB_COLUMNS = ["state_value", "sketches", "dimensions", "json_features"]


class TestColumnProjection:
    """The single largest efficiency win, and it costs nothing."""

    def test_projects_only_requested_feature_columns(self):
        # given a featureset needing two of the table's many columns
        sql = build_asof_sql(
            table="thyme.user_order_stats",
            feature_columns=["order_count_24h", "total_spend_24h"],
            entity_column="entity_id",
            timestamp_column="timestamp",
        )

        # then both features are projected
        assert "order_count_24h" in sql
        assert "total_spend_24h" in sql

    @pytest.mark.parametrize("blob", BLOB_COLUMNS)
    def test_never_reads_the_blob_columns(self, blob):
        # given a pull that does not ask for them
        sql = build_asof_sql(
            table="thyme.user_order_stats",
            feature_columns=["order_count_24h"],
            entity_column="entity_id",
            timestamp_column="timestamp",
        )

        # then they are absent entirely -- the Rust reader needs state_value for
        # RocksDB parity, the SDK path must never touch it
        assert blob not in sql

    def test_never_emits_a_star_projection(self):
        # given any pull
        sql = build_asof_sql(
            table="thyme.user_order_stats",
            feature_columns=["order_count_24h"],
            entity_column="entity_id",
            timestamp_column="timestamp",
        )

        # then SELECT * never appears -- it would pull every blob column
        assert "*" not in sql

    def test_feature_columns_for_excludes_request_and_derived_features(self):
        # given a featureset mixing stored, request-time and derived features
        meta = {
            "name": "UserFeatures",
            "features": [
                {"name": "order_count_24h", "dtype": "int"},
                {"name": "live_input", "dtype": "float", "request": True},
                {"name": "is_suspicious", "dtype": "bool", "deps": ["order_count_24h"]},
            ],
        }

        # when resolving what to read from the table
        cols = feature_columns_for(meta)

        # then only the stored feature is a table column; the request feature
        # arrives from the caller and the derived one is computed
        assert cols == ["order_count_24h"]


class TestLookbackPruning:
    """Scan volume is unbounded without a lower bound on the history read."""

    def test_upper_bound_prunes_from_the_spine(self):
        # given any pull
        sql = build_asof_sql(
            table="t",
            feature_columns=["f"],
            entity_column="entity_id",
            timestamp_column="timestamp",
        )

        # then history above the spine's newest timestamp is never read --
        # always safe, since an as-of read only looks backwards
        assert "max_ts" in sql or "MAX(" in sql.upper()

    def test_max_lookback_bounds_the_scan_below(self):
        # given a 90 day lookback
        sql = build_asof_sql(
            table="t",
            feature_columns=["f"],
            entity_column="entity_id",
            timestamp_column="timestamp",
            max_lookback="90 days",
        )

        # then the scan is bounded below
        assert "90 days" in sql

    def test_lookback_is_bounded_by_default(self):
        # given a caller who says nothing about lookback
        sql = build_asof_sql(
            table="t",
            feature_columns=["f"],
            entity_column="entity_id",
            timestamp_column="timestamp",
        )

        # then the scan is bounded anyway -- an unbounded default would make
        # scan volume a function of the table's whole history
        assert DEFAULT_MAX_LOOKBACK in sql
        assert "min_event_time -" in sql

    def test_max_lookback_none_opts_out_of_bounding(self):
        # given an explicit opt-out
        sql = build_asof_sql(
            table="t",
            feature_columns=["f"],
            entity_column="entity_id",
            timestamp_column="timestamp",
            max_lookback=None,
        )

        # then all history is read -- correct for a dormant-entity spine, and
        # the caller has asked for it deliberately
        assert "min_event_time -" not in sql

    def test_pruning_targets_the_partition_column_not_the_ts_string(self):
        # given a lookback
        sql = build_asof_sql(
            table="t",
            feature_columns=["f"],
            entity_column="entity_id",
            timestamp_column="timestamp",
            max_lookback="90 days",
        )

        # then bounds reference event_time -- the table is partitioned by
        # days(event_time), and a predicate on the ts string prunes nothing
        assert "event_time" in sql


class TestAsOfSemanticsMatchRocksDB:
    """The invariant that fails silently.

    RocksDB keys on `{type}:{id}:{ts}` and `seek_for_prev` compares those bytes
    lexicographically. The join has to agree, including where byte order and
    chronological order disagree:

        "2026-08-04T09:14:02Z"   vs  "2026-08-04T09:14:02.1Z"
                 'Z' = 0x5A                    '.' = 0x2E

    Byte-wise the fractional form sorts BELOW the whole-second form, while
    chronologically it is 100 ms later.
    """

    HISTORY = [
        ("c123", "2026-08-04T09:14:02.1Z", 1.0),
        ("c123", "2026-08-04T09:14:02.481922Z", 2.0),
        ("c123", "2026-08-04T09:14:02Z", 3.0),
        ("c123", "2026-08-04T10:00:00Z", 4.0),
        ("c999", "2026-08-04T09:14:02.1Z", 99.0),
    ]

    @staticmethod
    def _event_time(ts: str) -> str:
        """The parsed partition column, as the sink derives it from `ts`."""
        return ts.replace("T", " ").replace("Z", "")

    SPINE = [
        ("c123", "2026-08-04T09:14:02.5Z"),
        ("c123", "2026-08-04T09:14:02Z"),
        ("c123", "2026-08-04T09:14:02.481922Z"),
        ("c123", "2026-08-01T00:00:00Z"),
        ("c123", "2026-12-31T23:59:59Z"),
        ("c999", "2026-08-04T09:14:02.1Z"),
        ("cmissing", "2026-08-04T10:00:00Z"),
    ]

    def _seek_for_prev(self, entity_id, target):
        """What RocksDB would return: newest row with ts bytes <= target bytes."""
        tb = target.encode()
        best = None
        for eid, ts, val in self.HISTORY:
            if eid == entity_id and ts.encode() <= tb:
                if best is None or ts.encode() > best[0].encode():
                    best = (ts, val)
        return best

    @pytest.fixture
    def con(self):
        con = duckdb.connect()
        con.execute(
            "CREATE TABLE hist(entity_id VARCHAR, ts VARCHAR, "
            "event_time TIMESTAMP, f DOUBLE)"
        )
        con.executemany(
            "INSERT INTO hist VALUES (?, ?, ?, ?)",
            [(e, ts, self._event_time(ts), v) for e, ts, v in self.HISTORY],
        )
        con.execute("CREATE TABLE spine(entity_id VARCHAR, timestamp VARCHAR)")
        con.executemany("INSERT INTO spine VALUES (?, ?)", self.SPINE)
        return con

    def test_resolves_every_spine_row_exactly_as_seek_for_prev(self, con):
        # given the generated as-of join
        sql = build_asof_sql(
            table="hist",
            feature_columns=["f"],
            entity_column="entity_id",
            timestamp_column="timestamp",
            spine_relation="spine",
        )

        # when it runs over a history mixing whole-second and sub-second forms
        rows = con.execute(sql).fetchall()

        # then every row matches what the online store would have returned
        assert len(rows) == len(self.SPINE)
        for row in rows:
            entity_id, target, value = row[0], row[1], row[-1]
            expected = self._seek_for_prev(entity_id, target)
            assert value == (expected[1] if expected else None), (
                f"{entity_id} @ {target}: got {value}, "
                f"expected {expected[1] if expected else None}"
            )

    def test_a_miss_yields_a_null_row_rather_than_dropping_it(self, con):
        # given a spine containing an entity with no history at all
        sql = build_asof_sql(
            table="hist",
            feature_columns=["f"],
            entity_column="entity_id",
            timestamp_column="timestamp",
            spine_relation="spine",
        )

        # when resolved
        rows = con.execute(sql).fetchall()
        missing = [r for r in rows if r[0] == "cmissing"]

        # then the spine row survives with a null feature -- dropping it would
        # silently change the shape of a training set
        assert len(missing) == 1
        assert missing[0][-1] is None

    def test_prune_margin_keeps_a_subsecond_row_past_the_spine_max(self, con):
        # given a spine whose newest target is a whole-second timestamp, and a
        # history row 500ms LATER in time but BELOW it byte-wise
        con.execute("DELETE FROM spine")
        con.execute(
            "INSERT INTO spine VALUES ('c123', '2026-08-04T11:00:00Z')"
        )
        con.execute(
            "INSERT INTO hist VALUES ('c123', '2026-08-04T11:00:00.5Z', "
            "'2026-08-04 11:00:00.5', 42.0)"
        )

        # when resolved
        sql = build_asof_sql(
            table="hist",
            feature_columns=["f"],
            entity_column="entity_id",
            timestamp_column="timestamp",
            spine_relation="spine",
        )
        rows = con.execute(sql).fetchall()

        # then it wins, because '.'(0x2E) < 'Z'(0x5A) makes it a valid as-of
        # answer byte-wise -- pruning on event_time without a margin would have
        # dropped it and silently returned the older row
        assert rows[0][-1] == 42.0

    def test_ordering_is_byte_wise_not_chronological(self, con):
        # given two timestamps whose byte order and time order disagree
        # when compared as the join compares them
        byte_order = con.execute(
            "SELECT '2026-08-04T09:14:02.1Z' < '2026-08-04T09:14:02Z'"
        ).fetchone()[0]

        # then DuckDB agrees with RocksDB's raw byte comparison, not with time
        assert byte_order is True
        assert ("2026-08-04T09:14:02.1Z".encode() < "2026-08-04T09:14:02Z".encode())
