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
    SHARE_COUNT,
    SHARE_SUM,
    build_asof_sql,
    feature_columns_for,
    resolve_spine,
    windowed_columns,
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
        # The lower bound reads the spine's earliest moment as a scalar
        # subquery. Joining `bounds` in as a relation would plan as a
        # NESTED_LOOP_JOIN instead -- see build_asof_sql.
        assert "(SELECT min_event_time FROM bounds) -" in sql

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
        # `event_time` is TIMESTAMPTZ, as the sink writes it: a naive column
        # here would hide a bound that is cast without a zone.
        con.execute(
            "CREATE TABLE hist(entity_id VARCHAR, ts VARCHAR, "
            "event_time TIMESTAMPTZ, f DOUBLE)"
        )
        con.executemany(
            "INSERT INTO hist VALUES (?, ?, ?::TIMESTAMPTZ, ?)",
            [(e, ts, ts, v) for e, ts, v in self.HISTORY],
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
            "'2026-08-04T11:00:00.5Z'::TIMESTAMPTZ, 42.0)"
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

    def test_the_newest_row_resolves_whatever_the_session_time_zone(self, con):
        # given a session whose zone is east of UTC: a bound cast without a
        # zone reads the spine's "14:35:02Z" as 14:35:02 local, nine hours
        # before the instant the Z names, and prunes every newer row away
        con.execute("SET TimeZone = 'Asia/Tokyo'")
        con.execute("DELETE FROM spine")
        con.execute("INSERT INTO spine VALUES ('c123', '2026-08-04T10:00:01Z')")

        # when resolved
        sql = build_asof_sql(
            table="hist",
            feature_columns=["f"],
            entity_column="entity_id",
            timestamp_column="timestamp",
            spine_relation="spine",
        )
        rows = con.execute(sql).fetchall()

        # then the row one second before the target is the answer, not the
        # newest row that survived the mis-zoned prune
        assert rows == [("c123", "2026-08-04T10:00:01Z", 4.0)]

    def test_ordering_is_byte_wise_not_chronological(self, con):
        # given two timestamps whose byte order and time order disagree
        # when compared as the join compares them
        byte_order = con.execute(
            "SELECT '2026-08-04T09:14:02.1Z' < '2026-08-04T09:14:02Z'"
        ).fetchone()[0]

        # then DuckDB agrees with RocksDB's raw byte comparison, not with time
        assert byte_order is True
        assert ("2026-08-04T09:14:02.1Z".encode() < "2026-08-04T09:14:02Z".encode())


class TestPlanShape:
    """Two query-shape choices that cost three orders of magnitude.

    Both were found by `tests/bench_offline_pull.py`, and neither is visible in
    a test small enough to run in CI: at a few thousand rows the wrong plan
    finishes instantly. So the shape is asserted directly instead.
    """

    def test_bounds_are_scalar_subqueries_not_a_joined_relation(self):
        # given the default pull
        sql = build_asof_sql(
            table="t",
            feature_columns=["f"],
            entity_column="entity_id",
            timestamp_column="timestamp",
        )

        # then `bounds` is never a relation in the history scan's FROM. Written
        # as `FROM t h, bounds`, the inequality against it plans as a
        # NESTED_LOOP_JOIN: 87s of CPU on a 500k-row history.
        assert "bounds" not in sql.split("FROM")[2].split("WHERE")[0]
        assert "(SELECT max_event_time FROM bounds)" in sql

    def test_the_spine_is_materialised_before_the_join(self):
        # given a spine handed over as Arrow
        import duckdb
        import polars as pl

        con = duckdb.connect()
        con.execute(
            "CREATE TABLE hist(entity_id VARCHAR, ts VARCHAR, "
            "event_time TIMESTAMP, f DOUBLE)"
        )
        con.execute(
            "INSERT INTO hist VALUES "
            "('c123', '2026-08-04T09:00:00Z', '2026-08-04 09:00:00', 3.0)"
        )
        spine = pl.DataFrame(
            {"entity_id": ["c123"], "timestamp": ["2026-08-04T10:00:00Z"]}
        ).to_arrow()

        resolve_spine(
            con,
            table="hist",
            feature_columns=["f"],
            spine=spine,
            entity_column="entity_id",
            timestamp_column="timestamp",
            max_lookback=None,
        )

        # then the join reads a real table, not the Arrow view. DuckDB will not
        # use its native ASOF operator against an Arrow scan -- it falls back to
        # NESTED_LOOP_JOIN, measured at 9k rows/sec against 5.1M.
        kind = con.execute(
            "SELECT table_type FROM information_schema.tables "
            "WHERE table_name = 'thyme_spine'"
        ).fetchone()
        assert kind is not None, "spine was never materialised"
        assert kind[0] in ("LOCAL TEMPORARY", "BASE TABLE"), kind
        con.close()


class TestWindowsAreAddedUpFromShares:
    """A windowed Count, Sum or Avg is computed at each spine row from the rows'
    shares, never read from a stored total: a late event's row adds its share
    and no stored total is corrected.

    The rule is the engine's: an event is in the window at T when it is not
    after T and its second is one of the W whole seconds ending with T's.
    """

    WINDOWS = {"n_5m": ("count", 300), "spend_5m": ("sum", 300), "avg_5m": ("avg", 300), "n_1h": ("count", 3600)}
    EVENTS = [
        ("card_4417", "2026-10-26T13:00:30Z", 7.0),
        ("card_4417", "2026-10-26T13:55:07.900Z", 12.0),
        ("card_4417", "2026-10-26T13:59:58Z", 950.0),
        ("card_4417", "2026-10-26T14:00:07.120Z", 5.0),
        ("card_4417", "2026-10-26T14:00:07.650Z", 20.0),
        ("c999", "2026-10-26T14:00:00Z", 1.0),
        # Written last, as a late event is: the pull does not care.
        ("card_4417", "2026-10-26T13:57:30Z", 40.0),
    ]
    SPINE = [
        ("card_4417", "2026-10-26T14:00:07.400Z"),
        ("card_4417", "2026-10-26T14:00:07Z"),
        ("card_4417", "2026-10-26T13:55:07.950Z"),
        ("c999", "2026-10-26T15:00:00Z"),
        ("c999", "2026-10-26T13:00:00Z"),
        ("cmissing", "2026-10-26T14:00:00Z"),
    ]

    @staticmethod
    def _us(ts: str) -> int:
        from datetime import datetime

        return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1_000_000)

    def _expected(self, entity: str, at: str) -> dict:
        """Brute force over the events, None when no row is at or before T."""
        t = self._us(at)
        mine = [(self._us(ts), v) for e, ts, v in self.EVENTS if e == entity]
        if not any(u <= t for u, _ in mine):
            return {c: None for c in self.WINDOWS}
        out = {}
        for col, (kind, w) in self.WINDOWS.items():
            start = (t // 1_000_000 + 1) * 1_000_000 - w * 1_000_000
            inside = [v for u, v in mine if start <= u <= t]
            out[col] = {
                "count": float(len(inside)),
                "sum": sum(inside),
                "avg": sum(inside) / len(inside) if inside else 0.0,
            }[kind]
        return out

    @pytest.fixture
    def con(self):
        con = duckdb.connect()
        shares = ", ".join(f'"{c}{SHARE_SUM}" DOUBLE, "{c}{SHARE_COUNT}" DOUBLE, "{c}" DOUBLE' for c in self.WINDOWS)
        con.execute(f"CREATE TABLE hist(entity_id VARCHAR, ts VARCHAR, event_time TIMESTAMPTZ, {shares})")
        for e, ts, v in self.EVENTS:
            values = []
            for kind, _ in self.WINDOWS.values():
                # The stored total is deliberately wrong: the pull must not read it.
                values += [1.0 if kind == "count" else v, 1.0, -1.0]
            con.execute(
                f"INSERT INTO hist VALUES (?, ?, ?::TIMESTAMPTZ, {', '.join('?' * len(values))})",
                [e, ts, ts, *values],
            )
        con.execute("CREATE TABLE spine(entity_id VARCHAR, timestamp VARCHAR)")
        con.executemany("INSERT INTO spine VALUES (?, ?)", self.SPINE)
        return con

    def test_each_spine_row_gets_the_exact_window_at_its_time(self, con):
        # given the pull over rows that carry shares, with two window lengths
        sql = build_asof_sql(
            table="hist",
            feature_columns=list(self.WINDOWS),
            entity_column="entity_id",
            timestamp_column="timestamp",
            spine_relation="spine",
            windows=self.WINDOWS,
        )

        # when it runs
        rows = con.execute(sql).fetchall()

        # then every spine row holds the window by the engine's rule: a later
        # event in T's own second is out, the second W back is out, an empty
        # window is 0, and an entity with no row by T is null
        assert len(rows) == len(self.SPINE)
        for entity, at, *values in rows:
            got = dict(zip(self.WINDOWS, values))
            want = self._expected(entity, at)
            for col in self.WINDOWS:
                assert got[col] == pytest.approx(want[col]) if want[col] is not None else got[col] is None, (
                    f"{entity} @ {at} {col}: got {got[col]}, want {want[col]}"
                )

    def test_the_worked_example(self, con):
        # given card_4417's spine row at 14:00:07.400
        sql = build_asof_sql(
            table="hist", feature_columns=list(self.WINDOWS), entity_column="entity_id",
            timestamp_column="timestamp", spine_relation="spine", windows=self.WINDOWS,
        )

        # when the pull runs
        rows = {(r[0], r[1]): r[2:] for r in con.execute(sql).fetchall()}

        # then the 5-minute window holds 40 + 950 + 5, and the hour 5 payments
        n_5m, spend_5m, avg_5m, n_1h = rows[("card_4417", "2026-10-26T14:00:07.400Z")]
        assert (n_5m, spend_5m, n_1h) == (3.0, 995.0, 5.0)
        assert avg_5m == pytest.approx(995.0 / 3)


class TestWindowedColumns:
    def test_lists_each_windowed_count_sum_and_avg_of_a_dataset(self):
        # given a pipeline aggregating into CardStats
        from thyme.dataset import _PIPELINE_REGISTRY

        _PIPELINE_REGISTRY[("CardStats", "p")] = {
            "output_dataset": "CardStats",
            "operators": [
                {"filter": {}},
                {"aggregate": {"keys": ["card_id"], "specs": [
                    {"type": "count", "window": "5m", "output_field": "n_5m"},
                    {"type": "avg", "window": "1h", "output_field": "avg_1h"},
                    {"type": "max", "window": "1h", "output_field": "max_1h"},
                    {"type": "sum", "window": "", "output_field": "spend_all"},
                ]}},
            ],
        }
        try:
            # then only the bounded Count, Sum and Avg are listed
            assert windowed_columns("CardStats") == {"n_5m": ("count", 300), "avg_1h": ("avg", 3600)}
            assert windowed_columns("Other") == {}
        finally:
            del _PIPELINE_REGISTRY[("CardStats", "p")]
