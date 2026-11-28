"""Tests for Knowledge Time (`as_of='known'`) in DuckDB offline pulls and backtest rebuilds."""

from __future__ import annotations

import json
import time

import duckdb
import httpx
import polars as pl
import pytest

from thyme import Config, ThymeClient, feature, featureset
from thyme.backtest import LATE_DATA_EVENT_TIME_WARNING, backtest_rebuild
from thyme.offline_iceberg import SHARE_COUNT, SHARE_SUM, build_asof_sql
from thyme.result import ThymeResult


def _make_card_4417_con() -> duckdb.DuckDBPyConnection:
    """Running example: payments at 14:31:20 ($12), 14:31:40 ($40 on-time? no:
    two on-time payments at 14:31:20 & 14:31:40, plus a late $40 payment at
    event_time 14:31:50 committed at known_at 14:33:12).
    """
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE card_stats(
            entity_id VARCHAR,
            ts VARCHAR,
            event_time TIMESTAMPTZ,
            known_at TIMESTAMPTZ,
            txn_count_5m DOUBLE,
            txn_count_5m__share_count DOUBLE,
            spend_24h DOUBLE,
            spend_24h__share_sum DOUBLE,
            max_amount_24h DOUBLE
        )
        """
    )
    rows = [
        (
            "card_4417",
            "2026-11-01T14:31:20.000000Z",
            "2026-11-01T14:31:20.000000Z",
            "2026-11-01T14:31:21.000000Z",
            1.0,
            1.0,
            12.0,
            12.0,
            12.0,
        ),
        (
            "card_4417",
            "2026-11-01T14:31:40.000000Z",
            "2026-11-01T14:31:40.000000Z",
            "2026-11-01T14:31:41.000000Z",
            2.0,
            1.0,
            1002.0,
            990.0,
            990.0,
        ),
        # Late $40 payment: event_time 14:31:50, committed at 14:33:12
        (
            "card_4417",
            "2026-11-01T14:31:50.000000Z",
            "2026-11-01T14:31:50.000000Z",
            "2026-11-01T14:33:12.000000Z",
            None,
            1.0,
            None,
            40.0,
            1050.0,
        ),
    ]
    con.executemany(
        "INSERT INTO card_stats VALUES (?, ?, ?::TIMESTAMPTZ, ?::TIMESTAMPTZ, ?, ?, ?, ?, ?)",
        rows,
    )
    con.execute("CREATE TABLE spine(card_id VARCHAR, decided_at VARCHAR)")
    con.executemany(
        "INSERT INTO spine VALUES (?, ?)",
        [
            ("card_4417", "2026-11-01T14:32:01.000000Z"),
            ("card_4417", "2026-11-01T14:34:00.000000Z"),
        ],
    )
    return con


def test_known_time_pull_excludes_late_payment_at_14_32_01_and_includes_at_14_34_00():
    # Given the running example with a late $40 payment committed at 14:33:12
    con = _make_card_4417_con()
    windows = {
        "txn_count_5m": ("count", 300),
        "spend_24h": ("sum", 86_400),
    }

    # When resolved with as_of="known"
    sql = build_asof_sql(
        table="card_stats",
        feature_columns=["txn_count_5m", "spend_24h", "max_amount_24h"],
        entity_column="card_id",
        timestamp_column="decided_at",
        spine_relation="spine",
        windows=windows,
        as_of="known",
    )
    assert "*" not in sql
    got = {row[1]: row[2:] for row in con.execute(sql).fetchall()}

    # Then at 14:32:01 the late $40 payment is excluded (count=2, spend=1002, max=990)
    assert got["2026-11-01T14:32:01.000000Z"] == pytest.approx((2.0, 1002.0, 990.0))
    # And at 14:34:00 the late $40 payment is included (count=3, spend=1042, max=1050)
    assert got["2026-11-01T14:34:00.000000Z"] == pytest.approx((3.0, 1042.0, 1050.0))


def test_known_time_pull_resolves_bitemporal_versions_of_updated_source_row():
    # Given a source row updated at known_at=12:00 after originally arriving at 10:00:05
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE merchant(
            entity_id VARCHAR,
            ts VARCHAR,
            event_time TIMESTAMPTZ,
            known_at TIMESTAMPTZ,
            risk_score DOUBLE
        )
        """
    )
    con.executemany(
        "INSERT INTO merchant VALUES (?, ?, ?::TIMESTAMPTZ, ?::TIMESTAMPTZ, ?)",
        [
            ("m1", "2026-11-01T10:00:00.000000Z", "2026-11-01T10:00:00Z", "2026-11-01T10:00:05Z", 0.2),
            ("m1", "2026-11-01T10:00:00.000000Z", "2026-11-01T10:00:00Z", "2026-11-01T12:00:00Z", 0.9),
        ],
    )
    con.execute("CREATE TABLE spine(merchant_id VARCHAR, at_ts VARCHAR)")
    con.executemany(
        "INSERT INTO spine VALUES (?, ?)",
        [
            ("m1", "2026-11-01T10:00:02.000000Z"),  # before v1 was known -> NULL
            ("m1", "2026-11-01T11:00:00.000000Z"),  # v1 known, v2 not yet -> 0.2
            ("m1", "2026-11-01T13:00:00.000000Z"),  # v2 known -> 0.9
        ],
    )

    # When pulled with as_of="known"
    sql_known = build_asof_sql(
        table="merchant",
        feature_columns=["risk_score"],
        entity_column="merchant_id",
        timestamp_column="at_ts",
        spine_relation="spine",
        as_of="known",
    )
    got_known = {row[1]: row[2] for row in con.execute(sql_known).fetchall()}
    assert got_known["2026-11-01T10:00:02.000000Z"] is None
    assert got_known["2026-11-01T11:00:00.000000Z"] == pytest.approx(0.2)
    assert got_known["2026-11-01T13:00:00.000000Z"] == pytest.approx(0.9)

    # And when pulled with as_of="event" on the same versioned table, the latest version wins
    sql_event = build_asof_sql(
        table="merchant",
        feature_columns=["risk_score"],
        entity_column="merchant_id",
        timestamp_column="at_ts",
        spine_relation="spine",
        as_of="event",
        has_known_at=True,
    )
    got_event = {row[1]: row[2] for row in con.execute(sql_event).fetchall()}
    assert got_event["2026-11-01T11:00:00.000000Z"] == pytest.approx(0.9)


def test_known_time_pull_deduplicates_versioned_window_shares():
    # Given a windowed event that was rewritten (two versions of the same (entity_id, ts))
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE stats(
            entity_id VARCHAR,
            ts VARCHAR,
            event_time TIMESTAMPTZ,
            known_at TIMESTAMPTZ,
            total_1h DOUBLE,
            total_1h__share_sum DOUBLE,
            total_1h__share_count DOUBLE
        )
        """
    )
    con.executemany(
        "INSERT INTO stats VALUES (?, ?, ?::TIMESTAMPTZ, ?::TIMESTAMPTZ, ?, ?, ?)",
        [
            ("c1", "2026-11-01T10:00:00.000000Z", "2026-11-01T10:00:00Z", "2026-11-01T10:00:05Z", 10.0, 10.0, 1.0),
            ("c1", "2026-11-01T10:00:00.000000Z", "2026-11-01T10:00:00Z", "2026-11-01T10:10:00Z", 25.0, 25.0, 1.0),
        ],
    )
    con.execute("CREATE TABLE spine(entity_id VARCHAR, ts VARCHAR)")
    con.executemany(
        "INSERT INTO spine VALUES (?, ?)",
        [
            ("c1", "2026-11-01T10:05:00.000000Z"),
            ("c1", "2026-11-01T10:15:00.000000Z"),
        ],
    )

    sql = build_asof_sql(
        table="stats",
        feature_columns=["total_1h"],
        entity_column="entity_id",
        timestamp_column="ts",
        spine_relation="spine",
        windows={"total_1h": ("sum", 3600)},
        as_of="known",
    )
    got = {row[1]: row[2] for row in con.execute(sql).fetchall()}
    # Must never double-count: 10.0 at 10:05 (v1), 25.0 at 10:15 (v2)
    assert got["2026-11-01T10:05:00.000000Z"] == pytest.approx(10.0)
    assert got["2026-11-01T10:15:00.000000Z"] == pytest.approx(25.0)


def test_known_time_pull_meets_60k_rows_per_sec_on_hot_key_with_late_events():
    # Given 100k events on a single hot key with 5% late arrivals (delayed 2-10 minutes)
    # and a 100k-row spine on that same hot key
    n_events = 100_000
    n_spine = 100_000
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")

    base = pl.datetime(2026, 3, 1, time_zone="UTC")
    idx = pl.int_range(0, n_events, dtype=pl.Int64)
    ev_t = base + pl.duration(seconds=idx * 6)  # ~7 days
    # 5% of rows arrive 120s late; normal rows have 0s lag (on-time) or 50ms lag
    lag_ms = pl.when((idx % 20) == 0).then(120_000).otherwise(50)
    hist_df = pl.select(
        entity_id=pl.lit("e_hot"),
        ts=ev_t.dt.strftime("%Y-%m-%dT%H:%M:%S%.6fZ"),
        event_time=ev_t,
        known_at=ev_t + pl.duration(milliseconds=lag_ms),
        cnt_1h=pl.lit(1.0),
        **{"cnt_1h" + SHARE_COUNT: pl.lit(1.0)},
        **{"sum_1h": pl.lit(10.0)},
        **{"sum_1h" + SHARE_SUM: pl.lit(10.0)},
    )
    spine_idx = pl.int_range(0, n_spine, dtype=pl.Int64)
    sp_t = base + pl.duration(seconds=((spine_idx * 7919) % (n_events * 6)))
    spine_df = pl.select(
        entity_id=pl.lit("e_hot"),
        timestamp=sp_t.dt.strftime("%Y-%m-%dT%H:%M:%S%.6fZ"),
    )

    con.register("h_arrow", hist_df.to_arrow())
    con.execute("CREATE TEMP TABLE hot_hist AS SELECT * FROM h_arrow")
    con.register("s_arrow", spine_df.to_arrow())
    con.execute("CREATE TEMP TABLE hot_spine AS SELECT * FROM s_arrow")

    sql = build_asof_sql(
        table="hot_hist",
        feature_columns=["cnt_1h", "sum_1h"],
        entity_column="entity_id",
        timestamp_column="timestamp",
        spine_relation="hot_spine",
        windows={"cnt_1h": ("count", 3600), "sum_1h": ("sum", 3600)},
        as_of="known",
    )

    # When executed
    t0 = time.perf_counter()
    res = con.execute(sql).pl()
    elapsed = time.perf_counter() - t0
    rate = len(res) / elapsed

    # Then every spine row resolves and throughput exceeds 60,000 rows/sec
    assert len(res) == n_spine
    assert rate >= 60_000, f"known-time hot-key pull ran at {rate:,.0f} rows/sec (< 60,000)"


def test_known_time_pull_handles_ultra_late_event_delayed_longer_than_window():
    # Given a 5m window where an event at 10:00 arrives 2 hours late (known_at=12:00),
    # plus an on-time event at 10:10 (known_at=10:10:01).
    # At T=10:12 (window [10:07:01, 10:12]), the 10:00 event is outside the window AND
    # not yet known (cell2 case: t < lo AND k > T). True count at 10:12 is 1, not 0!
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE stats(
            entity_id VARCHAR,
            ts VARCHAR,
            event_time TIMESTAMPTZ,
            known_at TIMESTAMPTZ,
            cnt_5m DOUBLE,
            cnt_5m__share_count DOUBLE
        )
        """
    )
    con.executemany(
        "INSERT INTO stats VALUES (?, ?, ?::TIMESTAMPTZ, ?::TIMESTAMPTZ, ?, ?)",
        [
            ("c1", "2026-11-01T10:00:00.000000Z", "2026-11-01T10:00:00Z", "2026-11-01T12:00:00Z", None, 1.0),
            ("c1", "2026-11-01T10:10:00.000000Z", "2026-11-01T10:10:00Z", "2026-11-01T10:10:01Z", 1.0, 1.0),
        ],
    )
    con.execute("CREATE TABLE spine(entity_id VARCHAR, ts VARCHAR)")
    con.executemany(
        "INSERT INTO spine VALUES (?, ?)",
        [
            ("c1", "2026-11-01T10:12:00.000000Z"),
            ("c1", "2026-11-01T12:05:00.000000Z"),
        ],
    )

    sql = build_asof_sql(
        table="stats",
        feature_columns=["cnt_5m"],
        entity_column="entity_id",
        timestamp_column="ts",
        spine_relation="spine",
        windows={"cnt_5m": ("count", 300)},
        as_of="known",
    )
    got = {row[1]: row[2] for row in con.execute(sql).fetchall()}
    assert got["2026-11-01T10:12:00.000000Z"] == pytest.approx(1.0)
    assert got["2026-11-01T12:05:00.000000Z"] == pytest.approx(0.0)


def test_backtest_rebuild_defaults_to_known_time_without_late_data_warning():
    # Given card_4417 whose live decision saw txn_count_5m=2
    @featureset
    class CardCheckoutKnownRebuild:
        card_id: str = feature()
        txn_count_5m: int = feature(description="Payments in 5m")
        spend_24h: float = feature(description="Spend in 24h")
        amount: float = feature(request=True, description="Amount")

    records = [
        {
            "decision_id": "dec-card4417",
            "featureset": "CardCheckoutKnownRebuild",
            "entity_id": "card_4417",
            "decided_at": "2026-11-01T14:32:01Z",
            "state": json.dumps(
                {
                    "txn_count_5m": {"value": 2, "description": "Payments in 5m"},
                    "spend_24h": {"value": 1002.0, "description": "Spend in 24h"},
                    "amount": {"value": 950.0, "description": "Amount"},
                }
            ),
            "judgments": json.dumps({}),
            "action": "step_up",
            "rule_fired": "2:step_up",
            "status": "ok",
        }
    ]

    seen_as_of: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode())
        return httpx.Response(
            200,
            json={
                "mode": "rebuild",
                "renderer_version": 1,
                "policy_version": 1,
                "samples": body["samples"],
                "results": [
                    {
                        "id": "dec-card4417",
                        "state_hash": "hash-match",
                        "judgments": {},
                        "action": "step_up",
                        "rule_fired": "2:step_up",
                        "status": "ok",
                        "latency_ms": 50,
                    }
                ],
            },
        )

    client = ThymeClient(
        config=Config(api_base="http://def.test", query_url="http://qs.test"),
        _transport=httpx.MockTransport(handler),
    )

    def fake_query_offline(fs, spine, *, entity_column, timestamp_column, as_of="event", **kwargs):
        seen_as_of.append(as_of)
        df = pl.DataFrame(
            [
                {
                    entity_column: "card_4417",
                    timestamp_column: "2026-11-01T14:32:01Z",
                    "txn_count_5m": 2 if as_of == "known" else 3,
                    "spend_24h": 1002.0 if as_of == "known" else 1042.0,
                }
            ]
        )
        return ThymeResult(df, metadata={"mode": "offline", "as_of": as_of})

    client.query_offline = fake_query_offline  # type: ignore[method-assign]

    # When running rebuild with default arguments
    report = backtest_rebuild(
        client,
        CardCheckoutKnownRebuild,
        records=records,
        entity_column="card_id",
        samples=1,
    )

    # Then as_of="known" was used, no late-data drift occurred, and the warning is gone
    assert seen_as_of == ["known"]
    assert report.rows[0].late_data is False
    assert report.rows[0].drifted_features == ()
    assert LATE_DATA_EVENT_TIME_WARNING not in report.warnings
