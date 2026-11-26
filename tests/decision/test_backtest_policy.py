"""Given / when / then tests for DuckDB decision/outcome reads and Rust-backed policy replay."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import duckdb
import httpx
import pytest

from thyme import Config, ThymeClient
from thyme.backtest import BacktestReport, backtest_policy
from thyme.decision import Rule, attach_outcomes, policy
from thyme.decision_offline import (
    attach_outcomes_duckdb,
    read_decision_outcomes,
    read_decision_records,
)
from thyme.expr import col


def _seed_iceberg_like_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Create local DuckDB tables matching offline-sink's raw Iceberg schema."""
    con.execute(
        """
        CREATE TABLE decision_records (
            entity_id VARCHAR,
            ts VARCHAR,
            event_time TIMESTAMPTZ,
            state_value BLOB,
            op VARCHAR,
            dimensions MAP(VARCHAR, VARCHAR),
            json_features MAP(VARCHAR, VARCHAR),
            renderer_version DOUBLE,
            policy_version DOUBLE,
            latency_ms DOUBLE,
            cost_usd DOUBLE
        );
        CREATE TABLE decision_outcomes (
            entity_id VARCHAR,
            ts VARCHAR,
            event_time TIMESTAMPTZ,
            state_value BLOB,
            op VARCHAR,
            dimensions MAP(VARCHAR, VARCHAR),
            json_features MAP(VARCHAR, VARCHAR),
            value DOUBLE
        );
        """
    )

    records = [
        (
            "dec-1",
            "2026-11-01T14:32:01Z",
            datetime(2026, 11, 1, 14, 32, 1, tzinfo=timezone.utc),
            b"{}",
            "c",
            {
                "featureset": "CardCheckout",
                "entity_id": "card_4417",
                "replica_id": "qs-0",
                "state": json.dumps(
                    {
                        "txn_count_5m": {"value": 2, "description": "Payments in 5m"},
                        "amount": {"value": 950.0, "description": "Amount"},
                    }
                ),
                "state_hash": "hash-1",
                "feature_versions": "{}",
                "questions_hash": "qh-1",
                "model_alias": "jev:jev-1.14.0",
                "model_version": "jev-1.14.0",
                "judgments": json.dumps(
                    {
                        "risk": {
                            "type": "choice",
                            "choice": "takeover",
                            "probabilities": {"ordinary": 0.1, "unusual": 0.19, "takeover": 0.71},
                        },
                        "card_testing": {"type": "noul", "noul": 0.40},
                    }
                ),
                "rule_fired": "2:step_up",
                "action": "step_up",
                "status": "ok",
            },
            {},
            1.0,
            1.0,
            142.0,
            0.00002,
        ),
        (
            "dec-2",
            "2026-11-01T15:00:00Z",
            datetime(2026, 11, 1, 15, 0, 0, tzinfo=timezone.utc),
            b"{}",
            "c",
            {
                "featureset": "CardCheckout",
                "entity_id": "card_9999",
                "replica_id": "qs-0",
                "state": json.dumps(
                    {
                        "txn_count_5m": {"value": 1, "description": "Payments in 5m"},
                        "amount": {"value": 45.0, "description": "Amount"},
                    }
                ),
                "state_hash": "hash-2",
                "feature_versions": "{}",
                "questions_hash": "qh-1",
                "model_alias": "jev:jev-1.14.0",
                "model_version": "jev-1.14.0",
                "judgments": json.dumps(
                    {
                        "risk": {
                            "type": "choice",
                            "choice": "unusual",
                            "probabilities": {"ordinary": 0.3, "unusual": 0.15, "takeover": 0.55},
                        },
                        "card_testing": {"type": "noul", "noul": 0.12},
                    }
                ),
                "rule_fired": "3:approve",
                "action": "approve",
                "status": "ok",
            },
            {},
            1.0,
            1.0,
            110.0,
            0.00002,
        ),
    ]
    con.executemany(
        "INSERT INTO decision_records VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        records,
    )

    outcomes = [
        # 9 days later for card_4417 (no decision_id) -> attaches within 30d window
        (
            "out-in-window",
            "2026-11-10T14:32:01Z",
            datetime(2026, 11, 10, 14, 32, 1, tzinfo=timezone.utc),
            b"{}",
            "c",
            {
                "featureset": "CardCheckout",
                "entity_id": "card_4417",
                "outcome": "chargeback",
                "recorded_at": "2026-11-10T14:33:00Z",
            },
            {},
            950.0,
        ),
        # 45 days later for card_4417 (no decision_id) -> outside 30d window
        (
            "out-expired",
            "2026-12-16T14:32:01Z",
            datetime(2026, 12, 16, 14, 32, 1, tzinfo=timezone.utc),
            b"{}",
            "c",
            {
                "featureset": "CardCheckout",
                "entity_id": "card_4417",
                "outcome": "chargeback",
                "recorded_at": "2026-12-16T14:33:00Z",
            },
            {},
            950.0,
        ),
        # Direct decision_id even 60 days later -> attaches directly
        (
            "out-direct",
            "2027-01-01T00:00:00Z",
            datetime(2027, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            b"{}",
            "c",
            {
                "decision_id": "dec-2",
                "featureset": "CardCheckout",
                "entity_id": "card_9999",
                "outcome": "legit",
                "recorded_at": "2027-01-01T00:01:00Z",
            },
            {},
            45.0,
        ),
    ]
    con.executemany(
        "INSERT INTO decision_outcomes VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        outcomes,
    )


def test_duckdb_reads_decision_records_and_attaches_outcomes_matching_python():
    # Given Iceberg-shaped decision_records and decision_outcomes tables
    con = duckdb.connect()
    _seed_iceberg_like_tables(con)

    # When reading both tables directly via DuckDB and attaching with a 30d window
    records_df = read_decision_records(
        "CardCheckout", con=con, table="decision_records"
    )
    outcomes_df = read_decision_outcomes(
        "CardCheckout", con=con, table="decision_outcomes"
    )
    attached_sql = attach_outcomes_duckdb(
        records_df, outcomes_df, window="30d", con=con
    )
    attached_py = attach_outcomes(records_df, outcomes_df, window="30d")

    # Then DuckDB and Python attachment agree on every outcome
    by_sql = {r["outcome_id"]: r["attached_decision_id"] for r in attached_sql.to_dicts()}
    by_py = {r["outcome_id"]: r["attached_decision_id"] for r in attached_py}
    assert by_sql == {
        "out-in-window": "dec-1",
        "out-expired": None,
        "out-direct": "dec-2",
    }
    assert by_sql == by_py
    assert records_df.height == 2


def test_policy_replay_chunks_requests_through_rust_and_flags_policy_changes():
    # Given two stored decisions (takeover=0.71 step_up, takeover=0.55 approve)
    con = duckdb.connect()
    _seed_iceberg_like_tables(con)
    records = read_decision_records("CardCheckout", con=con, table="decision_records").to_dicts()

    seen_requests: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/decisions/backtest"
        body = json.loads(request.content.decode())
        seen_requests.append(body)
        assert body["mode"] == "policy_replay"
        # Simulate Rust policy v2: step_up when risk.takeover > 0.50
        results = []
        for item in body["items"]:
            j = item["judgments"]
            if isinstance(j, str):
                j = json.loads(j)
            p_takeover = j["risk"]["probabilities"]["takeover"]
            if p_takeover > 0.50:
                action, rule = "step_up", "2:step_up"
            else:
                action, rule = "approve", "3:approve"
            results.append(
                {
                    "id": item["id"],
                    "action": action,
                    "rule_fired": rule,
                    "status": "ok",
                    "latency_ms": 1,
                }
            )
        return httpx.Response(
            200,
            json={
                "mode": "policy_replay",
                "renderer_version": 1,
                "policy_version": body["policy"]["version"],
                "samples": 0,
                "results": results,
            },
        )

    client = ThymeClient(
        config=Config(api_base="http://def.test", query_url="http://qs.test"),
        _transport=httpx.MockTransport(handler),
    )

    candidate = policy(
        Rule("block", when=col("card_testing") > 0.85),
        Rule("step_up", when=col("risk.takeover") > 0.50),
        Rule("approve"),
        fallback="step_up",
        version=2,
    )

    # When running backtest_policy with chunk_size=1
    report = backtest_policy(
        client,
        "CardCheckout",
        policy=candidate,
        records=records,
        chunk_size=1,
    )

    # Then two chunked HTTP calls hit /decisions/backtest and only dec-2 changed
    assert isinstance(report, BacktestReport)
    assert len(seen_requests) == 2
    assert report.policy_version == 2
    assert report.total == 2
    assert report.changed == 1
    assert report.changed_rate == pytest.approx(0.5)
    assert report.counts_by_change_kind == {"unchanged": 1, "policy_change": 1}
    assert report.action_matrix == {("step_up", "step_up"): 1, ("approve", "step_up"): 1}
    by_id = {r.decision_id: r for r in report.rows}
    assert by_id["dec-1"].change_kind == "unchanged"
    assert by_id["dec-2"].change_kind == "policy_change"
    assert by_id["dec-2"].candidate_action == "step_up"
