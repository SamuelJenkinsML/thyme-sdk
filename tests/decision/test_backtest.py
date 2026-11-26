"""Given / when / then tests for re-ask, rebuild, checkpointing, Python deciders, and compare_arms."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import polars as pl
import pytest

from thyme import Config, ThymeClient, feature, featureset
from thyme.backtest import (
    CallableDecider,
    ComparisonReport,
    DeciderResult,
    SklearnDecider,
    SystemOneArm,
    backtest_reask,
    backtest_rebuild,
    compare_arms,
)
from thyme.decision import Provider
from thyme.result import ThymeResult


def _sample_records() -> list[dict[str, Any]]:
    return [
        {
            "decision_id": "dec-card4417",
            "featureset": "CardCheckout",
            "entity_id": "card_4417",
            "decided_at": "2026-11-01T14:32:01Z",
            "state": json.dumps(
                {
                    "txn_count_5m": {"value": 2, "description": "Payments in 5m"},
                    "spend_24h": {"value": 1002.0, "description": "Spend in 24h"},
                    "amount": {"value": 950.0, "description": "Amount"},
                }
            ),
            "state_hash": "hash-live-1",
            "model_version": "jev-1.14.0",
            "judgments": json.dumps(
                {
                    "risk": {
                        "type": "choice",
                        "choice": "takeover",
                        "probabilities": {"ordinary": 0.1, "unusual": 0.29, "takeover": 0.61},
                    },
                    "card_testing": {"type": "noul", "noul": 0.40},
                }
            ),
            "policy_version": 1,
            "rule_fired": "2:step_up",
            "action": "step_up",
            "status": "ok",
        },
        {
            "decision_id": "dec-card9999",
            "featureset": "CardCheckout",
            "entity_id": "card_9999",
            "decided_at": "2026-11-01T14:40:00Z",
            "state": json.dumps(
                {
                    "txn_count_5m": {"value": 1, "description": "Payments in 5m"},
                    "spend_24h": {"value": 50.0, "description": "Spend in 24h"},
                    "amount": {"value": 50.0, "description": "Amount"},
                }
            ),
            "state_hash": "hash-live-2",
            "model_version": "jev-1.14.0",
            "judgments": json.dumps(
                {
                    "risk": {
                        "type": "choice",
                        "choice": "ordinary",
                        "probabilities": {"ordinary": 0.85, "unusual": 0.10, "takeover": 0.05},
                    },
                    "card_testing": {"type": "noul", "noul": 0.05},
                }
            ),
            "policy_version": 1,
            "rule_fired": "3:approve",
            "action": "approve",
            "status": "ok",
        },
    ]


def test_reask_classifies_within_noise_vs_model_change_and_resumes_checkpoint(tmp_path: Path):
    # Given two decisions where candidate flips dec-card4417 by 0.02 (within noise)
    # and flips dec-card9999 by 0.65 (clear model_change)
    records = _sample_records()
    calls: list[list[str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode())
        assert body["mode"] == "reask"
        assert body["samples"] == 3
        calls.append([item["id"] for item in body["items"]])
        results = []
        for item in body["items"]:
            if item["id"] == "dec-card4417":
                # Takeover moves 0.61 -> 0.59 (delta 0.02 <= 0.038 noise band) -> flips to approve
                results.append(
                    {
                        "id": item["id"],
                        "state_hash": "hash-live-1",
                        "model_version": "d3-flash-1.0",
                        "judgments": {
                            "risk": {
                                "type": "choice",
                                "choice": "unusual",
                                "probabilities": {"ordinary": 0.12, "unusual": 0.29, "takeover": 0.59},
                            },
                            "card_testing": {"type": "noul", "noul": 0.39},
                        },
                        "spread": {"risk": {"probabilities_sd": {"takeover": 0.015}, "samples": 3}},
                        "sample_actions": ["approve", "step_up", "approve"],
                        "action": "approve",
                        "rule_fired": "3:approve",
                        "status": "ok",
                        "latency_ms": 120,
                        "cost_usd": 0.00005,
                    }
                )
            else:
                # Takeover moves 0.05 -> 0.70 (delta 0.65 > 0.038) -> model_change
                results.append(
                    {
                        "id": item["id"],
                        "state_hash": "hash-live-2",
                        "model_version": "d3-flash-1.0",
                        "judgments": {
                            "risk": {
                                "type": "choice",
                                "choice": "takeover",
                                "probabilities": {"ordinary": 0.10, "unusual": 0.20, "takeover": 0.70},
                            },
                            "card_testing": {"type": "noul", "noul": 0.10},
                        },
                        "spread": {"risk": {"probabilities_sd": {"takeover": 0.01}, "samples": 3}},
                        "sample_actions": ["step_up", "step_up", "step_up"],
                        "action": "step_up",
                        "rule_fired": "2:step_up",
                        "status": "ok",
                        "latency_ms": 140,
                        "cost_usd": 0.00005,
                    }
                )
        return httpx.Response(
            200,
            json={
                "mode": "reask",
                "renderer_version": 1,
                "policy_version": 1,
                "samples": body["samples"],
                "results": results,
            },
        )

    client = ThymeClient(
        config=Config(api_base="http://def.test", query_url="http://qs.test"),
        _transport=httpx.MockTransport(handler),
    )
    ckpt = tmp_path / "reask.jsonl"

    # When running re-ask with checkpointing
    report = backtest_reask(
        client,
        "CardCheckout",
        model="d3-flash-1.0",
        records=records,
        checkpoint_path=ckpt,
        chunk_size=1,
    )

    # Then both noise labels appear and checkpoint contains both decisions
    by_id = {r.decision_id: r for r in report.rows}
    assert by_id["dec-card4417"].change_kind == "within_noise"
    assert by_id["dec-card9999"].change_kind == "model_change"
    assert len(calls) == 2

    # And when re-running against the same checkpoint, zero additional HTTP calls are made
    calls.clear()
    resumed = backtest_reask(
        client,
        "CardCheckout",
        model="d3-flash-1.0",
        records=records,
        checkpoint_path=ckpt,
        chunk_size=1,
    )
    assert calls == []
    assert resumed.counts_by_change_kind == {"within_noise": 1, "model_change": 1}


def test_reask_uses_provider_samples_setting_for_deterministic_models():
    # Given a deterministic provider declaring samples=1
    d3 = Provider("d3", base_url="http://localhost:8099", samples=1)
    seen_samples: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode())
        seen_samples.append(body["samples"])
        return httpx.Response(
            200,
            json={
                "mode": "reask",
                "renderer_version": 1,
                "policy_version": 1,
                "samples": body["samples"],
                "results": [
                    {
                        "id": body["items"][0]["id"],
                        "action": "step_up",
                        "rule_fired": "2:step_up",
                        "judgments": {},
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

    backtest_reask(client, "CardCheckout", provider=d3, model="d3-edge@bee5e872", records=_sample_records()[:1])
    assert seen_samples == [1]


def test_rebuild_detects_late_event_drift_and_emits_late_data_warning():
    # Given card_4417 whose live decision saw txn_count_5m=2, while offline event-time
    # rebuild includes the late $40 payment (txn_count_5m=3, spend_24h=1042.0)
    @featureset
    class CardCheckoutRebuild:
        card_id: str = feature()
        txn_count_5m: int = feature(description="Payments in 5m")
        spend_24h: float = feature(description="Spend in 24h")
        amount: float = feature(request=True, description="Amount")

    records = _sample_records()[:1]
    records[0]["featureset"] = "CardCheckoutRebuild"

    sent_states: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode())
        assert body["mode"] == "rebuild"
        sent_states.append(body["items"][0]["state"])
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
                        "state_hash": "hash-rebuilt-1",
                        "judgments": {
                            "risk": {
                                "type": "choice",
                                "choice": "takeover",
                                "probabilities": {"ordinary": 0.05, "unusual": 0.10, "takeover": 0.85},
                            },
                            "card_testing": {"type": "noul", "noul": 0.90},
                        },
                        "action": "block",
                        "rule_fired": "1:block",
                        "status": "ok",
                        "latency_ms": 95,
                    }
                ],
            },
        )

    client = ThymeClient(
        config=Config(api_base="http://def.test", query_url="http://qs.test"),
        _transport=httpx.MockTransport(handler),
    )

    def fake_query_offline(fs, spine, *, entity_column, timestamp_column, **kwargs):
        df = pl.DataFrame(
            [
                {
                    entity_column: "card_4417",
                    timestamp_column: "2026-11-01T14:32:01Z",
                    "txn_count_5m": 3,
                    "spend_24h": 1042.0,
                }
            ]
        )
        return ThymeResult(df, metadata={"mode": "offline"})

    client.query_offline = fake_query_offline  # type: ignore[method-assign]

    # When running rebuild
    report = backtest_rebuild(
        client,
        CardCheckoutRebuild,
        records=records,
        entity_column="card_id",
        samples=1,
    )

    # Then request feature `amount` was preserved alongside rebuilt windows,
    # late_data was flagged, and the event-time warning is present
    assert sent_states == [{"txn_count_5m": 3, "spend_24h": 1042.0, "amount": 950.0}]
    assert report.rows[0].late_data is True
    assert report.rows[0].change_kind == "late_data"
    assert any("event time" in w.lower() for w in report.warnings)


class _DummySklearnEstimator:
    """Minimal scikit-learn predict_proba stub (avoids adding sklearn as a hard dep)."""

    def predict_proba(self, X):
        out = []
        for row in X:
            p = 0.85 if row[0] >= 2 and row[1] >= 500 else 0.10
            out.append([1.0 - p, p])
        return out


def test_compare_arms_runs_system_one_and_python_deciders_in_one_table():
    # Given stored records and outcomes for two cards
    records = _sample_records()
    outcomes = [
        {"decision_id": "dec-card4417", "outcome": "chargeback", "occurred_at": "2026-11-10T00:00:00Z"},
        {"decision_id": "dec-card9999", "outcome": "legit", "occurred_at": "2026-11-10T00:00:00Z"},
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode())
        results = []
        for item in body["items"]:
            if item["id"] == "dec-card4417":
                results.append(
                    {
                        "id": item["id"],
                        "judgments": {
                            "risk": {
                                "type": "choice",
                                "choice": "takeover",
                                "probabilities": {"ordinary": 0.1, "unusual": 0.1, "takeover": 0.80},
                            }
                        },
                        "action": "step_up",
                        "rule_fired": "2:step_up",
                        "status": "ok",
                        "latency_ms": 150,
                        "cost_usd": 0.00003,
                    }
                )
            else:
                results.append(
                    {
                        "id": item["id"],
                        "judgments": {
                            "risk": {
                                "type": "choice",
                                "choice": "ordinary",
                                "probabilities": {"ordinary": 0.9, "unusual": 0.05, "takeover": 0.05},
                            }
                        },
                        "action": "approve",
                        "rule_fired": "3:approve",
                        "status": "ok",
                        "latency_ms": 120,
                        "cost_usd": 0.00003,
                    }
                )
        return httpx.Response(
            200,
            json={
                "mode": "reask",
                "renderer_version": 1,
                "policy_version": 1,
                "samples": body["samples"],
                "results": results,
            },
        )

    client = ThymeClient(
        config=Config(api_base="http://def.test", query_url="http://qs.test"),
        _transport=httpx.MockTransport(handler),
    )

    jev_arm = SystemOneArm(name="jev-1.14.0", model="jev-1.14.0", samples=1)
    sk_arm = SklearnDecider(
        name="rf-baseline",
        estimator=_DummySklearnEstimator(),
        features=("txn_count_5m", "amount"),
        threshold=0.5,
        positive_action="step_up",
        negative_action="approve",
        probability_key="risk.takeover",
    )
    llm_arm = CallableDecider(
        name="custom-rule",
        fn=lambda state: DeciderResult(
            action="step_up" if (state.get("amount") or 0) > 500 else "approve",
            probabilities={"risk.takeover": 0.75 if (state.get("amount") or 0) > 500 else 0.15},
        ),
    )

    # When comparing all three arms on the same states
    comp = compare_arms(
        client,
        "CardCheckout",
        [jev_arm, sk_arm, llm_arm],
        records=records,
        outcomes=outcomes,
        positive_outcome="chargeback",
        probability_key="risk.takeover",
    )

    # Then one table compares System One, sklearn, and callable deciders side by side
    assert isinstance(comp, ComparisonReport)
    df = comp.to_dataframe()
    assert df.height == 3
    assert set(df["arm"].to_list()) == {"jev-1.14.0", "rf-baseline", "custom-rule"}
    by_arm = {s.arm: s for s in comp.summaries}
    assert by_arm["jev-1.14.0"].agreement_vs_live == pytest.approx(1.0)
    assert by_arm["rf-baseline"].agreement_vs_live == pytest.approx(1.0)
    assert by_arm["rf-baseline"].brier_score == pytest.approx(((0.85 - 1.0) ** 2 + (0.10 - 0.0) ** 2) / 2)
    assert by_arm["custom-rule"].brier_score == pytest.approx(((0.75 - 1.0) ** 2 + (0.15 - 0.0) ** 2) / 2)
