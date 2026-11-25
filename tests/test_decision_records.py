"""Given / when / then tests for decision records, outcomes, and attach windows."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from thyme import Config, ThymeClient, dataset, feature, featureset, field
from thyme.decision import Provider, Rule, attach_outcomes, judge, outcome, policy
from thyme.expr import col


def _mock_client(handler) -> ThymeClient:
    transport = httpx.MockTransport(handler)
    cfg = Config(api_url="http://def.local", query_url="http://qs.local")
    return ThymeClient(config=cfg, _transport=transport)


def test_outcome_inside_window_attaches_to_latest_decision():
    # Given two decisions for card_4417, 10 days apart
    t0 = datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(days=10)
    decisions = [
        {"decision_id": "d_old", "featureset": "CardCheckout", "entity_id": "card_4417", "decided_at": t0.isoformat()},
        {"decision_id": "d_new", "featureset": "CardCheckout", "entity_id": "card_4417", "decided_at": t1.isoformat()},
    ]
    # When a chargeback arrives 9 days after the second decision with no decision_id
    outcomes = [
        {
            "outcome_id": "o1",
            "decision_id": None,
            "featureset": "CardCheckout",
            "entity_id": "card_4417",
            "outcome": "chargeback",
            "value": 950.0,
            "occurred_at": (t1 + timedelta(days=9)).isoformat(),
        }
    ]
    attached = attach_outcomes(decisions, outcomes, window="30d")

    # Then it attaches to the latest decision inside the 30-day window
    assert len(attached) == 1
    assert attached[0]["attached_decision_id"] == "d_new"


def test_outcome_outside_window_does_not_attach():
    # Given a decision 45 days before the outcome
    t0 = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
    decisions = [
        {"decision_id": "d_old", "featureset": "CardCheckout", "entity_id": "card_4417", "decided_at": t0.isoformat()},
    ]
    outcomes = [
        {
            "outcome_id": "o_late",
            "decision_id": None,
            "featureset": "CardCheckout",
            "entity_id": "card_4417",
            "outcome": "chargeback",
            "occurred_at": (t0 + timedelta(days=45)).isoformat(),
        }
    ]

    # When attached with a 30-day window
    attached = attach_outcomes(decisions, outcomes, window="30d")

    # Then the outcome outside the window does not attach
    assert attached[0]["attached_decision_id"] is None
    assert attached[0]["decision"] is None


def test_outcome_with_explicit_decision_id_attaches_directly():
    # Given two decisions where the explicit decision_id points to the older one
    t0 = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(days=5)
    decisions = [
        {"decision_id": "d_first", "featureset": "CardCheckout", "entity_id": "card_4417", "decided_at": t0.isoformat()},
        {"decision_id": "d_second", "featureset": "CardCheckout", "entity_id": "card_4417", "decided_at": t1.isoformat()},
    ]
    outcomes = [
        {
            "outcome_id": "o_direct",
            "decision_id": "d_first",
            "featureset": "CardCheckout",
            "entity_id": "card_4417",
            "outcome": "step_up_passed",
            "occurred_at": (t0 + timedelta(days=40)).isoformat(),
        }
    ]

    # When attached with a 30-day window
    attached = attach_outcomes(decisions, outcomes, window="30d")

    # Then the explicit decision_id wins directly
    assert attached[0]["attached_decision_id"] == "d_first"


def test_outcome_decorator_records_attach_window_metadata():
    # Given a dataset decorated with @outcome
    @outcome(featureset="CardCheckout", window="30d")
    @dataset()
    class Chargebacks:
        outcome_id: str = field(key=True)
        card_id: str = field()
        occurred_at: datetime = field(timestamp=True)

    # Then the dataset carries its outcome attach window metadata
    assert Chargebacks._outcome_spec.featureset == "CardCheckout"
    assert Chargebacks._outcome_spec.window == timedelta(days=30)
    assert Chargebacks._dataset_meta["outcome"] == {
        "featureset": "CardCheckout",
        "window_seconds": 30 * 86400,
    }


def test_featureset_record_mode_validation_and_payload():
    # Given a valid async featureset
    prov = Provider("jev", base_url="http://localhost:8098")

    @featureset(record="async")
    class AsyncCheckout:
        card_id: str = feature()
        amount: float = feature(request=True, description="Payment amount")
        risk: float = judge.noul("Fraud?", state=[amount], provider=prov, model="jev-1.14.0")
        action: str = policy(Rule("block", when=col("risk") > 0.8), Rule("approve"), fallback="step_up")

    assert AsyncCheckout._featureset_meta["record_mode"] == "async"

    # When an invalid record mode is passed, then it fails immediately
    with pytest.raises(ValueError, match="expected 'sync' or 'async'"):
        @featureset(record="invalid")
        class BadRecordMode:
            card_id: str = feature()


def test_client_record_decision_record_outcome_get_and_erase():
    # Given a mocked definition-service and query-server transport
    seen: list[tuple[str, str, dict | None, dict]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode()) if request.content else None
        seen.append((request.method, str(request.url), body, dict(request.headers)))
        if request.url.path == "/api/v1/decisions":
            return httpx.Response(200, json={"decision_id": "0193-dec-1"})
        if request.url.path == "/api/v1/outcomes":
            return httpx.Response(200, json={"outcome_id": "0193-out-1"})
        if request.url.path == "/decisions/0193-dec-1":
            return httpx.Response(200, json={"decision_id": "0193-dec-1", "action": "step_up"})
        if request.url.path == "/decisions":
            return httpx.Response(200, json={"decisions": [{"decision_id": "0193-dec-1"}]})
        if request.url.path == "/api/v1/erase":
            return httpx.Response(200, json={"entities_erased": 1})
        if request.url.path == "/features":
            return httpx.Response(
                200,
                json={
                    "entity_type": "CardCheckout",
                    "entity_id": "card_4417",
                    "features": {"card_id": "card_4417", "amount": 950.0, "action": "step_up"},
                    "mode": "featureset",
                    "decision_id": "0193-dec-1",
                    "recorded": False,
                    "judgments": {"risk": {"choice": "takeover"}},
                    "policies": {"action": {"version": 1, "rule": "2:step_up"}},
                },
            )
        raise AssertionError(f"unexpected {request.method} {request.url}")

    client = _mock_client(handler)

    # When recording a decision, recording an outcome, fetching, and erasing
    dec_id = client.record_decision(
        featureset="CardCheckout",
        entity_id="card_4417",
        state={"amount": {"value": 950.0, "description": "Amount"}},
        state_hash="h1",
        renderer_version=1,
        feature_versions={},
        questions_hash="q1",
        model_alias="xgboost:v1",
        model_version="v1",
        judgments={"score": 0.91},
        policy_version=1,
        rule_fired="1:block",
        action="block",
        latency_ms=8,
    )
    out_id = client.record_outcome(
        "chargeback",
        featureset="CardCheckout",
        entity_id="card_4417",
        value=950.0,
    )
    fetched = client.get_decision("0193-dec-1")
    listed = client.list_decisions(featureset="CardCheckout", entity_id="card_4417")
    client.erase("card_4417")

    # Then each endpoint received the expected contract payload
    assert dec_id == "0193-dec-1"
    assert out_id == "0193-out-1"
    assert fetched["action"] == "step_up"
    assert len(listed) == 1
    assert seen[0][2]["state"] == '{"amount": {"description": "Amount", "value": 950.0}}'
    assert seen[4][2] == {"entity_ids": ["card_4417"]}
