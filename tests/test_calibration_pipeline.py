"""Tests for the production calibration pipeline template."""

from __future__ import annotations

import json

import pytest

from thyme.dataset import clear_registry, get_commit_payload
from thyme.decision import CalibrationTemplate, calibration_pipeline
from thyme.featureset import clear_featureset_registry


def _make_record(
    decision_id: str,
    entity_id: str,
    decided_at: str,
    takeover_prob: float,
    *,
    model_version: str = "jev-1.14.0",
    featureset: str = "CardCheckout",
) -> dict:
    ordinary = round(max(0.0, 1.0 - takeover_prob), 4)
    return {
        "decision_id": decision_id,
        "featureset": featureset,
        "entity_id": entity_id,
        "decided_at": decided_at,
        "model_version": model_version,
        "judgments": json.dumps(
            {
                "risk": {
                    "type": "choice",
                    "choice": "takeover" if takeover_prob >= 0.5 else "ordinary",
                    "probabilities": {
                        "ordinary": ordinary,
                        "takeover": takeover_prob,
                    },
                }
            }
        ),
        "action": "step_up" if takeover_prob >= 0.6 else "approve",
    }


def _make_outcome(
    outcome_id: str,
    decision_id: str,
    entity_id: str,
    outcome: str,
    occurred_at: str,
    *,
    featureset: str = "CardCheckout",
) -> dict:
    return {
        "outcome_id": outcome_id,
        "decision_id": decision_id,
        "featureset": featureset,
        "entity_id": entity_id,
        "outcome": outcome,
        "occurred_at": occurred_at,
    }


def test_calibration_pipeline_commits_cleanly_over_system_datasets():
    # Given an empty registry
    clear_registry()
    clear_featureset_registry()

    # When declaring a calibration pipeline for CardCheckout
    tpl = calibration_pipeline(
        "CardCheckout",
        question="risk.takeover",
        positive_outcome="chargeback",
        bins=10,
        window="30d",
    )
    payload = get_commit_payload()

    # Then it returns a CalibrationTemplate and emits valid dataset, pipeline,
    # and featureset specs without re-declaring reserved system datasets
    assert isinstance(tpl, CalibrationTemplate)
    assert tpl.json_path == "risk.probabilities.takeover"
    dataset_names = [d["name"] for d in payload["datasets"]]
    assert "CardCheckoutCalibration" in dataset_names
    assert "decision_records" not in dataset_names
    assert "decision_outcomes" not in dataset_names

    assert len(payload["pipelines"]) == 1
    pipe = payload["pipelines"][0]
    assert pipe["input_datasets"] == ["decision_outcomes", "decision_records"]
    assert pipe["output_dataset"] == "CardCheckoutCalibration"

    ops = pipe["operators"]
    assert any("filter" in op for op in ops)
    assert any(
        "temporal_join" in op and op["temporal_join"]["right_dataset"] == "decision_records"
        for op in ops
    )
    agg_op = next(op["aggregate"] for op in ops if "aggregate" in op)
    assert agg_op["keys"] == ["model_version"]
    assert len(agg_op["specs"]) == 20  # 10 bands * (total + positive)

    fs_names = [fs["name"] for fs in payload["featuresets"]]
    assert "CardCheckoutCalibrationFeatures" in fs_names
    cal_fs = next(fs for fs in payload["featuresets"] if fs["name"] == "CardCheckoutCalibrationFeatures")
    ext = next(e for e in cal_fs["extractors"] if e["name"] == "compute_calibration")
    assert "band_7_precision" in ext["outputs"]
    assert "ece" in ext["outputs"]
    assert "is_miscalibrated" in ext["outputs"]
    assert ext["pycode"]["source_code"].startswith("def compute_calibration(")


def test_calibration_pipeline_evaluates_payments_fixture_band_precision():
    # Given a payments fixture where 25 decisions in the 0.7-0.8 band have 16
    # chargebacks (64% empirical precision) plus 10 low-risk legitimate payments
    clear_registry()
    clear_featureset_registry()
    tpl = calibration_pipeline(
        "CardCheckout",
        question="risk.takeover",
        positive_outcome="chargeback",
        bins=10,
        window="30d",
        min_samples=20,
        max_ece=0.05,
    )

    records = []
    outcomes = []
    for i in range(25):
        did = f"dec-hi-{i}"
        cid = f"card_{4400 + i}"
        records.append(_make_record(did, cid, f"2026-11-09T14:{i:02d}:00Z", 0.75))
        outcomes.append(
            _make_outcome(
                f"out-hi-{i}",
                did,
                cid,
                "chargeback" if i < 16 else "legit",
                f"2026-11-18T14:{i:02d}:00Z",
            )
        )
    for i in range(10):
        did = f"dec-lo-{i}"
        cid = f"card_{5500 + i}"
        records.append(_make_record(did, cid, f"2026-11-09T15:{i:02d}:00Z", 0.15))
        outcomes.append(
            _make_outcome(
                f"out-lo-{i}",
                did,
                cid,
                "chargeback" if i == 0 else "legit",
                f"2026-11-18T15:{i:02d}:00Z",
            )
        )

    # When evaluating the template on the payments fixture
    features_by_model = tpl.evaluate(records, outcomes)

    # Then band 7 (0.7-0.8) has 25 outcomes, 16 positives, precision 0.64,
    # and the model is flagged as miscalibrated above max_ece=0.05
    jev = features_by_model["jev-1.14.0"]
    assert jev["band_7_total"] == 25
    assert jev["band_7_positive"] == 16
    assert jev["band_7_precision"] == pytest.approx(0.64)
    assert jev["band_1_total"] == 10
    assert jev["band_1_positive"] == 1
    assert jev["band_1_precision"] == pytest.approx(0.10)
    assert jev["total_outcomes"] == 35
    assert jev["band_0_precision"] is None
    assert jev["ece"] == pytest.approx((25 / 35) * abs(0.75 - 0.64) + (10 / 35) * abs(0.15 - 0.10))
    assert jev["is_miscalibrated"] is True


def test_calibration_pipeline_boolean_question_and_validation():
    # Given a boolean question without a dot
    clear_registry()
    clear_featureset_registry()
    tpl = calibration_pipeline(
        "CardCheckout",
        question="card_testing",
        positive_outcome="chargeback",
        bins=5,
    )
    assert tpl.json_path == "card_testing.noul"
    assert len(tpl.band_labels) == 5

    # Invalid inputs raise ValueError
    with pytest.raises(ValueError, match="bins"):
        calibration_pipeline("CardCheckout", question="risk.takeover", positive_outcome="chargeback", bins=1)
    with pytest.raises(ValueError, match="positive_outcome"):
        calibration_pipeline("CardCheckout", question="risk.takeover", positive_outcome="  ")
