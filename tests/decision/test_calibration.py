"""Given / when / then tests for the SDK calibration report (Brier score, ECE, reliability bins)."""

from __future__ import annotations

import json

import pytest

from thyme.calibration import CalibrationReport, calibration_report


def _record(
    decision_id: str,
    entity_id: str,
    decided_at: str,
    *,
    model_version: str,
    takeover_p: float,
    card_testing_p: float = 0.2,
    segment: str = "retail",
) -> dict:
    return {
        "decision_id": decision_id,
        "featureset": "CardCheckout",
        "entity_id": entity_id,
        "decided_at": decided_at,
        "model_version": model_version,
        "state": json.dumps(
            {
                "merchant_category": {"value": segment, "description": "Merchant category"},
                "amount": {"value": 100.0, "description": "Amount"},
            }
        ),
        "judgments": json.dumps(
            {
                "risk": {
                    "type": "choice",
                    "choice": "takeover" if takeover_p > 0.5 else "ordinary",
                    "probabilities": {
                        "ordinary": round(1.0 - takeover_p, 4),
                        "takeover": takeover_p,
                    },
                },
                "card_testing": {"type": "noul", "noul": card_testing_p},
            }
        ),
        "action": "step_up" if takeover_p > 0.6 else "approve",
        "status": "ok",
    }


def test_calibration_report_matches_hand_calculated_brier_ece_and_bins():
    # Given four resolved decisions in two probability bands [0.1, 0.2) and [0.8, 0.9)
    # plus one unresolved decision with no outcome
    records = [
        _record("d1", "c1", "2026-11-01T10:00:00Z", model_version="jev-1.14.0", takeover_p=0.10, segment="retail"),
        _record("d2", "c2", "2026-11-01T10:00:00Z", model_version="jev-1.14.0", takeover_p=0.15, segment="retail"),
        _record("d3", "c3", "2026-11-01T10:00:00Z", model_version="jev-1.14.0", takeover_p=0.80, segment="travel"),
        _record("d4", "c4", "2026-11-01T10:00:00Z", model_version="d3-flash-1.0", takeover_p=0.85, segment="travel"),
        _record("d5", "c5", "2026-11-01T10:00:00Z", model_version="d3-flash-1.0", takeover_p=0.50, segment="travel"),
    ]
    outcomes = [
        {"decision_id": "d1", "outcome": "legit", "occurred_at": "2026-11-05T00:00:00Z"},
        {"decision_id": "d2", "outcome": "legit", "occurred_at": "2026-11-05T00:00:00Z"},
        {"decision_id": "d3", "outcome": "chargeback", "occurred_at": "2026-11-05T00:00:00Z"},
        {"decision_id": "d4", "outcome": "chargeback", "occurred_at": "2026-11-05T00:00:00Z"},
    ]

    # When computing the calibration report for risk.takeover with 10 equal bins
    report = calibration_report(
        records,
        outcomes,
        question="risk.takeover",
        positive_outcome="chargeback",
        bins=10,
        segment_by="merchant_category",
    )

    # Then Brier score equals ((0.10-0)^2 + (0.15-0)^2 + (0.80-1)^2 + (0.85-1)^2) / 4 = 0.02375
    expected_brier = ((0.10**2) + (0.15**2) + (0.20**2) + (0.15**2)) / 4.0
    # And ECE equals (2/4)*|0.125 - 0.0| + (2/4)*|0.825 - 1.0| = 0.0625 + 0.0875 = 0.15
    expected_ece = 0.5 * 0.125 + 0.5 * 0.175

    assert isinstance(report, CalibrationReport)
    assert report.question == "risk.takeover"
    assert report.unattached_count == 1
    assert report.overall.count == 4
    assert report.overall.positive_rate == pytest.approx(0.5)
    assert report.overall.brier_score == pytest.approx(expected_brier)
    assert report.overall.ece == pytest.approx(expected_ece)

    non_empty = [b for b in report.overall.bins if b.count > 0]
    assert len(non_empty) == 2
    assert non_empty[0].lower == pytest.approx(0.1)
    assert non_empty[0].upper == pytest.approx(0.2)
    assert non_empty[0].count == 2
    assert non_empty[0].mean_predicted == pytest.approx(0.125)
    assert non_empty[0].empirical_rate == pytest.approx(0.0)
    assert non_empty[1].lower == pytest.approx(0.8)
    assert non_empty[1].upper == pytest.approx(0.9)
    assert non_empty[1].count == 2
    assert non_empty[1].mean_predicted == pytest.approx(0.825)
    assert non_empty[1].empirical_rate == pytest.approx(1.0)

    # And breakdowns separate model versions and segments cleanly
    assert set(report.by_model_version) == {"jev-1.14.0", "d3-flash-1.0"}
    assert report.by_model_version["jev-1.14.0"].count == 3
    assert report.by_model_version["d3-flash-1.0"].brier_score == pytest.approx(0.15**2)
    assert set(report.by_segment) == {"retail", "travel"}
    assert report.by_segment["retail"].positive_rate == pytest.approx(0.0)
    assert report.by_segment["travel"].positive_rate == pytest.approx(1.0)


def test_calibration_report_attaches_delayed_outcomes_by_window_and_supports_noul():
    # Given two decisions and delayed outcomes without decision_id
    records = [
        _record("d1", "card_4417", "2026-11-01T14:32:01Z", model_version="jev-1.14.0", takeover_p=0.71, card_testing_p=0.90),
        _record("d2", "card_9999", "2026-11-01T15:00:00Z", model_version="jev-1.14.0", takeover_p=0.10, card_testing_p=0.10),
    ]
    outcomes = [
        # 9 days later -> attaches to d1
        {"featureset": "CardCheckout", "entity_id": "card_4417", "outcome": "chargeback", "occurred_at": "2026-11-10T14:32:01Z"},
        # 45 days later -> outside 30d window, does not attach to d2
        {"featureset": "CardCheckout", "entity_id": "card_9999", "outcome": "chargeback", "occurred_at": "2026-12-16T15:00:00Z"},
    ]

    # When computing calibration on noul question `card_testing` with attach_window="30d"
    report = calibration_report(
        records,
        outcomes,
        question="card_testing",
        positive_outcome="chargeback",
        attach_window="30d",
    )

    # Then only d1 is resolved (d2 is unattached) and Brier = (0.90 - 1.0)^2 = 0.01
    assert report.overall.count == 1
    assert report.unattached_count == 1
    assert report.overall.brier_score == pytest.approx(0.01)
    assert report.to_dataframe().height == 10
