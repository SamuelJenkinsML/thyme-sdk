"""Calibration reports: Brier score, ECE, and reliability curves over decision records and outcomes."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Callable

import polars as pl

from thyme.backtest import (
    _flatten_judgment_probabilities,
    _normalize_records,
    _unwrap_state_values,
)
from thyme.decision import attach_outcomes


@dataclass(frozen=True)
class ReliabilityBin:
    """One probability band of a reliability curve."""

    lower: float
    upper: float
    count: int
    mean_predicted: float | None
    empirical_rate: float | None
    gap: float | None


@dataclass(frozen=True)
class CalibrationMetrics:
    """Calibration summary for one cohort (overall, model version, or segment)."""

    group: str
    count: int
    positive_rate: float | None
    brier_score: float | None
    ece: float | None
    bins: list[ReliabilityBin]


@dataclass(frozen=True)
class CalibrationReport:
    """Calibration metrics overall and broken down by model version and segment."""

    question: str
    overall: CalibrationMetrics
    by_model_version: dict[str, CalibrationMetrics] = field(default_factory=dict)
    by_segment: dict[str, CalibrationMetrics] = field(default_factory=dict)
    unattached_count: int = 0

    def to_dataframe(self) -> pl.DataFrame:
        """Return the overall reliability bins as a Polars DataFrame."""
        return pl.DataFrame(
            [
                {
                    "lower": b.lower,
                    "upper": b.upper,
                    "count": b.count,
                    "mean_predicted": b.mean_predicted,
                    "empirical_rate": b.empirical_rate,
                    "gap": b.gap,
                }
                for b in self.overall.bins
            ]
        )


def _compute_metrics(group: str, pairs: list[tuple[float, float]], *, bins: int) -> CalibrationMetrics:
    n = len(pairs)
    width = 1.0 / bins
    bucketed: list[list[tuple[float, float]]] = [[] for _ in range(bins)]

    for p, y in pairs:
        clamped = min(1.0, max(0.0, p))
        idx = min(bins - 1, int(clamped * bins))
        bucketed[idx].append((clamped, y))

    reliability_bins: list[ReliabilityBin] = []
    ece_sum = 0.0
    for idx, items in enumerate(bucketed):
        lower = idx * width
        upper = 1.0 if idx == bins - 1 else (idx + 1) * width
        cnt = len(items)
        if cnt == 0:
            reliability_bins.append(
                ReliabilityBin(
                    lower=lower,
                    upper=upper,
                    count=0,
                    mean_predicted=None,
                    empirical_rate=None,
                    gap=None,
                )
            )
            continue
        mean_p = sum(p for p, _ in items) / cnt
        emp = sum(y for _, y in items) / cnt
        gap = abs(mean_p - emp)
        ece_sum += (cnt / n) * gap
        reliability_bins.append(
            ReliabilityBin(
                lower=lower,
                upper=upper,
                count=cnt,
                mean_predicted=mean_p,
                empirical_rate=emp,
                gap=gap,
            )
        )

    if n == 0:
        return CalibrationMetrics(
            group=group,
            count=0,
            positive_rate=None,
            brier_score=None,
            ece=None,
            bins=reliability_bins,
        )

    brier = sum((p - y) ** 2 for p, y in pairs) / n
    pos_rate = sum(y for _, y in pairs) / n
    return CalibrationMetrics(
        group=group,
        count=n,
        positive_rate=pos_rate,
        brier_score=brier,
        ece=ece_sum,
        bins=reliability_bins,
    )


def _extract_segment(rec: dict[str, Any], segment_by: str | Callable[[dict[str, Any]], Any]) -> str:
    if callable(segment_by):
        val = segment_by(rec)
    elif segment_by in rec and rec[segment_by] is not None:
        val = rec[segment_by]
    else:
        state_vals = _unwrap_state_values(rec.get("state"))
        val = state_vals.get(segment_by)
    return "unknown" if val is None else str(val)


def calibration_report(
    records: Any,
    outcomes: Any = None,
    *,
    question: str,
    positive_outcome: str,
    positive_option: str | None = None,
    bins: int = 10,
    segment_by: str | Callable[[dict[str, Any]], Any] | None = None,
    attach_window: str | timedelta | None = None,
    score_max: float | None = None,
) -> CalibrationReport:
    """Compute Brier score, ECE, and reliability bins over decisions and outcomes.

    Args:
        records: Decision records (list of dicts or Polars DataFrame).
        outcomes: Optional outcomes (list of dicts or Polars DataFrame). If
            omitted, each record is expected to already carry an ``outcome`` key.
        question: Probability key (e.g. ``"risk.takeover"``, ``"card_testing"``)
            or choice question name when paired with ``positive_option``.
        positive_outcome: Outcome label treated as ``y = 1`` (all other resolved
            outcomes count as ``y = 0``).
        positive_option: Option name for a choice question when ``question`` is
            the bare question name (e.g. ``question="risk", positive_option="takeover"``).
        bins: Number of equal-width probability bins across ``[0, 1]``.
        segment_by: Feature name (looked up on the record or inside its unwrapped
            state snapshot) or callable returning a segment label.
        attach_window: Lookback window passed to :func:`attach_outcomes` for
            outcomes without a ``decision_id`` (e.g. ``"30d"``).
        score_max: Optional denominator to normalize ``score`` judgments onto ``[0, 1]``.
    """
    if bins <= 0:
        raise ValueError("bins must be a positive integer")

    rec_list = _normalize_records(records)
    prob_key = f"{question}.{positive_option}" if positive_option else question

    # Build decision_id -> resolved binary label (1.0 positive, 0.0 negative).
    labels_by_id: dict[str, float] = {}
    if outcomes is not None:
        attached = attach_outcomes(rec_list, outcomes, window=attach_window)
        for item in attached:
            did = item.get("attached_decision_id")
            if not did:
                continue
            y = 1.0 if item.get("outcome") == positive_outcome else 0.0
            labels_by_id[str(did)] = max(labels_by_id.get(str(did), 0.0), y)
    else:
        for rec in rec_list:
            if rec.get("outcome") is not None:
                did = str(rec["decision_id"])
                labels_by_id[did] = 1.0 if rec.get("outcome") == positive_outcome else 0.0

    overall_pairs: list[tuple[float, float]] = []
    by_version_pairs: dict[str, list[tuple[float, float]]] = defaultdict(list)
    by_segment_pairs: dict[str, list[tuple[float, float]]] = defaultdict(list)
    unattached = 0

    for rec in rec_list:
        did = str(rec["decision_id"])
        if did not in labels_by_id:
            unattached += 1
            continue
        flat = _flatten_judgment_probabilities(rec.get("judgments"))
        raw_p = flat.get(prob_key)
        if raw_p is None:
            continue
        p = float(raw_p) / float(score_max) if score_max else float(raw_p)
        y = labels_by_id[did]

        overall_pairs.append((p, y))
        mv = str(rec.get("model_version") or "unknown")
        by_version_pairs[mv].append((p, y))
        if segment_by is not None:
            seg = _extract_segment(rec, segment_by)
            by_segment_pairs[seg].append((p, y))

    overall = _compute_metrics("overall", overall_pairs, bins=bins)
    by_version = {
        mv: _compute_metrics(mv, pairs, bins=bins)
        for mv, pairs in sorted(by_version_pairs.items())
    }
    by_segment = {
        seg: _compute_metrics(seg, pairs, bins=bins)
        for seg, pairs in sorted(by_segment_pairs.items())
    }

    return CalibrationReport(
        question=prob_key,
        overall=overall,
        by_model_version=by_version,
        by_segment=by_segment,
        unattached_count=unattached,
    )
