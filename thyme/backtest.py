"""Backtests over past decisions: policy replay, re-ask, rebuild, and multi-arm comparison.

Policy replay, re-ask, and rebuild execute through ``POST /decisions/backtest``
on the query server so canonical state rendering, model calls, and policy
evaluation share one Rust implementation with live reads.
"""

from __future__ import annotations

import json
import math
import os
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Literal, Protocol, Sequence, runtime_checkable

import polars as pl

from thyme.decision import PolicyDescriptor, Provider, attach_outcomes
from thyme.decision_offline import read_decision_records
from thyme.expr._wire import proto_to_wire
from thyme.offline_catalog import CatalogConfig

ChangeKind = Literal[
    "unchanged",
    "policy_change",
    "within_noise",
    "model_change",
    "late_data",
    "error",
]

# Default 3-sample 95% noise band measured in Spike 0.4 (`systemone-spread.md`).
DEFAULT_NOISE_BAND = 0.038
LATE_DATA_EVENT_TIME_WARNING = (
    "Rebuild used event time; late arrivals can differ from live state until "
    "knowledge time (as_of='known') is enabled."
)


@dataclass(frozen=True)
class BacktestRow:
    """One decision evaluated by a backtest."""

    decision_id: str
    entity_id: str
    decided_at: str
    original_action: str | None
    candidate_action: str | None
    original_rule: str | None = None
    candidate_rule: str | None = None
    change_kind: ChangeKind = "unchanged"
    status: str = "ok"
    late_data: bool = False
    state_hash: str | None = None
    judgments: dict[str, Any] | None = None
    spread: dict[str, Any] | None = None
    sample_actions: tuple[str, ...] = ()
    latency_ms: int = 0
    cost_usd: float | None = None
    error: str | None = None
    drifted_features: tuple[str, ...] = ()


@dataclass(frozen=True)
class BacktestReport:
    """Aggregated diff report produced by a policy replay, re-ask, or rebuild."""

    mode: str
    featureset: str
    rows: list[BacktestRow]
    policy_version: int | None = None
    samples: int = 0
    warnings: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.rows)

    @property
    def changed(self) -> int:
        return sum(1 for r in self.rows if r.original_action != r.candidate_action)

    @property
    def changed_rate(self) -> float:
        return self.changed / self.total if self.total else 0.0

    @property
    def counts_by_change_kind(self) -> dict[str, int]:
        return dict(Counter(r.change_kind for r in self.rows))

    @property
    def action_matrix(self) -> dict[tuple[str | None, str | None], int]:
        return dict(Counter((r.original_action, r.candidate_action) for r in self.rows))

    def to_dataframe(self) -> pl.DataFrame:
        if not self.rows:
            return pl.DataFrame(
                schema={
                    "decision_id": pl.String,
                    "entity_id": pl.String,
                    "decided_at": pl.String,
                    "original_action": pl.String,
                    "candidate_action": pl.String,
                    "original_rule": pl.String,
                    "candidate_rule": pl.String,
                    "change_kind": pl.String,
                    "status": pl.String,
                    "late_data": pl.Boolean,
                    "state_hash": pl.String,
                    "latency_ms": pl.Int64,
                    "cost_usd": pl.Float64,
                }
            )
        return pl.DataFrame(
            [
                {
                    "decision_id": r.decision_id,
                    "entity_id": r.entity_id,
                    "decided_at": r.decided_at,
                    "original_action": r.original_action,
                    "candidate_action": r.candidate_action,
                    "original_rule": r.original_rule,
                    "candidate_rule": r.candidate_rule,
                    "change_kind": r.change_kind,
                    "status": r.status,
                    "late_data": r.late_data,
                    "state_hash": r.state_hash,
                    "latency_ms": r.latency_ms,
                    "cost_usd": r.cost_usd,
                }
                for r in self.rows
            ]
        )


# ---------------------------------------------------------------------------
# Python deciders & multi-arm comparison
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DeciderResult:
    """Output of a Python decider (classical ML model, LLM, or rule function)."""

    action: str
    probabilities: dict[str, float] = field(default_factory=dict)
    rule_fired: str | None = None
    latency_ms: float = 0.0
    cost_usd: float | None = None
    judgments: dict[str, Any] | None = None


@runtime_checkable
class Decider(Protocol):
    """Protocol implemented by Python deciders (sklearn, LightGBM, LLM, custom)."""

    name: str

    def decide(self, state: dict[str, Any]) -> DeciderResult: ...


class CallableDecider:
    """Wrap any Python callable ``(state: dict) -> DeciderResult | dict | str``."""

    def __init__(self, name: str, fn: Callable[[dict[str, Any]], Any]):
        self.name = name
        self._fn = fn

    def decide(self, state: dict[str, Any]) -> DeciderResult:
        started = time.perf_counter()
        raw = self._fn(state)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        if isinstance(raw, DeciderResult):
            if raw.latency_ms == 0.0:
                return DeciderResult(
                    action=raw.action,
                    probabilities=dict(raw.probabilities),
                    rule_fired=raw.rule_fired,
                    latency_ms=elapsed_ms,
                    cost_usd=raw.cost_usd,
                    judgments=raw.judgments,
                )
            return raw
        if isinstance(raw, str):
            return DeciderResult(action=raw, latency_ms=elapsed_ms)
        if isinstance(raw, dict):
            return DeciderResult(
                action=str(raw["action"]),
                probabilities=dict(raw.get("probabilities", {})),
                rule_fired=raw.get("rule_fired"),
                latency_ms=float(raw.get("latency_ms", elapsed_ms)),
                cost_usd=raw.get("cost_usd"),
                judgments=raw.get("judgments"),
            )
        raise TypeError(f"CallableDecider '{self.name}' returned unsupported {type(raw).__name__}")


def _to_float_or_nan(value: Any) -> float:
    if value is None:
        return float("nan")
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


class SklearnDecider:
    """Wrap a scikit-learn estimator (``predict_proba`` or ``predict``) as a Decider."""

    def __init__(
        self,
        name: str,
        estimator: Any,
        features: Sequence[str],
        *,
        positive_class: int = 1,
        threshold: float = 0.5,
        positive_action: str = "block",
        negative_action: str = "approve",
        probability_key: str = "score",
    ):
        self.name = name
        self.estimator = estimator
        self.features = tuple(features)
        self.positive_class = positive_class
        self.threshold = threshold
        self.positive_action = positive_action
        self.negative_action = negative_action
        self.probability_key = probability_key

    def decide(self, state: dict[str, Any]) -> DeciderResult:
        started = time.perf_counter()
        row = [[_to_float_or_nan(state.get(col)) for col in self.features]]
        if hasattr(self.estimator, "predict_proba"):
            proba = self.estimator.predict_proba(row)[0]
            p = float(proba[self.positive_class])
        else:
            p = float(self.estimator.predict(row)[0])
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        positive = p >= self.threshold
        action = self.positive_action if positive else self.negative_action
        rule = f"1:{self.positive_action}" if positive else f"2:{self.negative_action}"
        return DeciderResult(
            action=action,
            probabilities={self.probability_key: p},
            rule_fired=rule,
            latency_ms=elapsed_ms,
        )


class LightGBMDecider:
    """Wrap a LightGBM ``Booster`` as a Decider."""

    def __init__(
        self,
        name: str,
        booster: Any,
        features: Sequence[str],
        *,
        threshold: float = 0.5,
        positive_action: str = "block",
        negative_action: str = "approve",
        probability_key: str = "score",
    ):
        self.name = name
        self.booster = booster
        self.features = tuple(features)
        self.threshold = threshold
        self.positive_action = positive_action
        self.negative_action = negative_action
        self.probability_key = probability_key

    def decide(self, state: dict[str, Any]) -> DeciderResult:
        started = time.perf_counter()
        row = [[_to_float_or_nan(state.get(col)) for col in self.features]]
        p = float(self.booster.predict(row)[0])
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        positive = p >= self.threshold
        action = self.positive_action if positive else self.negative_action
        rule = f"1:{self.positive_action}" if positive else f"2:{self.negative_action}"
        return DeciderResult(
            action=action,
            probabilities={self.probability_key: p},
            rule_fired=rule,
            latency_ms=elapsed_ms,
        )


@dataclass(frozen=True)
class SystemOneArm:
    """Configuration for one System One model/policy arm inside ``compare_arms``."""

    name: str
    provider: Provider | str | None = None
    model: str | None = None
    policy: PolicyDescriptor | dict[str, Any] | None = None
    samples: int | None = None
    mode: Literal["policy_replay", "reask", "rebuild"] = "reask"
    noise_band: float = DEFAULT_NOISE_BAND
    deadline_ms: int | None = None


@dataclass(frozen=True)
class ArmSummary:
    """Aggregate metrics for one arm in a multi-arm comparison."""

    arm: str
    decisions: int
    agreement_vs_live: float
    model_changes: int
    within_noise: int
    late_data_count: int
    accuracy: float | None
    brier_score: float | None
    total_cost_usd: float | None
    latency_p50_ms: float | None
    latency_p95_ms: float | None


@dataclass(frozen=True)
class ComparisonReport:
    """Side-by-side comparison across System One models, classical ML, and LLM arms."""

    featureset: str
    summaries: list[ArmSummary]
    per_arm_rows: dict[str, list[BacktestRow]]
    warnings: list[str] = field(default_factory=list)

    def to_dataframe(self) -> pl.DataFrame:
        return pl.DataFrame(
            [
                {
                    "arm": s.arm,
                    "decisions": s.decisions,
                    "agreement_vs_live": s.agreement_vs_live,
                    "model_changes": s.model_changes,
                    "within_noise": s.within_noise,
                    "late_data_count": s.late_data_count,
                    "accuracy": s.accuracy,
                    "brier_score": s.brier_score,
                    "total_cost_usd": s.total_cost_usd,
                    "latency_p50_ms": s.latency_p50_ms,
                    "latency_p95_ms": s.latency_p95_ms,
                }
                for s in self.summaries
            ]
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _featureset_name(featureset: str | type) -> str:
    return featureset.__name__ if isinstance(featureset, type) else str(featureset)


def _normalize_records(records: Any) -> list[dict[str, Any]]:
    if isinstance(records, pl.DataFrame):
        return records.to_dicts()
    if hasattr(records, "to_dicts"):
        return records.to_dicts()
    return [dict(r) for r in records]


def _load_records(
    client: Any,
    featureset: str | type,
    *,
    records: Any = None,
    start: datetime | str | None = None,
    end: datetime | str | None = None,
    entity_ids: Sequence[str] | None = None,
    catalog: CatalogConfig | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    if records is not None:
        return _normalize_records(records)
    has_catalog = catalog is not None or bool(os.environ.get("THYME_ICEBERG_CATALOG"))
    if has_catalog:
        df = read_decision_records(
            featureset,
            start=start,
            end=end,
            entity_ids=entity_ids,
            catalog=catalog,
        )
        return df.to_dicts()
    rows = client.list_decisions(
        featureset=featureset,
        start=start,
        end=end,
        limit=limit,
    )
    if entity_ids is not None:
        wanted = set(entity_ids)
        rows = [r for r in rows if r.get("entity_id") in wanted]
    return rows


def _serialize_policy(
    candidate: PolicyDescriptor | dict[str, Any] | None,
    *,
    output: str = "action",
) -> dict[str, Any] | None:
    if candidate is None:
        return None
    if isinstance(candidate, PolicyDescriptor):
        spec = candidate.policy_spec(output)
    elif isinstance(candidate, dict):
        spec = dict(candidate)
    else:
        raise TypeError(
            f"policy must be a policy(...) descriptor or dict, got {type(candidate).__name__}"
        )
    wire_rules = []
    for rule in spec.get("rules", []):
        entry = dict(rule)
        when = entry.get("when")
        if when is not None and not isinstance(when, dict):
            entry["when"] = proto_to_wire(when)
        wire_rules.append(entry)
    return {**spec, "rules": wire_rules}


def _parse_json_field(raw: Any) -> Any:
    if isinstance(raw, str):
        return json.loads(raw)
    return raw


def _unwrap_state_values(raw_state: Any) -> dict[str, Any]:
    """Unwrap canonical ``{feat: {"value": v, "description": d}}`` to ``{feat: v}``."""
    parsed = _parse_json_field(raw_state) if raw_state else {}
    if not isinstance(parsed, dict):
        return {}
    out: dict[str, Any] = {}
    for k, v in parsed.items():
        if isinstance(v, dict) and "value" in v and "description" in v:
            out[k] = v["value"]
        else:
            out[k] = v
    return out


def _flatten_judgment_probabilities(judgments: Any) -> dict[str, float]:
    """Extract comparable numeric probability keys (``risk.takeover``, ``card_testing``, ...)."""
    parsed = _parse_json_field(judgments) if judgments else {}
    if not isinstance(parsed, dict):
        return {}
    out: dict[str, float] = {}
    for name, val in parsed.items():
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            out[name] = float(val)
            continue
        if not isinstance(val, dict):
            continue
        kind = val.get("type")
        if kind == "choice":
            probs = val.get("probabilities") or {}
            if isinstance(probs, dict):
                for opt, p in probs.items():
                    if isinstance(p, (int, float)):
                        out[f"{name}.{opt}"] = float(p)
        elif kind == "noul" and isinstance(val.get("noul"), (int, float)):
            out[name] = float(val["noul"])
        elif kind == "score" and isinstance(val.get("score"), (int, float)):
            out[name] = float(val["score"])
    return out


def _max_observed_sd(spread: Any) -> tuple[float, int]:
    """Return ``(max_sd, sample_count)`` from a backtest result's spread map."""
    if not isinstance(spread, dict):
        return 0.0, 1
    max_sd = 0.0
    n_samples = 1
    for entry in spread.values():
        if not isinstance(entry, dict):
            continue
        if isinstance(entry.get("samples"), int):
            n_samples = max(n_samples, entry["samples"])
        for key in ("noul_sd", "score_sd"):
            if isinstance(entry.get(key), (int, float)):
                max_sd = max(max_sd, float(entry[key]))
        probs_sd = entry.get("probabilities_sd")
        if isinstance(probs_sd, dict):
            for v in probs_sd.values():
                if isinstance(v, (int, float)):
                    max_sd = max(max_sd, float(v))
    return max_sd, n_samples


def _classify_model_change(
    original_action: str | None,
    candidate_action: str | None,
    original_judgments: Any,
    candidate_judgments: Any,
    spread: Any,
    *,
    noise_band: float,
    late_data: bool = False,
    status: str = "ok",
) -> ChangeKind:
    if status != "ok":
        return "error"
    if original_action == candidate_action:
        return "unchanged"
    if late_data:
        return "late_data"

    orig_probs = _flatten_judgment_probabilities(original_judgments)
    cand_probs = _flatten_judgment_probabilities(candidate_judgments)
    shared = set(orig_probs) & set(cand_probs)
    if not shared:
        return "model_change"

    max_delta = max(abs(cand_probs[k] - orig_probs[k]) for k in shared)
    obs_sd, n = _max_observed_sd(spread)
    dynamic_band = 1.96 * obs_sd * math.sqrt(2.0 / n) if n > 1 and obs_sd > 0 else 0.0
    effective_band = max(noise_band, dynamic_band)
    return "within_noise" if max_delta <= effective_band else "model_change"


def _values_differ(a: Any, b: Any, *, rel_tol: float = 1e-9, abs_tol: float = 1e-9) -> bool:
    if a is None and b is None:
        return False
    if a is None or b is None:
        return True
    if isinstance(a, (int, float)) and isinstance(b, (int, float)) and not isinstance(a, bool) and not isinstance(b, bool):
        if math.isnan(a) and math.isnan(b):
            return False
        return not math.isclose(float(a), float(b), rel_tol=rel_tol, abs_tol=abs_tol)
    return a != b


def _chunks(items: list[Any], size: int):
    if size <= 0:
        raise ValueError("chunk_size must be positive")
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def _load_checkpoint(path: Path | None, arm_key: str) -> dict[str, dict[str, Any]]:
    if path is None or not path.exists():
        return {}
    cached: dict[str, dict[str, Any]] = {}
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            entry = json.loads(line)
            if entry.get("arm") == arm_key and "id" in entry:
                cached[str(entry["id"])] = entry["result"]
    return cached


def _append_checkpoint(path: Path | None, arm_key: str, results: Sequence[dict[str, Any]]) -> None:
    if path is None or not results:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for res in results:
            f.write(json.dumps({"arm": arm_key, "id": res["id"], "result": res}) + "\n")


def _resolve_provider_and_samples(
    provider: Provider | str | None,
    samples: int | None,
) -> tuple[str | None, int]:
    provider_name = provider.name if isinstance(provider, Provider) else provider
    if samples is not None:
        return provider_name, samples
    if isinstance(provider, Provider) and provider.samples is not None:
        return provider_name, provider.samples
    return provider_name, 3


def _entity_column_for_featureset(featureset: str | type, explicit: str | None) -> str:
    if explicit:
        return explicit
    meta = getattr(featureset, "_featureset_meta", None)
    if meta:
        for feat in meta.get("features", []):
            if not feat.get("request") and feat.get("dtype") == "str":
                return str(feat["name"])
    return "entity_id"


def _percentile(values: Sequence[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(math.ceil(q * len(ordered))) - 1))
    return float(ordered[idx])


# ---------------------------------------------------------------------------
# Public backtest entry points
# ---------------------------------------------------------------------------


def backtest_policy(
    client: Any,
    featureset: str | type,
    *,
    policy: PolicyDescriptor | dict[str, Any] | None = None,
    records: Any = None,
    start: datetime | str | None = None,
    end: datetime | str | None = None,
    entity_ids: Sequence[str] | None = None,
    catalog: CatalogConfig | None = None,
    judgment_columns: dict[str, str] | None = None,
    policy_output: str = "action",
    chunk_size: int = 100,
) -> BacktestReport:
    """Replay a policy over stored decision records in Rust with zero model calls."""
    fs_name = _featureset_name(featureset)
    loaded = _load_records(
        client,
        featureset,
        records=records,
        start=start,
        end=end,
        entity_ids=entity_ids,
        catalog=catalog,
    )
    policy_spec = _serialize_policy(policy, output=policy_output)

    rows: list[BacktestRow] = []
    reported_policy_version: int | None = (
        policy_spec.get("version") if isinstance(policy_spec, dict) else None
    )

    for batch in _chunks(loaded, chunk_size):
        payload: dict[str, Any] = {
            "mode": "policy_replay",
            "featureset": fs_name,
            "items": [],
        }
        if policy_spec is not None:
            payload["policy"] = policy_spec
        if judgment_columns is not None:
            payload["judgment_columns"] = dict(judgment_columns)

        by_id: dict[str, dict[str, Any]] = {}
        for rec in batch:
            did = str(rec["decision_id"])
            by_id[did] = rec
            item: dict[str, Any] = {
                "id": did,
                "judgments": _parse_json_field(rec.get("judgments") or "{}"),
            }
            if rec.get("state"):
                item["raw_state"] = (
                    rec["state"] if isinstance(rec["state"], str) else json.dumps(rec["state"])
                )
            payload["items"].append(item)

        response = client._http.post("/decisions/backtest", json=payload)
        response.raise_for_status()
        body = response.json()
        if reported_policy_version is None:
            reported_policy_version = body.get("policy_version")

        for res in body.get("results", []):
            rec = by_id[res["id"]]
            orig_action = rec.get("action")
            cand_action = res.get("action")
            status = res.get("status", "ok")
            if status != "ok":
                kind: ChangeKind = "error"
            elif orig_action == cand_action:
                kind = "unchanged"
            else:
                kind = "policy_change"
            rows.append(
                BacktestRow(
                    decision_id=res["id"],
                    entity_id=str(rec.get("entity_id", "")),
                    decided_at=str(rec.get("decided_at", "")),
                    original_action=orig_action,
                    candidate_action=cand_action,
                    original_rule=rec.get("rule_fired"),
                    candidate_rule=res.get("rule_fired"),
                    change_kind=kind,
                    status=status,
                    judgments=res.get("judgments"),
                    latency_ms=int(res.get("latency_ms", 0)),
                    error=res.get("error"),
                )
            )

    return BacktestReport(
        mode="policy_replay",
        featureset=fs_name,
        rows=rows,
        policy_version=reported_policy_version,
        samples=0,
    )


def backtest_reask(
    client: Any,
    featureset: str | type,
    *,
    provider: Provider | str | None = None,
    model: str | None = None,
    policy: PolicyDescriptor | dict[str, Any] | None = None,
    samples: int | None = None,
    records: Any = None,
    start: datetime | str | None = None,
    end: datetime | str | None = None,
    entity_ids: Sequence[str] | None = None,
    catalog: CatalogConfig | None = None,
    noise_band: float = DEFAULT_NOISE_BAND,
    policy_output: str = "action",
    chunk_size: int = 50,
    checkpoint_path: str | Path | None = None,
    arm_name: str | None = None,
    deadline_ms: int | None = None,
) -> BacktestReport:
    """Re-ask stored canonical decision states against a candidate model and/or policy."""
    fs_name = _featureset_name(featureset)
    loaded = _load_records(
        client,
        featureset,
        records=records,
        start=start,
        end=end,
        entity_ids=entity_ids,
        catalog=catalog,
    )
    provider_name, resolved_samples = _resolve_provider_and_samples(provider, samples)
    policy_spec = _serialize_policy(policy, output=policy_output)

    ckpt = Path(checkpoint_path) if checkpoint_path is not None else None
    arm_key = arm_name or f"reask:{provider_name or 'default'}:{model or 'default'}:s{resolved_samples}"
    cached = _load_checkpoint(ckpt, arm_key)

    pending = [rec for rec in loaded if str(rec["decision_id"]) not in cached]
    reported_policy_version: int | None = (
        policy_spec.get("version") if isinstance(policy_spec, dict) else None
    )

    for batch in _chunks(pending, chunk_size):
        payload: dict[str, Any] = {
            "mode": "reask",
            "featureset": fs_name,
            "samples": resolved_samples,
            "items": [],
        }
        if provider_name is not None:
            payload["provider"] = provider_name
        if model is not None:
            payload["model"] = model
        if policy_spec is not None:
            payload["policy"] = policy_spec
        if deadline_ms is not None:
            payload["deadline_ms"] = int(deadline_ms)

        for rec in batch:
            did = str(rec["decision_id"])
            item: dict[str, Any] = {"id": did}
            if rec.get("state"):
                item["raw_state"] = (
                    rec["state"] if isinstance(rec["state"], str) else json.dumps(rec["state"])
                )
            else:
                item["record_id"] = did
            payload["items"].append(item)

        response = client._http.post("/decisions/backtest", json=payload)
        response.raise_for_status()
        body = response.json()
        if reported_policy_version is None:
            reported_policy_version = body.get("policy_version")
        batch_results = body.get("results", [])
        _append_checkpoint(ckpt, arm_key, batch_results)
        for res in batch_results:
            cached[str(res["id"])] = res

    rows: list[BacktestRow] = []
    for rec in loaded:
        did = str(rec["decision_id"])
        res = cached[did]
        orig_action = rec.get("action")
        cand_action = res.get("action")
        status = res.get("status", "ok")
        kind = _classify_model_change(
            orig_action,
            cand_action,
            rec.get("judgments"),
            res.get("judgments"),
            res.get("spread"),
            noise_band=noise_band,
            late_data=False,
            status=status,
        )
        rows.append(
            BacktestRow(
                decision_id=did,
                entity_id=str(rec.get("entity_id", "")),
                decided_at=str(rec.get("decided_at", "")),
                original_action=orig_action,
                candidate_action=cand_action,
                original_rule=rec.get("rule_fired"),
                candidate_rule=res.get("rule_fired"),
                change_kind=kind,
                status=status,
                state_hash=res.get("state_hash"),
                judgments=res.get("judgments"),
                spread=res.get("spread"),
                sample_actions=tuple(res.get("sample_actions") or ()),
                latency_ms=int(res.get("latency_ms", 0)),
                cost_usd=res.get("cost_usd"),
                error=res.get("error"),
            )
        )

    return BacktestReport(
        mode="reask",
        featureset=fs_name,
        rows=rows,
        policy_version=reported_policy_version,
        samples=resolved_samples,
    )


def _rebuild_states_for_records(
    client: Any,
    featureset: type,
    records: list[dict[str, Any]],
    *,
    entity_column: str | None = None,
    catalog: CatalogConfig | None = None,
    as_of: Literal["event", "known"] = "known",
) -> tuple[dict[str, dict[str, Any]], dict[str, tuple[str, ...]]]:
    """Pull offline state at each decision's timestamp and detect late-event drift."""
    if not records:
        return {}, {}
    ent_col = _entity_column_for_featureset(featureset, entity_column)
    spine = [
        {ent_col: str(r["entity_id"]), "decided_at": str(r["decided_at"])}
        for r in records
    ]
    offline = client.query_offline(
        featureset,
        spine,
        entity_column=ent_col,
        timestamp_column="decided_at",
        catalog=catalog,
        as_of=as_of,
    )
    pulled_rows = (
        offline.to_polars().to_dicts()
        if hasattr(offline, "to_polars")
        else offline.to_dicts()
    )
    pulled_by_key: dict[tuple[str, str], dict[str, Any]] = {
        (str(row[ent_col]), str(row["decided_at"])): row for row in pulled_rows
    }

    rebuilt_by_id: dict[str, dict[str, Any]] = {}
    drifted_by_id: dict[str, tuple[str, ...]] = {}
    for rec in records:
        did = str(rec["decision_id"])
        stored_vals = _unwrap_state_values(rec.get("state"))
        pulled = pulled_by_key.get((str(rec["entity_id"]), str(rec["decided_at"])), {})

        # Overlay rebuilt materialized features while preserving request-time
        # inputs captured in the original record snapshot.
        merged = dict(stored_vals)
        drifted: list[str] = []
        for col_name, val in pulled.items():
            if col_name in (ent_col, "decided_at"):
                continue
            if col_name in stored_vals and _values_differ(stored_vals[col_name], val):
                drifted.append(col_name)
            merged[col_name] = val

        rebuilt_by_id[did] = merged
        drifted_by_id[did] = tuple(sorted(drifted))
    return rebuilt_by_id, drifted_by_id


def backtest_rebuild(
    client: Any,
    featureset: type,
    *,
    provider: Provider | str | None = None,
    model: str | None = None,
    policy: PolicyDescriptor | dict[str, Any] | None = None,
    samples: int | None = None,
    records: Any = None,
    start: datetime | str | None = None,
    end: datetime | str | None = None,
    entity_ids: Sequence[str] | None = None,
    entity_column: str | None = None,
    catalog: CatalogConfig | None = None,
    as_of: Literal["event", "known"] = "known",
    state_features: Sequence[dict[str, str]] | None = None,
    noise_band: float = DEFAULT_NOISE_BAND,
    policy_output: str = "action",
    chunk_size: int = 50,
    checkpoint_path: str | Path | None = None,
    arm_name: str | None = None,
    deadline_ms: int | None = None,
) -> BacktestReport:
    """Rebuild historical features from the offline store at each decision time and evaluate."""
    if as_of not in ("event", "known"):
        raise ValueError(f"invalid as_of {as_of!r}: expected 'event' or 'known'")
    fs_name = _featureset_name(featureset)
    loaded = _load_records(
        client,
        featureset,
        records=records,
        start=start,
        end=end,
        entity_ids=entity_ids,
        catalog=catalog,
    )
    provider_name, resolved_samples = _resolve_provider_and_samples(provider, samples)
    policy_spec = _serialize_policy(policy, output=policy_output)

    rebuilt_states, drifted_by_id = _rebuild_states_for_records(
        client,
        featureset,
        loaded,
        entity_column=entity_column,
        catalog=catalog,
        as_of=as_of,
    )

    ckpt = Path(checkpoint_path) if checkpoint_path is not None else None
    arm_key = arm_name or f"rebuild:{provider_name or 'default'}:{model or 'default'}:s{resolved_samples}"
    cached = _load_checkpoint(ckpt, arm_key)

    pending = [rec for rec in loaded if str(rec["decision_id"]) not in cached]
    reported_policy_version: int | None = (
        policy_spec.get("version") if isinstance(policy_spec, dict) else None
    )

    for batch in _chunks(pending, chunk_size):
        payload: dict[str, Any] = {
            "mode": "rebuild",
            "featureset": fs_name,
            "samples": resolved_samples,
            "items": [
                {
                    "id": str(rec["decision_id"]),
                    "state": rebuilt_states[str(rec["decision_id"])],
                }
                for rec in batch
            ],
        }
        if provider_name is not None:
            payload["provider"] = provider_name
        if model is not None:
            payload["model"] = model
        if policy_spec is not None:
            payload["policy"] = policy_spec
        if state_features is not None:
            payload["state_features"] = list(state_features)
        if deadline_ms is not None:
            payload["deadline_ms"] = int(deadline_ms)

        response = client._http.post("/decisions/backtest", json=payload)
        response.raise_for_status()
        body = response.json()
        if reported_policy_version is None:
            reported_policy_version = body.get("policy_version")
        batch_results = body.get("results", [])
        _append_checkpoint(ckpt, arm_key, batch_results)
        for res in batch_results:
            cached[str(res["id"])] = res

    rows: list[BacktestRow] = []
    for rec in loaded:
        did = str(rec["decision_id"])
        res = cached[did]
        orig_action = rec.get("action")
        cand_action = res.get("action")
        status = res.get("status", "ok")
        drifted = drifted_by_id.get(did, ())
        is_late = bool(drifted)
        kind = _classify_model_change(
            orig_action,
            cand_action,
            rec.get("judgments"),
            res.get("judgments"),
            res.get("spread"),
            noise_band=noise_band,
            late_data=is_late,
            status=status,
        )
        rows.append(
            BacktestRow(
                decision_id=did,
                entity_id=str(rec.get("entity_id", "")),
                decided_at=str(rec.get("decided_at", "")),
                original_action=orig_action,
                candidate_action=cand_action,
                original_rule=rec.get("rule_fired"),
                candidate_rule=res.get("rule_fired"),
                change_kind=kind,
                status=status,
                late_data=is_late,
                state_hash=res.get("state_hash"),
                judgments=res.get("judgments"),
                spread=res.get("spread"),
                sample_actions=tuple(res.get("sample_actions") or ()),
                latency_ms=int(res.get("latency_ms", 0)),
                cost_usd=res.get("cost_usd"),
                error=res.get("error"),
                drifted_features=drifted,
            )
        )

    warnings = [LATE_DATA_EVENT_TIME_WARNING] if as_of == "event" else []
    return BacktestReport(
        mode="rebuild",
        featureset=fs_name,
        rows=rows,
        policy_version=reported_policy_version,
        samples=resolved_samples,
        warnings=warnings,
    )


def _evaluate_python_decider(
    decider: Decider,
    records: list[dict[str, Any]],
    states_by_id: dict[str, dict[str, Any]],
    drifted_by_id: dict[str, tuple[str, ...]],
) -> list[BacktestRow]:
    rows: list[BacktestRow] = []
    for rec in records:
        did = str(rec["decision_id"])
        state = states_by_id[did]
        drifted = drifted_by_id.get(did, ())
        is_late = bool(drifted)
        res = decider.decide(state)
        orig_action = rec.get("action")
        if orig_action == res.action:
            kind: ChangeKind = "unchanged"
        elif is_late:
            kind = "late_data"
        else:
            kind = "model_change"
        judgments = res.judgments if res.judgments is not None else dict(res.probabilities)
        rows.append(
            BacktestRow(
                decision_id=did,
                entity_id=str(rec.get("entity_id", "")),
                decided_at=str(rec.get("decided_at", "")),
                original_action=orig_action,
                candidate_action=res.action,
                original_rule=rec.get("rule_fired"),
                candidate_rule=res.rule_fired,
                change_kind=kind,
                status="ok",
                late_data=is_late,
                judgments=judgments,
                latency_ms=int(round(res.latency_ms)),
                cost_usd=res.cost_usd,
                drifted_features=drifted,
            )
        )
    return rows


def _extract_probability(row: BacktestRow, probability_key: str | None) -> float | None:
    if not probability_key or not row.judgments:
        return None
    flat = _flatten_judgment_probabilities(row.judgments)
    if probability_key in flat:
        return flat[probability_key]
    raw = row.judgments.get(probability_key)
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return float(raw)
    return None


def compare_arms(
    client: Any,
    featureset: str | type,
    arms: Sequence[SystemOneArm | Decider],
    *,
    records: Any = None,
    outcomes: Any = None,
    positive_outcome: str | None = None,
    positive_actions: Sequence[str] | None = None,
    probability_key: str | None = None,
    attach_window: str | timedelta | None = None,
    rebuild: bool = False,
    as_of: Literal["event", "known"] = "known",
    entity_column: str | None = None,
    start: datetime | str | None = None,
    end: datetime | str | None = None,
    entity_ids: Sequence[str] | None = None,
    catalog: CatalogConfig | None = None,
    checkpoint_dir: str | Path | None = None,
) -> ComparisonReport:
    """Compare System One models, classical ML models, and LLMs on the exact same states."""
    fs_name = _featureset_name(featureset)
    loaded = _load_records(
        client,
        featureset,
        records=records,
        start=start,
        end=end,
        entity_ids=entity_ids,
        catalog=catalog,
    )

    warnings: list[str] = []
    if rebuild:
        if not isinstance(featureset, type):
            raise TypeError("rebuild=True requires a @featureset class, not a string name")
        states_by_id, drifted_by_id = _rebuild_states_for_records(
            client,
            featureset,
            loaded,
            entity_column=entity_column,
            catalog=catalog,
            as_of=as_of,
        )
        if as_of == "event":
            warnings.append(LATE_DATA_EVENT_TIME_WARNING)
    else:
        states_by_id = {
            str(rec["decision_id"]): _unwrap_state_values(rec.get("state"))
            for rec in loaded
        }
        drifted_by_id = {}

    # Map decision_id -> binary outcome label (1.0 if positive_outcome else 0.0) when supplied.
    labels_by_id: dict[str, float] = {}
    if outcomes is not None and positive_outcome is not None:
        attached = attach_outcomes(loaded, outcomes, window=attach_window, featureset=fs_name)
        for item in attached:
            did = item.get("attached_decision_id")
            if not did:
                continue
            is_pos = 1.0 if item.get("outcome") == positive_outcome else 0.0
            # Positive outcome takes precedence if multiple outcomes attached to one decision.
            labels_by_id[did] = max(labels_by_id.get(did, 0.0), is_pos)

    pos_action_set = set(positive_actions) if positive_actions is not None else {"block", "step_up"}
    ckpt_root = Path(checkpoint_dir) if checkpoint_dir is not None else None

    per_arm_rows: dict[str, list[BacktestRow]] = {}
    summaries: list[ArmSummary] = []

    for arm in arms:
        if isinstance(arm, SystemOneArm):
            ckpt = ckpt_root / f"{arm.name}.jsonl" if ckpt_root is not None else None
            if arm.mode == "policy_replay":
                rep = backtest_policy(client, featureset, policy=arm.policy, records=loaded)
            elif rebuild or arm.mode == "rebuild":
                if not isinstance(featureset, type):
                    raise TypeError("rebuild mode requires a @featureset class")
                rep = backtest_rebuild(
                    client,
                    featureset,
                    provider=arm.provider,
                    model=arm.model,
                    policy=arm.policy,
                    samples=arm.samples,
                    records=loaded,
                    entity_column=entity_column,
                    catalog=catalog,
                    as_of=as_of,
                    noise_band=arm.noise_band,
                    checkpoint_path=ckpt,
                    arm_name=arm.name,
                    deadline_ms=arm.deadline_ms,
                )
            else:
                rep = backtest_reask(
                    client,
                    featureset,
                    provider=arm.provider,
                    model=arm.model,
                    policy=arm.policy,
                    samples=arm.samples,
                    records=loaded,
                    noise_band=arm.noise_band,
                    checkpoint_path=ckpt,
                    arm_name=arm.name,
                    deadline_ms=arm.deadline_ms,
                )
            arm_rows = rep.rows
        else:
            arm_rows = _evaluate_python_decider(arm, loaded, states_by_id, drifted_by_id)

        per_arm_rows[arm.name] = arm_rows
        total = len(arm_rows)
        agreed = sum(1 for r in arm_rows if r.original_action == r.candidate_action)
        model_changes = sum(1 for r in arm_rows if r.change_kind == "model_change")
        within_noise = sum(1 for r in arm_rows if r.change_kind == "within_noise")
        late_count = sum(1 for r in arm_rows if r.late_data)

        latencies = [float(r.latency_ms) for r in arm_rows if r.status == "ok"]
        costs = [r.cost_usd for r in arm_rows if r.cost_usd is not None]
        total_cost = sum(costs) if costs else None

        acc_hits: list[float] = []
        brier_terms: list[float] = []
        if labels_by_id:
            for r in arm_rows:
                if r.decision_id not in labels_by_id:
                    continue
                y = labels_by_id[r.decision_id]
                pred_pos = 1.0 if r.candidate_action in pos_action_set else 0.0
                acc_hits.append(1.0 if pred_pos == y else 0.0)
                p = _extract_probability(r, probability_key)
                if p is not None:
                    brier_terms.append((p - y) ** 2)

        summaries.append(
            ArmSummary(
                arm=arm.name,
                decisions=total,
                agreement_vs_live=agreed / total if total else 0.0,
                model_changes=model_changes,
                within_noise=within_noise,
                late_data_count=late_count,
                accuracy=(sum(acc_hits) / len(acc_hits)) if acc_hits else None,
                brier_score=(sum(brier_terms) / len(brier_terms)) if brier_terms else None,
                total_cost_usd=total_cost,
                latency_p50_ms=_percentile(latencies, 0.50),
                latency_p95_ms=_percentile(latencies, 0.95),
            )
        )

    return ComparisonReport(
        featureset=fs_name,
        summaries=summaries,
        per_arm_rows=per_arm_rows,
        warnings=warnings,
    )
