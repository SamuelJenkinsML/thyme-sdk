"""Backtests over past decisions: policy replay, re-ask, rebuild, and multi-arm comparison.

Policy replay, re-ask, and rebuild execute through ``POST /decisions/backtest``
on the query server so canonical state rendering, model calls, and policy
evaluation share one Rust implementation with live reads.
"""

from __future__ import annotations

import json
import os
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, Sequence

import polars as pl

from thyme.decision import PolicyDescriptor
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


def _chunks(items: list[Any], size: int):
    if size <= 0:
        raise ValueError("chunk_size must be positive")
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


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
