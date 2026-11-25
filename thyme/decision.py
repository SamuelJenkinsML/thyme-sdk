"""Judgments and policies: a featureset asks a System One model about a state
and turns the answers into an action.

>>> from thyme import feature, featureset, Secret
>>> from thyme.decision import Provider, Rule, judge, policy
>>> from thyme.expr import col
>>> jev = Provider("jev", base_url="https://api.jev.example",
...                api_key=Secret(env="JEV_API_KEY"))
>>> @featureset
... class CardCheckout:
...     card_id: str = feature()
...     amount: float = feature(request=True, description="Amount of this payment")
...     card_testing: float = judge.noul(
...         "Several small payments come before one large payment",
...         state=[amount], provider=jev, model="jev-1.14.0")
...     action: str = policy(
...         Rule("block", when=col("card_testing") > 0.85),
...         Rule("approve"),
...         fallback="step_up")
>>> [e["name"] for e in CardCheckout._featureset_meta["extractors"]]
['_thyme_decision_card_testing']
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Sequence

from thyme.expr import PredicateExpr
from thyme.featureset import FeatureDescriptor
from thyme.secret import Secret

EXTRACTOR_KIND_DECISION = "DECISION"

_DOTTED_VERSION = re.compile(r"\d\.\d")
_DATE = re.compile(r"\d{4}-\d{2}-\d{2}|20\d{6}")
_COMMIT = re.compile(r"[0-9a-fA-F]{7,}")


def is_alias(model: str) -> bool:
    """A model name that can come to mean another model, such as ``jev-latest``.

    A record must name the model that answered and a backtest must ask that
    same model, so only a dotted version (``jev-1.14.0``), a dated snapshot
    (``gpt-4o-2024-08-06``) or a commit revision (``d3-edge@bee5e872``) is
    pinned. The definition service applies the same rule; this one fails
    before the commit is sent.

    >>> is_alias("jev-latest"), is_alias("jev-1.14.0"), is_alias("d3-edge@main")
    (True, False, True)
    """
    if "latest" in model.lower():
        return True
    if "@" in model:
        return _COMMIT.fullmatch(model.split("@", 1)[1]) is None
    return not (_DOTTED_VERSION.search(model) or _DATE.search(model))


@dataclass(frozen=True)
class Provider:
    """A model service that judgments are sent to.

    ``api_key`` is a :class:`~thyme.Secret`, resolved by the query server; a
    literal key is refused at commit. ``samples`` is how many times a backtest
    asks each decision: absent means 3, and a model that always gives the same
    answer sets 1.
    """

    name: str
    base_url: str
    api_key: Optional[Secret] = None
    rate_limit_per_s: Optional[float] = None
    samples: Optional[int] = None
    input_price_per_mtok: Optional[float] = None
    output_price_per_mtok: Optional[float] = None

    def __post_init__(self):
        if self.api_key is not None and not isinstance(self.api_key, Secret):
            raise TypeError(
                f"Provider '{self.name}' api_key must be a Secret, so the key never sits in "
                "committed code. Pass api_key=Secret(env=\"JEV_API_KEY\")."
            )

    def to_dict(self) -> dict:
        out: dict[str, Any] = {"name": self.name, "format": "systemone", "base_url": self.base_url}
        if self.api_key is not None:
            out["api_key"] = self.api_key.to_dict()
        for key in ("rate_limit_per_s", "samples", "input_price_per_mtok", "output_price_per_mtok"):
            value = getattr(self, key)
            if value is not None:
                out[key] = value
        return out


# The feature's value: the chosen option, the score, or the probability.
# The probabilities of a choice are in the response's judgments and in the
# policy's columns, never in the feature.
_VALUE_DTYPES = {"choice": "str", "score": "float", "noul": "float"}


class JudgmentDescriptor(FeatureDescriptor):
    """A feature whose value is a model's answer to one question."""

    def __init__(
        self,
        question: dict,
        state: Sequence[str | FeatureDescriptor],
        provider: Provider,
        model: str,
        allow_alias: bool,
        deadline_ms: Optional[int],
        retries: Optional[int],
        description: Optional[str],
    ):
        if not allow_alias and is_alias(model):
            raise ValueError(
                f"model '{model}' is an alias: a vendor can change the model under the "
                "policy without a commit. Pin a version, such as 'jev-1.14.0', or pass "
                "allow_alias=True."
            )
        if not state:
            raise ValueError(
                "a judgment needs at least one state feature. Pass state=[...] with the "
                "features the model reads, each declared with a description."
            )
        super().__init__(description=description)
        self.question = question
        self.state = list(state)
        self.provider = provider
        self.model = model
        self.allow_alias = allow_alias
        self.deadline_ms = deadline_ms
        self.retries = retries

    def check_dtype(self, feature_name: str, dtype: str) -> None:
        expected = _VALUE_DTYPES[self.question["type"]]
        if dtype != expected:
            raise TypeError(
                f"judgment '{feature_name}' is a {self.question['type']}, whose value is a "
                f"{expected}; it is annotated {dtype}. Annotate it '{feature_name}: {expected}'."
            )

    def decision_spec(self, names: dict[int, str]) -> dict:
        spec: dict[str, Any] = {
            "provider": self.provider.name,
            "model": self.model,
            "state": [_state_name(s, names) for s in self.state],
            "question": self.question,
        }
        if self.allow_alias:
            spec["allow_alias"] = True
        if self.deadline_ms is not None:
            spec["deadline_ms"] = self.deadline_ms
        if self.retries is not None:
            spec["retries"] = self.retries
        return spec


def _state_name(feature: str | FeatureDescriptor, names: dict[int, str]) -> str:
    if isinstance(feature, str):
        return feature
    try:
        return names[id(feature)]
    except KeyError:
        raise ValueError(
            "a judgment's state names a feature of another class; pass its name as a string"
        ) from None


class judge:
    """The three questions a System One model answers.

    Each takes ``state`` (the features sent to the model, in order: order is
    part of the state's hash), ``provider`` and a pinned ``model``. A failed
    or late answer leaves the value empty and the policy falls back.
    """

    @staticmethod
    def choice(
        instructions: str,
        options: dict[str, str],
        *,
        state: Sequence[str | FeatureDescriptor],
        provider: Provider,
        model: str,
        allow_alias: bool = False,
        deadline_ms: Optional[int] = None,
        retries: Optional[int] = None,
        description: Optional[str] = None,
    ) -> Any:
        """Pick one of ``options`` (key to meaning). The feature is the chosen
        key, a ``str``; rules read each option's probability as
        ``col("<feature>.<key>")``."""
        question = {"type": "choice", "instructions": instructions, "criteria": dict(options)}
        return JudgmentDescriptor(question, state, provider, model, allow_alias, deadline_ms, retries, description)

    @staticmethod
    def score(
        instructions: str,
        levels: Sequence[str],
        *,
        state: Sequence[str | FeatureDescriptor],
        provider: Provider,
        model: str,
        allow_alias: bool = False,
        deadline_ms: Optional[int] = None,
        retries: Optional[int] = None,
        description: Optional[str] = None,
    ) -> Any:
        """Rate on ordered ``levels``, lowest first."""
        # The wire format names the levels `criteria`, as a choice's options.
        question = {"type": "score", "instructions": instructions, "criteria": list(levels)}
        return JudgmentDescriptor(question, state, provider, model, allow_alias, deadline_ms, retries, description)

    @staticmethod
    def noul(
        instructions: str,
        *,
        state: Sequence[str | FeatureDescriptor],
        provider: Provider,
        model: str,
        allow_alias: bool = False,
        deadline_ms: Optional[int] = None,
        retries: Optional[int] = None,
        description: Optional[str] = None,
    ) -> Any:
        """The probability that ``instructions``, a statement, is true."""
        question = {"type": "noul", "instructions": instructions}
        return JudgmentDescriptor(question, state, provider, model, allow_alias, deadline_ms, retries, description)


@dataclass(frozen=True)
class Rule:
    """``label`` is the action when ``when`` holds; no ``when`` always holds."""

    label: str
    when: Optional[PredicateExpr] = None


class PolicyDescriptor(FeatureDescriptor):
    """A feature whose value is the action of the first rule that holds."""

    def __init__(self, rules: Sequence[Rule], fallback: str, version: int, description: Optional[str]):
        if not rules:
            raise ValueError(
                "a policy needs at least one rule. Pass Rule(label, when=...) for each "
                "action, and a last Rule(label) with no condition if one should always apply."
            )
        if not fallback:
            raise ValueError(
                "a policy needs a fallback: the action when a judgment it reads fails. "
                "Pass fallback=\"step_up\" or another safe action."
            )
        super().__init__(description=description)
        self.rules = list(rules)
        self.fallback = fallback
        self.version = version

    def policy_spec(self, output: str) -> dict:
        rules = []
        for rule in self.rules:
            entry: dict[str, Any] = {"label": rule.label}
            if rule.when is not None:
                entry["when"] = rule.when.to_proto()
            rules.append(entry)
        return {"output": output, "version": self.version, "rules": rules, "fallback": self.fallback}


def policy(*rules: Rule, fallback: str, version: int = 1, description: Optional[str] = None) -> Any:
    """Ordered rules on the featureset's judgments and features.

    Raise ``version`` whenever the rules change: a stored decision names the
    version that made it, and the definition service refuses changed rules at
    the same version.
    """
    return PolicyDescriptor(rules, fallback, version, description)


def unique_providers(providers: Sequence[dict]) -> list[dict]:
    """Each provider once; two different providers with one name are refused,
    because both judgments would be sent to whichever came last."""
    by_name: dict[str, dict] = {}
    for provider in providers:
        seen = by_name.setdefault(provider["name"], provider)
        if seen != provider:
            raise ValueError(
                f"two different providers are named '{provider['name']}'. "
                "Define the Provider once and use it in every judgment."
            )
    return list(by_name.values())


def collect_providers(featuresets: Sequence[dict]) -> list[dict]:
    """The providers that the featuresets' judgments name, each once."""
    return unique_providers([p for fs in featuresets for p in fs.get("providers", [])])


_WINDOW_RE = re.compile(r"^\s*(\d+)\s*(ms|s|m|h|d)\s*$")
_WINDOW_UNITS = {
    "ms": lambda n: timedelta(milliseconds=n),
    "s": lambda n: timedelta(seconds=n),
    "m": lambda n: timedelta(minutes=n),
    "h": lambda n: timedelta(hours=n),
    "d": lambda n: timedelta(days=n),
}


def parse_window(window: str | timedelta) -> timedelta:
    """Parse a duration such as ``"30d"``, ``"24h"``, ``"15m"`` or a ``timedelta``."""
    if isinstance(window, timedelta):
        if window < timedelta(0):
            raise ValueError("outcome attach window must not be negative")
        return window
    m = _WINDOW_RE.match(window)
    if not m:
        raise ValueError(
            f"invalid window {window!r}; expected e.g. '30d', '24h', '15m', or '60s'"
        )
    return _WINDOW_UNITS[m.group(2)](int(m.group(1)))


def _parse_utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    raise TypeError(f"expected datetime or RFC-3339 string, got {type(value).__name__}")


@dataclass(frozen=True)
class OutcomeDatasetSpec:
    """Attach configuration declared on an outcome dataset."""

    featureset: str
    window: timedelta


def outcome(*, featureset: str | type, window: str | timedelta):
    """Attach a decision featureset and lookback window to an outcome ``@dataset``.

    Outcomes without an explicit ``decision_id`` attach to the latest decision
    for the same ``(featureset, entity_id)`` inside ``window``.
    """
    fs_name = featureset.__name__ if isinstance(featureset, type) else str(featureset)
    spec = OutcomeDatasetSpec(featureset=fs_name, window=parse_window(window))

    def _apply(cls: type) -> type:
        cls._outcome_spec = spec
        if hasattr(cls, "_dataset_meta"):
            cls._dataset_meta["outcome"] = {
                "featureset": spec.featureset,
                "window_seconds": int(spec.window.total_seconds()),
            }
        return cls

    return _apply


def attach_outcomes(
    decisions: Any,
    outcomes: Any,
    *,
    window: str | timedelta | None = None,
    featureset: str | type | None = None,
) -> list[dict[str, Any]]:
    """Attach each outcome to its decision by ID or by entity inside ``window``.

    - When ``outcome["decision_id"]`` is set, it attaches directly to that
      decision (even outside ``window``).
    - When ``outcome["decision_id"]`` is absent or ``None``, it attaches to the
      latest decision for the same ``(featureset, entity_id)`` with
      ``0 <= occurred_at - decided_at <= window``.
    - Outcomes that do not match any decision keep ``attached_decision_id = None``.
    """
    decision_rows = decisions.to_dicts() if hasattr(decisions, "to_dicts") else list(decisions)
    outcome_rows = outcomes.to_dicts() if hasattr(outcomes, "to_dicts") else list(outcomes)

    win = parse_window(window) if window is not None else None
    fs_filter = featureset.__name__ if isinstance(featureset, type) else featureset

    by_id: dict[str, dict[str, Any]] = {}
    by_key: dict[tuple[str, str], list[tuple[datetime, dict[str, Any]]]] = {}
    for dec in decision_rows:
        if fs_filter is not None and dec.get("featureset") != fs_filter:
            continue
        did = dec["decision_id"]
        by_id[did] = dec
        key = (dec["featureset"], dec["entity_id"])
        by_key.setdefault(key, []).append((_parse_utc(dec["decided_at"]), dec))
    for entries in by_key.values():
        entries.sort(key=lambda item: item[0])

    attached: list[dict[str, Any]] = []
    for out in outcome_rows:
        row = dict(out)
        explicit_id = out.get("decision_id")
        matched: dict[str, Any] | None = None
        if explicit_id:
            matched = by_id.get(explicit_id)
        elif win is not None:
            out_fs = out.get("featureset") or fs_filter
            out_entity = out.get("entity_id")
            out_ts = _parse_utc(out["occurred_at"])
            candidates = by_key.get((out_fs, out_entity), []) if out_fs and out_entity else []
            for dec_ts, dec in reversed(candidates):
                if dec_ts <= out_ts and (out_ts - dec_ts) <= win:
                    matched = dec
                    break
        row["attached_decision_id"] = matched["decision_id"] if matched else None
        row["decision"] = matched
        attached.append(row)
    return attached
