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

    def to_dict(self) -> dict:
        out: dict[str, Any] = {"name": self.name, "format": "systemone", "base_url": self.base_url}
        if self.api_key is not None:
            out["api_key"] = self.api_key.to_dict()
        for key in ("rate_limit_per_s", "samples", "input_price_per_mtok", "output_price_per_mtok"):
            value = getattr(self, key)
            if value is not None:
                out[key] = value
        return out


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
            raise ValueError("a judgment needs at least one state feature")
        super().__init__(description=description)
        self.question = question
        self.state = list(state)
        self.provider = provider
        self.model = model
        self.allow_alias = allow_alias
        self.deadline_ms = deadline_ms
        self.retries = retries

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
        """Pick one of ``options`` (key to meaning). Rules read each option's
        probability as ``col("<feature>.<key>")``."""
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
        question = {"type": "score", "instructions": instructions, "levels": list(levels)}
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
            raise ValueError("a policy needs at least one rule")
        if not fallback:
            raise ValueError("a policy needs a fallback: the action when a judgment it reads fails")
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


def collect_providers(featuresets: Sequence[dict]) -> list[dict]:
    """The providers that the featuresets' judgments name, each once."""
    providers: dict[str, dict] = {}
    for fs in featuresets:
        for provider in fs.get("providers", []):
            seen = providers.setdefault(provider["name"], provider)
            if seen != provider:
                raise ValueError(
                    f"two different providers are named '{provider['name']}'; "
                    "define the Provider once and use it in every judgment"
                )
    return list(providers.values())
