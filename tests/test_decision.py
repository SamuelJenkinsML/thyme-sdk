"""Judgments, policies and providers, on both commit paths."""

import pytest

from thyme import Secret, feature, featureset
from thyme.compiler import compile_commit_request
from thyme.dataset import get_commit_payload
from thyme.decision import Provider, Rule, judge, policy
from thyme.expr import col
from thyme.featureset import get_registered_featuresets
from thyme.gen import featureset_pb2, provider_pb2

JEV = Provider(
    "jev",
    base_url="http://localhost:8098",
    api_key=Secret(env="JEV_API_KEY"),
    rate_limit_per_s=5.0,
    input_price_per_mtok=0.04,
    output_price_per_mtok=0.16,
)
RISK_OPTIONS = {
    "ordinary": "Normal activity for this card",
    "unusual": "Unusual, but possibly the real cardholder",
    "takeover": "Matches account takeover or card testing",
}


def card_checkout():
    """The running example of the decisions design."""

    @featureset
    class CardCheckout:
        card_id: str = feature()
        txn_count_5m: int = feature(request=True, description="Payments by this card in the last 5 minutes")
        amount: float = feature(request=True, description="Amount of this payment")
        risk: str = judge.choice(
            "How risky is this payment?",
            RISK_OPTIONS,
            state=[txn_count_5m, amount],
            provider=JEV,
            model="jev-1.14.0",
            deadline_ms=800,
        )
        card_testing: float = judge.noul(
            "Several small payments come before one large payment",
            state=["txn_count_5m", "amount"],
            provider=JEV,
            model="jev-1.14.0",
            retries=0,
        )
        action: str = policy(
            Rule("block", when=col("card_testing") > 0.85),
            Rule("step_up", when=col("risk.takeover") > 0.6),
            Rule("approve"),
            fallback="step_up",
        )

    return CardCheckout


def above(column, threshold):
    return {
        "kind": {
            "Compare": {
                "lhs": {"kind": {"ColumnRef": column}},
                "op": 5,
                "rhs": {"kind": {"Literal": {"kind": {"FloatValue": threshold}}}},
            }
        }
    }


def test_the_json_payload_is_the_shape_the_definition_service_accepts():
    # Given the running example
    card_checkout()

    # When it becomes a commit payload
    payload = get_commit_payload()

    # Then the provider, the judgments and the policy are in the stored shape
    assert payload["providers"] == [
        {
            "name": "jev",
            "format": "systemone",
            "base_url": "http://localhost:8098",
            "api_key": {"kind": "env", "value": "JEV_API_KEY"},
            "rate_limit_per_s": 5.0,
            "input_price_per_mtok": 0.04,
            "output_price_per_mtok": 0.16,
        }
    ]
    fs = payload["featuresets"][0]
    described = {f["name"]: f["description"] for f in fs["features"]}
    assert described["amount"] == "Amount of this payment"
    assert described["risk"] is None
    decisions = {e["outputs"][0]: e for e in fs["extractors"] if e["kind"] == "DECISION"}
    assert decisions["risk"]["name"] == "_thyme_decision_risk"
    assert decisions["risk"]["decision"] == {
        "provider": "jev",
        "model": "jev-1.14.0",
        "state": ["txn_count_5m", "amount"],
        "question": {"type": "choice", "instructions": "How risky is this payment?", "criteria": RISK_OPTIONS},
        "deadline_ms": 800,
    }
    assert decisions["card_testing"]["decision"]["retries"] == 0
    assert fs["policies"] == [
        {
            "output": "action",
            "version": 1,
            "rules": [
                {"label": "block", "when": above("card_testing", 0.85)},
                {"label": "step_up", "when": above("risk.takeover", 0.6)},
                {"label": "approve"},
            ],
            "fallback": "step_up",
        }
    ]


def test_the_proto_payload_carries_the_same_definition():
    # Given the running example
    card_checkout()

    # When the CLI compiles it from the registry
    request = compile_commit_request(
        message="",
        datasets=[],
        pipelines=[],
        featuresets=list(get_registered_featuresets().values()),
        sources=[],
    )

    # Then the provider, judgments, descriptions and policy are all in it
    (provider,) = request.providers
    assert provider.format == provider_pb2.Provider.SYSTEMONE
    assert provider.api_key.value == "JEV_API_KEY"
    assert provider.rate_limit_per_s == 5.0
    assert not provider.HasField("samples")
    (fs,) = request.featuresets
    amount = next(f for f in fs.features if f.name == "amount")
    assert amount.description == "Amount of this payment"
    risk = next(e for e in fs.extractors if e.outputs == ["risk"])
    assert risk.kind == featureset_pb2.DECISION
    assert [o.key for o in risk.decision.question.choice.options] == list(RISK_OPTIONS)
    assert risk.decision.deadline_ms == 800
    assert not risk.decision.HasField("retries")
    (action,) = fs.policies
    assert [r.label for r in action.rules] == ["block", "step_up", "approve"]
    assert action.rules[1].when.compare.lhs.column_ref == "risk.takeover"
    assert not action.rules[2].HasField("when")


@pytest.mark.parametrize("model", ["jev-latest", "jev", "d3-edge@main"])
def test_an_alias_model_is_refused_before_the_commit(model):
    # Given a judgment on a model a vendor can change, when it is declared,
    # then it is refused and the error says how to pin it
    with pytest.raises(ValueError, match="alias"):
        judge.noul("Card testing", state=["amount"], provider=JEV, model=model)


def test_an_alias_model_is_allowed_when_asked_for():
    # Given allow_alias, when the judgment is declared, then the spec carries it
    judgment = judge.noul("Card testing", state=["amount"], provider=JEV, model="jev-latest", allow_alias=True)
    assert judgment.decision_spec({})["allow_alias"] is True


def test_two_providers_with_one_name_are_refused():
    # Given two featuresets whose judgments name different providers called jev
    card_checkout()
    other = Provider("jev", base_url="https://elsewhere.example")

    @featureset
    class Refunds:
        amount: float = feature(request=True, description="Refund amount")
        abuse: float = judge.noul("Refund abuse", state=["amount"], provider=other, model="jev-1.14.0")

    # When the commit is built, then it is refused rather than one silently winning
    with pytest.raises(ValueError, match="two different providers are named 'jev'"):
        get_commit_payload()


def test_a_policy_needs_a_rule_and_a_fallback():
    with pytest.raises(ValueError, match="fallback"):
        policy(Rule("approve"), fallback="")
    with pytest.raises(ValueError, match="at least one rule"):
        policy(fallback="step_up")


def test_a_judgment_is_annotated_with_its_value_type():
    # Given a noul annotated as str, when the featureset is declared, then it says what to write
    with pytest.raises(TypeError, match="Annotate it 'card_testing: float'"):

        @featureset
        class Wrong:
            amount: float = feature(request=True, description="Amount")
            card_testing: str = judge.noul("Card testing", state=["amount"], provider=JEV, model="jev-1.14.0")


def test_two_providers_with_one_name_in_one_featureset_are_refused():
    # Given two judgments in one featureset whose providers share a name
    local = Provider("jev", base_url="http://localhost:8098")

    # When it is declared, then it is refused rather than one silently winning
    with pytest.raises(ValueError, match="two different providers are named 'jev'"):

        @featureset
        class Mixed:
            amount: float = feature(request=True, description="Amount")
            a: float = judge.noul("A", state=["amount"], provider=JEV, model="jev-1.14.0")
            b: float = judge.noul("B", state=["amount"], provider=local, model="jev-1.14.0")


def test_a_literal_provider_key_is_refused():
    with pytest.raises(TypeError, match="Secret"):
        Provider("jev", base_url="https://api.jev.example", api_key="sk-123")
