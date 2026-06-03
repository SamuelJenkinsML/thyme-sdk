"""End-to-end test for catalog metadata in the JSON commit payload (TH-CAT-A2).

The acceptance criterion in the feature-catalog design is `thyme commit --dry-run`
showing `metadata` in the payload JSON. The dry-run path is built by
`build_commit_request_for_api()` in `thyme/commit_payload.py`; this test
exercises that path with metadata-decorated entities and asserts the JSON
shape downstream tooling will consume."""

import json

import pytest

from thyme.dataset import clear_registry, dataset, field, get_commit_payload
from thyme.featureset import (
    extractor,
    extractor_inputs,
    extractor_outputs,
    feature,
    featureset,
)


@pytest.fixture(autouse=True)
def _isolate_registry():
    clear_registry()
    yield
    clear_registry()


def test_dataset_metadata_round_trips_through_json_payload():
    @dataset(
        index=True,
        version=1,
        description="Per-user purchases.",
        owner="data-eng@thyme.io",
        tags=["ingest", "purchases"],
        project="risk",
    )
    class Purchase:
        user_id: int = field(key=True)
        amount: float = field()
        from datetime import datetime as _dt
        ts: _dt = field(timestamp=True)

    payload = get_commit_payload()
    json.dumps(payload)  # must be JSON-encodable end to end
    assert len(payload["datasets"]) == 1
    meta = payload["datasets"][0]["metadata"]
    assert meta["description"] == "Per-user purchases."
    assert meta["owner"] == "data-eng@thyme.io"
    assert meta["tags"] == {"ingest": "", "purchases": ""}
    assert meta["project"] == "risk"
    assert meta["deprecated"] is False


def test_featureset_metadata_round_trips_through_json_payload():
    @dataset(version=1, index=True)
    class _Anchor:
        id: int = field(key=True)
        from datetime import datetime as _dt
        ts: _dt = field(timestamp=True)

    @featureset(
        description="User-level fraud signals.",
        owner="ml-platform@thyme.io",
        tags={"team": "risk", "domain": "fraud"},
        project="risk",
    )
    class UserFraudSignals:
        user_id: int = feature()
        score: float = feature()

        @extractor
        @extractor_inputs("user_id")
        @extractor_outputs("score")
        def stub(cls, ts, user_id):
            return 0.5

    payload = get_commit_payload()
    json.dumps(payload)
    fs_payload = next(
        fs for fs in payload["featuresets"] if fs["name"] == "UserFraudSignals"
    )
    meta = fs_payload["metadata"]
    assert meta["description"] == "User-level fraud signals."
    assert meta["owner"] == "ml-platform@thyme.io"
    # dict tags pass through unchanged.
    assert meta["tags"] == {"team": "risk", "domain": "fraud"}
    assert meta["project"] == "risk"


def test_cross_featureset_fqn_input_survives_commit_payload():
    """A cross-featureset object-ref input (TH-157 / A2a) lands in the commit
    payload as a fully-qualified `Featureset.feature` string — no payload/proto
    change required."""
    @featureset
    class _A2aSessionFeatures:
        user_id: int = feature()
        intent_score: float = feature()

    @featureset
    class _A2aRankingFeatures:
        user_id: int = feature()
        rank_score: float = feature()

        @extractor(deps=[_A2aSessionFeatures])
        @extractor_inputs(_A2aSessionFeatures.intent_score)
        @extractor_outputs("rank_score")
        def rank(cls, ts, intent_score):
            return intent_score * 2

    payload = get_commit_payload()
    json.dumps(payload)
    fs_payload = next(
        fs for fs in payload["featuresets"] if fs["name"] == "_A2aRankingFeatures"
    )
    ext = next(e for e in fs_payload["extractors"] if e["name"] == "rank")
    assert ext["inputs"] == ["_A2aSessionFeatures.intent_score"]


def test_bare_decorators_produce_empty_metadata_block():
    """Decorators without metadata kwargs still emit a `metadata` key with the
    default shape — so backend code can rely on its presence rather than
    branching on absence."""
    @dataset(version=1, index=True)
    class Bare:
        id: int = field(key=True)
        from datetime import datetime as _dt
        ts: _dt = field(timestamp=True)

    @featureset
    class BareFeatureset:
        id: int = feature()

    payload = get_commit_payload()
    ds_meta = payload["datasets"][0]["metadata"]
    fs_meta = next(
        fs for fs in payload["featuresets"] if fs["name"] == "BareFeatureset"
    )["metadata"]
    for meta in (ds_meta, fs_meta):
        assert meta["description"] is None
        assert meta["owner"] is None
        assert meta["tags"] == {}
        assert meta["project"] is None
        assert meta["deprecated"] is False
