from datetime import datetime

import pytest

from thyme.dataset import dataset, field
from thyme.featureset import (
    EXTRACTOR_KIND_LOOKUP,
    EXTRACTOR_KIND_PY_FUNC,
    clear_featureset_registry,
    extractor,
    extractor_inputs,
    extractor_outputs,
    feature,
    featureset,
    get_registered_featuresets,
)


@pytest.fixture(autouse=True)
def clean_registry():
    clear_featureset_registry()
    yield
    clear_featureset_registry()


# ---------------------------------------------------------------------------
# Basic feature/extractor registration
# ---------------------------------------------------------------------------


def test_featureset_registers_features():
    @featureset
    class TestFeatures:
        user_id: str = feature()
        score: float = feature()

    fs = get_registered_featuresets()
    assert "TestFeatures" in fs
    assert len(fs["TestFeatures"]["features"]) == 2
    assert fs["TestFeatures"]["features"][0]["name"] == "user_id"
    assert fs["TestFeatures"]["features"][1]["name"] == "score"
    assert fs["TestFeatures"]["features"][1]["dtype"] == "float"


def test_features_have_no_id_field():
    """After dropping integer feature IDs, features should not carry an id key."""
    @featureset
    class TestFeatures:
        user_id: str = feature()
        score: float = feature()

    fs = get_registered_featuresets()
    for f in fs["TestFeatures"]["features"]:
        assert "id" not in f


def test_featureset_registers_extractors():
    @featureset
    class TestFeatures:
        score: float = feature()
        is_good: bool = feature()

        @extractor
        @extractor_inputs("score")
        @extractor_outputs("is_good")
        def compute_good(cls, ts, scores):
            return scores > 0.5

    fs = get_registered_featuresets()
    extractors = fs["TestFeatures"]["extractors"]
    assert len(extractors) == 1
    assert extractors[0]["name"] == "compute_good"
    assert extractors[0]["inputs"] == ["score"]
    assert extractors[0]["outputs"] == ["is_good"]
    assert extractors[0]["kind"] == EXTRACTOR_KIND_PY_FUNC


def test_user_extractor_with_deps_remains_pyfunc():
    """A user-written @extractor with deps stays PY_FUNC. Only auto-generated
    LOOKUP-kind extractors are created from feature(ref=...)."""
    class UserStats:
        pass

    @featureset
    class TestFeatures:
        user_id: str = feature()
        score: float = feature()

        @extractor(deps=[UserStats])
        @extractor_inputs("user_id")
        @extractor_outputs("score")
        def compute_score(cls, ts, user_ids):
            return 1.0

    fs = get_registered_featuresets()
    extractors = fs["TestFeatures"]["extractors"]
    assert len(extractors) == 1
    assert extractors[0]["deps"] == ["UserStats"]
    assert extractors[0]["kind"] == EXTRACTOR_KIND_PY_FUNC


def test_pyfunc_extractor_source_code_captured():
    @featureset
    class TestFeatures:
        x: int = feature()

        @extractor
        def compute_x(cls, ts, data):
            return data * 2

    fs = get_registered_featuresets()
    ext = fs["TestFeatures"]["extractors"][0]
    assert "compute_x" in ext["source_code"]
    assert "data * 2" in ext["source_code"]


def test_feature_descriptor_stores_dtype():
    @featureset
    class TestFeatures:
        name: str = feature()
        count: int = feature()
        active: bool = feature()

    fs = get_registered_featuresets()
    features = {f["name"]: f for f in fs["TestFeatures"]["features"]}
    assert features["name"]["dtype"] == "str"
    assert features["count"]["dtype"] == "int"
    assert features["active"]["dtype"] == "bool"


# ---------------------------------------------------------------------------
# feature(ref=...) auto-generates LOOKUP-kind extractors
# ---------------------------------------------------------------------------


@dataset(version=1, index=True)
class _UserProfile:
    user_id: str = field(key=True)
    loyalty_tier: str = field()
    timestamp: datetime = field(timestamp=True)


def test_feature_ref_generates_lookup_extractor():
    @featureset
    class WithLookup:
        user_id: str = feature()
        loyalty_tier: str = feature(ref=_UserProfile.loyalty_tier)

    fs = get_registered_featuresets()
    extractors = fs["WithLookup"]["extractors"]
    assert len(extractors) == 1
    ext = extractors[0]
    assert ext["kind"] == EXTRACTOR_KIND_LOOKUP
    assert ext["outputs"] == ["loyalty_tier"]
    assert ext["lookup_info"]["dataset_name"] == "_UserProfile"
    assert ext["lookup_info"]["field_name"] == "loyalty_tier"
    # LOOKUP extractors have no Python body — the planner short-circuits
    # to a ReadState step.
    assert "source_code" not in ext


def test_feature_ref_with_default_flows_into_lookup_info():
    @featureset
    class WithDefault:
        user_id: str = feature()
        loyalty_tier: str = feature(
            ref=_UserProfile.loyalty_tier,
            default="none",
        )

    fs = get_registered_featuresets()
    ext = fs["WithDefault"]["extractors"][0]
    assert ext["lookup_info"]["default"] == "none"


def test_feature_ref_without_default_omits_default_field():
    @featureset
    class NoDefault:
        user_id: str = feature()
        loyalty_tier: str = feature(ref=_UserProfile.loyalty_tier)

    fs = get_registered_featuresets()
    ext = fs["NoDefault"]["extractors"][0]
    assert "default" not in ext["lookup_info"]


def test_feature_ref_rejects_non_field_argument():
    with pytest.raises(TypeError, match="dataset Field reference"):
        @featureset
        class Bad:
            user_id: str = feature()
            wrong: str = feature(ref="not_a_field")  # type: ignore[arg-type]


def test_mixed_lookup_and_pyfunc_extractors_coexist():
    @featureset
    class Mixed:
        user_id: str = feature()
        loyalty_tier: str = feature(ref=_UserProfile.loyalty_tier, default="none")
        intent_score: float = feature()

        @extractor
        @extractor_inputs("loyalty_tier")
        @extractor_outputs("intent_score")
        def compute_intent(cls, ts, loyalty_tier):
            return 1.0

    fs = get_registered_featuresets()
    by_kind = {ext["kind"] for ext in fs["Mixed"]["extractors"]}
    assert by_kind == {EXTRACTOR_KIND_LOOKUP, EXTRACTOR_KIND_PY_FUNC}


# ---------------------------------------------------------------------------
# Extractor input/output validation
# ---------------------------------------------------------------------------


def test_extractor_valid_input_output_passes():
    @featureset
    class Good:
        score: float = feature()
        is_good: bool = feature()

        @extractor
        @extractor_inputs("score")
        @extractor_outputs("is_good")
        def compute(cls, ts, scores):
            return scores > 0.5

    fs = get_registered_featuresets()
    assert "Good" in fs


def test_extractor_bad_input_name_raises():
    with pytest.raises(ValueError, match="input feature 'scroe'"):
        @featureset
        class Bad:
            score: float = feature()
            is_good: bool = feature()

            @extractor
            @extractor_inputs("scroe")
            @extractor_outputs("is_good")
            def compute(cls, ts, scores):
                return scores > 0.5


def test_extractor_bad_output_name_raises():
    with pytest.raises(ValueError, match="output feature 'is_goood'"):
        @featureset
        class Bad:
            score: float = feature()
            is_good: bool = feature()

            @extractor
            @extractor_inputs("score")
            @extractor_outputs("is_goood")
            def compute(cls, ts, scores):
                return scores > 0.5


def test_extractor_empty_inputs_outputs_allowed():
    @featureset
    class DepsOnly:
        score: float = feature()

        @extractor
        def fetch(cls, ts, data):
            pass

    fs = get_registered_featuresets()
    assert "DepsOnly" in fs
    assert fs["DepsOnly"]["extractors"][0]["inputs"] == []
    assert fs["DepsOnly"]["extractors"][0]["outputs"] == []
