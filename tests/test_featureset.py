import pytest

from thyme.featureset import (
    featureset,
    feature,
    extractor,
    extractor_inputs,
    extractor_outputs,
    clear_featureset_registry,
    get_registered_featuresets,
)


@pytest.fixture(autouse=True)
def clean_registry():
    clear_featureset_registry()
    yield
    clear_featureset_registry()


def test_featureset_registers_features():
    @featureset
    class TestFeatures:
        user_id: str = feature(id=1)
        score: float = feature(id=2)

    fs = get_registered_featuresets()
    assert "TestFeatures" in fs
    assert len(fs["TestFeatures"]["features"]) == 2
    assert fs["TestFeatures"]["features"][0]["name"] == "user_id"
    assert fs["TestFeatures"]["features"][0]["id"] == 1
    assert fs["TestFeatures"]["features"][1]["name"] == "score"
    assert fs["TestFeatures"]["features"][1]["dtype"] == "float"


def test_featureset_registers_extractors():
    @featureset
    class TestFeatures:
        score: float = feature(id=1)
        is_good: bool = feature(id=2)

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


def test_extractor_with_deps():
    class UserStats:
        pass

    @featureset
    class TestFeatures:
        user_id: str = feature(id=1)
        score: float = feature(id=2)

        @extractor(deps=[UserStats])
        @extractor_inputs("user_id")
        @extractor_outputs("score")
        def get_score(cls, ts, user_ids):
            pass

    fs = get_registered_featuresets()
    extractors = fs["TestFeatures"]["extractors"]
    assert len(extractors) == 1
    assert extractors[0]["deps"] == ["UserStats"]
    assert extractors[0]["version"] == 1


def test_extractor_source_code_captured():
    @featureset
    class TestFeatures:
        x: int = feature(id=1)

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
        name: str = feature(id=1)
        count: int = feature(id=2)
        active: bool = feature(id=3)

    fs = get_registered_featuresets()
    features = {f["name"]: f for f in fs["TestFeatures"]["features"]}
    assert features["name"]["dtype"] == "str"
    assert features["count"]["dtype"] == "int"
    assert features["active"]["dtype"] == "bool"


# ---------------------------------------------------------------------------
# Task 14: Feature ID validation
# ---------------------------------------------------------------------------


def test_duplicate_feature_id_raises():
    with pytest.raises(ValueError, match="Duplicate feature id=1"):
        @featureset
        class Bad:
            a: int = feature(id=1)
            b: float = feature(id=1)


def test_feature_id_zero_raises():
    with pytest.raises(ValueError, match="invalid id=0"):
        @featureset
        class Bad:
            a: int = feature(id=0)


def test_feature_id_negative_raises():
    with pytest.raises(ValueError, match="invalid id=-1"):
        @featureset
        class Bad:
            a: int = feature(id=-1)


# ---------------------------------------------------------------------------
# Task 15: Extractor input/output validation
# ---------------------------------------------------------------------------


def test_extractor_valid_input_output_passes():
    @featureset
    class Good:
        score: float = feature(id=1)
        is_good: bool = feature(id=2)

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
            score: float = feature(id=1)
            is_good: bool = feature(id=2)

            @extractor
            @extractor_inputs("scroe")
            @extractor_outputs("is_good")
            def compute(cls, ts, scores):
                return scores > 0.5


def test_extractor_bad_output_name_raises():
    with pytest.raises(ValueError, match="output feature 'is_goood'"):
        @featureset
        class Bad:
            score: float = feature(id=1)
            is_good: bool = feature(id=2)

            @extractor
            @extractor_inputs("score")
            @extractor_outputs("is_goood")
            def compute(cls, ts, scores):
                return scores > 0.5


def test_extractor_empty_inputs_outputs_allowed():
    @featureset
    class DepsOnly:
        score: float = feature(id=1)

        @extractor
        def fetch(cls, ts, data):
            pass

    fs = get_registered_featuresets()
    assert "DepsOnly" in fs
    assert fs["DepsOnly"]["extractors"][0]["inputs"] == []
    assert fs["DepsOnly"]["extractors"][0]["outputs"] == []
