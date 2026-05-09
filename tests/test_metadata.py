import pytest

from thyme.metadata import EntityMetadata, _build_metadata


def test_default_metadata_is_empty():
    md = EntityMetadata()
    assert md.description is None
    assert md.owner is None
    assert md.tags == {}
    assert md.project is None
    assert md.deprecated is False
    assert md.deprecation_reason is None
    assert md.replacement is None


def test_tags_list_is_normalized_to_dict():
    md = EntityMetadata(tags=["fraud", "user"])
    assert md.tags == {"fraud": "", "user": ""}


def test_tags_dict_passes_through():
    md = EntityMetadata(tags={"team": "risk", "domain": "fraud"})
    assert md.tags == {"team": "risk", "domain": "fraud"}


def test_build_metadata_extracts_known_kwargs():
    md = _build_metadata(
        "featureset",
        {
            "owner": "ml-platform@thyme.io",
            "tags": ["fraud"],
            "description": "x",
            "project": "risk",
            "deprecated": True,
            "deprecation_reason": "moved",
            "replacement": "NewFraud",
        },
    )
    assert md.owner == "ml-platform@thyme.io"
    assert md.tags == {"fraud": ""}
    assert md.description == "x"
    assert md.project == "risk"
    assert md.deprecated is True
    assert md.deprecation_reason == "moved"
    assert md.replacement == "NewFraud"


def test_build_metadata_warns_on_unknown_kwarg():
    with pytest.warns(FutureWarning, match="future_kwarg"):
        md = _build_metadata("featureset", {"owner": "x", "future_kwarg": 1})
    # Known kwargs still flow through despite the warning
    assert md.owner == "x"


def test_build_metadata_no_warning_when_only_known_kwargs():
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("error", FutureWarning)
        # Should not raise — no FutureWarning emitted
        _build_metadata("dataset", {"owner": "x", "tags": ["t1"]})


def test_build_metadata_empty_kwargs_returns_default():
    md = _build_metadata("source", {})
    assert md == EntityMetadata()
