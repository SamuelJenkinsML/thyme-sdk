"""Tests for thyme.codegen.ir — building the intermediate representation
from the featureset / dataset registries."""

import keyword
from datetime import datetime
from typing import Optional

import pytest

from thyme.codegen.ir import (
    CodegenIR,
    DatasetIR,
    FeatureIR,
    FeaturesetIR,
    build_ir,
)
from thyme.dataset import dataset, field
from thyme.featureset import feature, featureset


class TestBuildIR:
    """Given decorated featuresets/datasets in the registry, when calling
    build_ir(), then a normalised CodegenIR is produced."""

    def test_builds_featureset_ir_from_registry(self) -> None:
        # given
        @featureset
        class UserFeatures:
            user_id: int = feature(id=1)
            total_spend: float = feature(id=2)
            is_active: bool = feature(id=3)

        # when
        ir = build_ir()

        # then
        assert isinstance(ir, CodegenIR)
        assert len(ir.featuresets) == 1
        fs = ir.featuresets[0]
        assert fs.name == "UserFeatures"
        assert [f.name for f in fs.features] == ["user_id", "total_spend", "is_active"]
        assert [f.python_annotation for f in fs.features] == ["int", "float", "bool"]
        assert [f.feature_id for f in fs.features] == [1, 2, 3]

    def test_builds_dataset_ir_from_registry(self) -> None:
        # given
        @dataset(index=True, version=1)
        class Purchase:
            user_id: int = field(key=True)
            amount: float = field()
            event_time: datetime = field(timestamp=True)

        # when
        ir = build_ir()

        # then
        assert len(ir.datasets) == 1
        ds = ir.datasets[0]
        assert ds.name == "Purchase"
        assert [f.name for f in ds.fields] == ["user_id", "amount", "event_time"]
        assert [f.python_annotation for f in ds.fields] == [
            "int",
            "float",
            "datetime.datetime",
        ]

    def test_dataset_optional_field_flagged_in_ir(self) -> None:
        # given
        @dataset(index=True, version=1)
        class SparseEvents:
            entity_id: int = field(key=True)
            note: Optional[str] = field()
            event_time: datetime = field(timestamp=True)

        # when
        ir = build_ir()

        # then
        ds = ir.datasets[0]
        fields_by_name = {f.name: f for f in ds.fields}
        assert fields_by_name["note"].optional is True
        assert fields_by_name["note"].python_annotation == "str"
        assert fields_by_name["entity_id"].optional is False

    def test_featureset_optional_feature_flagged_in_ir(self) -> None:
        # given a featureset whose features use Optional[T] and PEP-604 unions
        @featureset
        class SparseSignals:
            user_id: int = feature(id=1)
            last_score: Optional[float] = feature(id=2)
            last_label: str | None = feature(id=3)

        # when
        ir = build_ir()

        # then both nullable features are flagged optional with the base
        # python_annotation; the non-nullable one stays non-optional.
        fs = ir.featuresets[0]
        features_by_name = {f.name: f for f in fs.features}
        assert features_by_name["user_id"].optional is False
        assert features_by_name["user_id"].python_annotation == "int"
        assert features_by_name["last_score"].optional is True
        assert features_by_name["last_score"].python_annotation == "float"
        assert features_by_name["last_label"].optional is True
        assert features_by_name["last_label"].python_annotation == "str"

    def test_datetime_feature_annotation_is_fully_qualified(self) -> None:
        # given
        @featureset
        class Events:
            last_seen: datetime = feature(id=1)

        # when
        ir = build_ir()

        # then
        assert ir.featuresets[0].features[0].python_annotation == "datetime.datetime"

    def test_keyword_feature_name_raises(self) -> None:
        # given a featureset with a Python-keyword feature name
        @featureset
        class BadNames:
            # 'class' is a keyword — we hand-craft the registry entry because
            # Python won't let us write `class: int = feature(id=1)` in source.
            pass

        # monkey-patch the registry so build_ir sees the bad name
        from thyme.featureset import _FEATURESET_REGISTRY

        _FEATURESET_REGISTRY["BadNames"] = {
            "name": "BadNames",
            "features": [{"name": "class", "dtype": "int", "id": 1}],
            "extractors": [],
        }
        assert keyword.iskeyword("class")

        # when/then
        with pytest.raises(ValueError, match="Python keyword"):
            build_ir()

    def test_unknown_dtype_raises(self) -> None:
        # given registry with a bogus dtype
        from thyme.featureset import _FEATURESET_REGISTRY

        _FEATURESET_REGISTRY["Weird"] = {
            "name": "Weird",
            "features": [{"name": "payload", "dtype": "complex128", "id": 1}],
            "extractors": [],
        }

        # when/then
        with pytest.raises(ValueError, match="Unknown Thyme dtype"):
            build_ir()

    def test_empty_registries_return_empty_ir(self) -> None:
        ir = build_ir()
        assert ir.featuresets == ()
        assert ir.datasets == ()

    def test_ir_is_deterministic_sorted_by_name(self) -> None:
        # given two featuresets declared in non-alphabetical order
        @featureset
        class Zebra:
            x: int = feature(id=1)

        @featureset
        class Alpha:
            y: int = feature(id=1)

        # when
        ir = build_ir()

        # then — emission order is stable regardless of declaration order,
        # so stubs diff cleanly.
        assert [fs.name for fs in ir.featuresets] == ["Alpha", "Zebra"]


class TestFeatureIRDataclass:
    """Given hand-built IR objects, they behave as frozen dataclasses."""

    def test_feature_ir_is_frozen(self) -> None:
        f = FeatureIR(name="x", python_annotation="int", feature_id=1)
        with pytest.raises(Exception):
            f.name = "y"  # type: ignore[misc]

    def test_default_optional_is_false(self) -> None:
        f = FeatureIR(name="x", python_annotation="int", feature_id=1)
        assert f.optional is False

    def test_featureset_ir_extractors_default_empty(self) -> None:
        fs = FeaturesetIR(name="X", features=())
        assert fs.extractors == ()

    def test_dataset_ir_holds_fields(self) -> None:
        ds = DatasetIR(
            name="D",
            fields=(FeatureIR(name="k", python_annotation="int", feature_id=0),),
        )
        assert ds.fields[0].name == "k"
