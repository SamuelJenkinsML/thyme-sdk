"""Intermediate representation for codegen targets.

Every emitter (v1: Python stubs; future: Go, TypeScript, Java) reads the
same ``CodegenIR``. This decouples the registry wire format from emitter
code: add a new target by reading the IR, not by re-parsing the registry.
"""

from __future__ import annotations

import keyword
from dataclasses import dataclass, field as dc_field

from thyme.codegen.types import thyme_type_to_python_annotation
from thyme.dataset import get_registered_datasets
from thyme.featureset import get_registered_featuresets


@dataclass(frozen=True)
class FeatureIR:
    name: str
    python_annotation: str
    optional: bool = False


@dataclass(frozen=True)
class ExtractorSummaryIR:
    name: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    version: int


@dataclass(frozen=True)
class FeaturesetIR:
    name: str
    features: tuple[FeatureIR, ...]
    extractors: tuple[ExtractorSummaryIR, ...] = ()


@dataclass(frozen=True)
class DatasetIR:
    name: str
    fields: tuple[FeatureIR, ...]


@dataclass(frozen=True)
class CodegenIR:
    featuresets: tuple[FeaturesetIR, ...] = dc_field(default_factory=tuple)
    datasets: tuple[DatasetIR, ...] = dc_field(default_factory=tuple)


def _validate_name(name: str, context: str) -> None:
    if keyword.iskeyword(name):
        raise ValueError(
            f"Feature name '{name}' in {context} is a Python keyword and "
            f"cannot be emitted as a stub attribute. Rename the feature."
        )


def _feature_ir_from_featureset_entry(fs_name: str, entry: dict) -> FeatureIR:
    name = entry["name"]
    _validate_name(name, f"featureset '{fs_name}'")
    return FeatureIR(
        name=name,
        python_annotation=thyme_type_to_python_annotation(entry["dtype"]),
        optional=bool(entry.get("optional", False)),
    )


def _feature_ir_from_dataset_field(ds_name: str, entry: dict) -> FeatureIR:
    name = entry["name"]
    _validate_name(name, f"dataset '{ds_name}'")
    return FeatureIR(
        name=name,
        python_annotation=thyme_type_to_python_annotation(entry["type"]),
        optional=bool(entry.get("optional", False)),
    )


def _extractor_ir(entry: dict) -> ExtractorSummaryIR:
    return ExtractorSummaryIR(
        name=entry["name"],
        inputs=tuple(entry.get("inputs", ())),
        outputs=tuple(entry.get("outputs", ())),
        version=int(entry.get("version", 1)),
    )


def build_ir() -> CodegenIR:
    """Walk the featureset and dataset registries and build a normalised IR.

    Featuresets and datasets are sorted alphabetically by class name so the
    emitter output is deterministic and diffs cleanly.
    """
    fs_registry = get_registered_featuresets()
    ds_registry = get_registered_datasets()

    featuresets = tuple(
        FeaturesetIR(
            name=fs["name"],
            features=tuple(
                _feature_ir_from_featureset_entry(fs["name"], f)
                for f in fs["features"]
            ),
            extractors=tuple(_extractor_ir(e) for e in fs.get("extractors", [])),
        )
        for _, fs in sorted(fs_registry.items())
    )

    datasets = tuple(
        DatasetIR(
            name=ds["name"],
            fields=tuple(
                _feature_ir_from_dataset_field(ds["name"], f) for f in ds["fields"]
            ),
        )
        for _, ds in sorted(ds_registry.items())
    )

    return CodegenIR(featuresets=featuresets, datasets=datasets)
