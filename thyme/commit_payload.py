"""Build API-shaped commit payload (Rust definition service JSON). No protobuf dependency."""

from dataclasses import asdict, dataclass
from typing import Any, List, Optional


@dataclass
class PyCodeDef:
    """Matches Rust PyCodeDef for API JSON."""

    entry_point: str
    source_code: str
    generated_code: str
    imports: str = ""


@dataclass
class PipelineDef:
    """Matches Rust PipelineDef for API JSON."""

    name: str
    version: int
    input_datasets: List[str]
    output_dataset: str
    operators: List[Any]
    pycode: Optional[PyCodeDef] = None


@dataclass
class FeatureDef:
    """Matches Rust FeatureDef for API JSON."""

    name: str
    dtype: str


@dataclass
class LookupInfo:
    """Routing info for an auto-generated LOOKUP-kind extractor.
    Mirrors crates/query-server/src/metadata.rs::LookupInfo."""

    dataset_name: str
    field_name: str
    default: Any = None


@dataclass
class ExtractorDef:
    """Matches Rust ExtractorDef for API JSON."""

    name: str
    inputs: List[str]
    outputs: List[str]
    deps: List[str]
    version: int = 1
    pycode: Optional[PyCodeDef] = None
    kind: str = "PY_FUNC"
    lookup_info: Optional[LookupInfo] = None


@dataclass
class FeaturesetDef:
    """Matches Rust FeaturesetDef for API JSON."""

    name: str
    features: List[FeatureDef]
    extractors: List[ExtractorDef]


def _wire_operator(op: dict) -> dict:
    """Produce the JSON-serialisable wire form of an operator dict.

    Pipeline operators may carry protobuf messages (Predicate/Derivation) that
    are not directly JSON-encodable. filter/assign ops ship a ``_wire`` key
    holding the pre-converted prost-serde-shaped body; transform ops carry
    pycode (source + entry_point) that the engine reads directly. Aggregate
    ops may carry a per-spec ``_wire_predicate`` (TH-091) — if present we
    swap it into ``predicate`` so the spec is JSON-encodable.
    """
    if "filter" in op and "_wire" in op["filter"]:
        return {"filter": op["filter"]["_wire"]}
    if "assign" in op and "_wire" in op["assign"]:
        return {"assign": op["assign"]["_wire"]}
    if "transform" in op and "_wire" in op["transform"]:
        return {"transform": op["transform"]["_wire"]}
    if "aggregate" in op:
        agg = op["aggregate"]
        specs = agg.get("specs", [])
        wire_specs = []
        for spec in specs:
            wire_spec = {
                k: v for k, v in spec.items()
                if k not in ("predicate", "_wire_predicate")
            }
            if "_wire_predicate" in spec:
                wire_spec["predicate"] = spec["_wire_predicate"]
            wire_specs.append(wire_spec)
        return {"aggregate": {**agg, "specs": wire_specs}}
    return op


def _to_pipeline(p: dict) -> PipelineDef:
    pycode = None
    if p.get("source_code"):
        pycode = PyCodeDef(
            entry_point=p["name"],
            source_code=p["source_code"],
            generated_code=p["source_code"],
        )
    return PipelineDef(
        name=p["name"],
        version=p.get("version", 1),
        input_datasets=p.get("input_datasets", []),
        output_dataset=p.get("output_dataset", ""),
        operators=[_wire_operator(op) for op in p.get("operators", [])],
        pycode=pycode,
    )


def _to_extractor(ext: dict) -> ExtractorDef:
    pycode = None
    if ext.get("source_code"):
        pycode = PyCodeDef(
            entry_point=ext["name"],
            source_code=ext["source_code"],
            generated_code=ext["source_code"],
        )
    lookup_info = None
    if ext.get("lookup_info"):
        li = ext["lookup_info"]
        lookup_info = LookupInfo(
            dataset_name=li["dataset_name"],
            field_name=li["field_name"],
            default=li.get("default"),
        )
    return ExtractorDef(
        name=ext["name"],
        inputs=ext.get("inputs", []),
        outputs=ext.get("outputs", []),
        deps=ext.get("deps", []),
        version=ext.get("version", 1),
        pycode=pycode,
        kind=ext.get("kind", "PY_FUNC"),
        lookup_info=lookup_info,
    )


def _to_featureset(fs: dict) -> FeaturesetDef:
    features = [
        FeatureDef(name=f["name"], dtype=f["dtype"])
        for f in fs.get("features", [])
    ]
    extractors = [_to_extractor(ext) for ext in fs.get("extractors", [])]
    return FeaturesetDef(name=fs["name"], features=features, extractors=extractors)


def build_commit_request_for_api(
    datasets: List[dict],
    pipelines: List[dict],
    featuresets: List[dict],
    sources: List[dict],
) -> dict:
    """Build a dict matching the definition service CommitRequest (Rust API shape)."""
    return {
        "datasets": [dict(d) for d in datasets],
        "pipelines": [asdict(_to_pipeline(p)) for p in pipelines],
        "featuresets": [asdict(_to_featureset(fs)) for fs in featuresets],
        "sources": [dict(s) for s in sources],
    }
