import inspect
import textwrap
from dataclasses import asdict
from typing import Any, Callable, List, Optional, TypeVar, overload

from thyme.dataset import Field, _is_optional, _type_to_string
from thyme.metadata import EntityMetadata, _build_metadata


T = TypeVar("T")


# Extractor kinds — must match crates/query-server/src/metadata.rs ExtractorKind
# and the proto enum in proto/thyme/featureset.proto.
EXTRACTOR_KIND_PY_FUNC = "PY_FUNC"
EXTRACTOR_KIND_LOOKUP = "LOOKUP"


class FeatureDescriptor:
    """Descriptor for a feature in a featureset.

    Two flavours:
    - ``feature()`` / ``feature(ref=Dataset.field, default=...)`` — declares
      where the value comes from. ``ref`` triggers the @featureset decorator
      to synthesize an auto-generated LOOKUP extractor for the feature.
    - Bare ``feature()`` — value is produced by a user-defined PY_FUNC
      extractor (or is the featureset's entity ID).
    """

    def __init__(
        self,
        ref: Field | None = None,
        default: Any = None,
        dtype: Optional[str] = None,
    ):
        self.ref = ref
        self.default = default
        self.dtype = dtype


class FeatureRef:
    """Class-level reference to a feature on a featureset.

    After ``@featureset`` decorates a class, each declared feature is exposed as
    a ``FeatureRef`` class attribute, so ``Featureset.feature`` resolves to a
    ``FeatureRef`` carrying the featureset + feature name. This mirrors how
    ``@dataset`` leaves :class:`~thyme.dataset.Field` descriptors on the class so
    ``Dataset.field`` works.

    Consumed by :func:`extractor_inputs` to declare a cross-featureset input
    (``@extractor_inputs(SessionFeatures.intent_score)``), where it is
    normalized to the fully-qualified string ``"SessionFeatures.intent_score"``.
    """

    def __init__(self, name: str, featureset_name: str, dtype: Optional[str] = None):
        self.name = name
        self.featureset_name = featureset_name
        self.dtype = dtype

    def fqn(self) -> str:
        return f"{self.featureset_name}.{self.name}"

    def __repr__(self) -> str:
        return f"<FeatureRef {self.fqn()}>"


@overload
def feature(*, ref: Field[T], default: T | None = None) -> T: ...
@overload
def feature() -> Any: ...
def feature(
    *,
    ref: Field | None = None,
    default: Any = None,
) -> Any:
    """Declare a feature on a featureset.

    Args:
        ref: A dataset field reference (e.g. ``UserProfile.loyalty_tier``).
            When set, the SDK auto-generates a LOOKUP-kind extractor that
            pulls this field from the named dataset at query time. No body
            required.
        default: Value to return when the dataset lookup misses (the entity
            has no row in the dataset). Only meaningful with ``ref``.
    """
    return FeatureDescriptor(ref=ref, default=default)


class Extractor:
    """Metadata for an extractor method on a featureset."""

    def __init__(
        self,
        func: Callable,
        deps: List[str],
        input_features: List[str],
        output_features: List[str],
        version: int = 1,
    ):
        self.func = func
        self.name = func.__name__
        self.deps = deps
        self.input_features = input_features
        self.output_features = output_features
        self.version = version
        self.source_code = textwrap.dedent(inspect.getsource(func))


_FEATURESET_REGISTRY: dict[str, dict] = {}


def clear_featureset_registry() -> None:
    _FEATURESET_REGISTRY.clear()


def get_registered_featuresets() -> dict[str, dict]:
    from copy import deepcopy
    return deepcopy(_FEATURESET_REGISTRY)


def _literal_for_default(value: Any) -> Any:
    """Pass-through serialization for default values. Stored in the LOOKUP
    extractor's ``lookup_info.default`` and round-tripped through JSON to the
    Rust planner. Accepts the four scalar shapes the proto Literal supports:
    str, int, float, bool. Other shapes flow through unchanged for now and
    are rejected at commit time on the Rust side if unsupported."""
    return value


def featureset(cls=None, /, **kwargs):
    """Decorator to register a class as a featureset.

    Supports both `@featureset` (bare) and `@featureset(owner=..., tags=...)`.
    Catalog metadata kwargs (`description`, `owner`, `tags`, `project`,
    `deprecated`, `deprecation_reason`, `replacement`) are stashed on the
    class as `__thyme_metadata__`. Unknown kwargs trigger a `FutureWarning`
    so older SDKs don't break on newer commit-author kwargs."""
    metadata = _build_metadata("featureset", kwargs)

    def _apply(cls: type) -> type:
        return _register_featureset(cls, metadata)

    if cls is not None:
        return _apply(cls)
    return _apply


def _register_featureset(cls: type, metadata: EntityMetadata) -> type:
    features = []
    extractors = []

    annotations = getattr(cls, "__annotations__", {})

    # Cache (name → FeatureDescriptor) so the LOOKUP synthesis pass below can
    # match output features back to their declared `ref` / `default`.
    feature_descriptors: dict[str, FeatureDescriptor] = {}

    for attr_name, attr_type in annotations.items():
        default = getattr(cls, attr_name, None)
        if isinstance(default, FeatureDescriptor):
            type_name = _type_to_string(attr_type)
            default.dtype = type_name
            entry: dict[str, Any] = {
                "name": attr_name,
                "dtype": type_name,
            }
            if _is_optional(attr_type):
                entry["optional"] = True
            features.append(entry)
            feature_descriptors[attr_name] = default

    # Synthesize LOOKUP-kind extractors for every feature with a `ref`. These
    # are auto-generated at decoration time — no source body, no inspect.
    for attr_name, desc in feature_descriptors.items():
        if desc.ref is None:
            continue
        if not isinstance(desc.ref, Field):
            raise TypeError(
                f"feature(ref=...) for '{cls.__name__}.{attr_name}' expects a "
                f"dataset Field reference (e.g. UserProfile.loyalty_tier), got "
                f"{type(desc.ref).__name__}"
            )
        if desc.ref.dataset_name is None or desc.ref.name is None:
            raise RuntimeError(
                f"Field reference for '{cls.__name__}.{attr_name}' is unbound — "
                f"the parent dataset must be decorated with @dataset before the "
                f"featureset is defined."
            )
        lookup_info: dict[str, Any] = {
            "dataset_name": desc.ref.dataset_name,
            "field_name": desc.ref.name,
        }
        if desc.default is not None:
            lookup_info["default"] = _literal_for_default(desc.default)
        extractors.append({
            "name": f"_thyme_lookup_{attr_name}",
            "deps": [desc.ref.dataset_name],
            "inputs": [],
            "outputs": [attr_name],
            "version": 1,
            "kind": EXTRACTOR_KIND_LOOKUP,
            "lookup_info": lookup_info,
            # No source_code — LOOKUP extractors carry no Python body.
        })

    # User-defined PY_FUNC extractors. Source captured for these only.
    for attr_name in dir(cls):
        attr = getattr(cls, attr_name, None)
        if callable(attr) and hasattr(attr, "_is_extractor"):
            deps = getattr(attr, "_extractor_deps", [])
            dep_names = [
                d.__name__ if hasattr(d, "__name__") else str(d)
                for d in deps
            ]
            input_features = getattr(attr, "_extractor_inputs", [])
            output_features = getattr(attr, "_extractor_outputs", [])
            version = getattr(attr, "_extractor_version", 1)

            extractors.append({
                "name": attr_name,
                "deps": dep_names,
                "inputs": input_features,
                "outputs": output_features,
                "version": version,
                "kind": EXTRACTOR_KIND_PY_FUNC,
                "source_code": textwrap.dedent(inspect.getsource(attr)),
            })

    # Validate extractor input/output feature references
    feature_names = {f["name"] for f in features}
    for ext in extractors:
        for inp in ext["inputs"]:
            # Cross-featureset inputs are fully-qualified (`Featureset.feature`)
            # and validated at commit time by the definition-service against the
            # full set of known featuresets — skip the local declaration check.
            if "." in inp:
                continue
            if inp not in feature_names:
                raise ValueError(
                    f"Extractor '{ext['name']}' in featureset '{cls.__name__}' references "
                    f"input feature '{inp}' which is not declared in the featureset. "
                    f"Declared features: {sorted(feature_names)}"
                )
        for out in ext["outputs"]:
            if out not in feature_names:
                raise ValueError(
                    f"Extractor '{ext['name']}' in featureset '{cls.__name__}' references "
                    f"output feature '{out}' which is not declared in the featureset. "
                    f"Declared features: {sorted(feature_names)}"
                )

    schema = {
        "name": cls.__name__,
        "features": features,
        "extractors": extractors,
        "metadata": asdict(metadata),
    }
    _FEATURESET_REGISTRY[cls.__name__] = schema
    cls._featureset_meta = schema
    cls.__thyme_metadata__ = metadata

    # Expose each declared feature as a FeatureRef class attribute so another
    # featureset can reference it cross-featureset via `cls.feature` in
    # @extractor_inputs. Done last so it overrides the FeatureDescriptor default
    # only after the descriptor has been read for feature/extractor synthesis.
    for feat in features:
        feat_name = feat["name"]
        setattr(
            cls,
            feat_name,
            FeatureRef(feat_name, cls.__name__, feat.get("dtype")),
        )
    return cls


def extractor(func: Optional[Callable] = None, *, deps: Optional[List[Any]] = None, version: int = 1):
    """Decorator for extractor methods. Can be used with or without arguments."""
    if func is not None and callable(func):
        func._is_extractor = True
        func._extractor_deps = []
        func._extractor_version = 1
        func._extractor_inputs = getattr(func, "_extractor_inputs", [])
        func._extractor_outputs = getattr(func, "_extractor_outputs", [])
        return func

    def wrapper(f: Callable) -> Callable:
        f._is_extractor = True
        f._extractor_deps = deps or []
        f._extractor_version = version
        f._extractor_inputs = getattr(f, "_extractor_inputs", [])
        f._extractor_outputs = getattr(f, "_extractor_outputs", [])
        return f

    return wrapper


def extractor_inputs(*inputs: "str | FeatureRef") -> Callable:
    """Decorator to specify input features for an extractor.

    Each input is either a bare local feature name (``"score"``) or a
    cross-featureset object reference (``SessionFeatures.intent_score``). Object
    references are normalized to their fully-qualified ``"Featureset.feature"``
    string; the declared order is preserved (it is the positional param-binding
    contract for the extractor function).
    """

    def wrapper(func: Callable) -> Callable:
        normalized: List[str] = []
        for inp in inputs:
            if isinstance(inp, FeatureRef):
                normalized.append(inp.fqn())
            elif isinstance(inp, str):
                normalized.append(inp)
            elif isinstance(inp, FeatureDescriptor):
                # `Featureset.feature` on a class that hasn't been decorated yet
                # resolves to the raw FeatureDescriptor default, not a
                # FeatureRef. Point the user at decoration order.
                raise RuntimeError(
                    "extractor_inputs received an unbound feature reference. "
                    "Decorate the producer featureset with @featureset before "
                    "referencing its features as a cross-featureset input."
                )
            else:
                raise TypeError(
                    "extractor_inputs accepts feature names (str) or "
                    "Featureset.feature references, got "
                    f"{type(inp).__name__}"
                )
        func._extractor_inputs = normalized
        return func

    return wrapper


def extractor_outputs(*feature_names: str) -> Callable:
    """Decorator to specify output features for an extractor."""

    def wrapper(func: Callable) -> Callable:
        func._extractor_outputs = list(feature_names)
        return func

    return wrapper
