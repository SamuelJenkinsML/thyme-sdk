import inspect
import textwrap
from typing import Any, Callable, List, Optional

from thyme.dataset import _is_optional, _type_to_string


class FeatureDescriptor:
    """Descriptor for a feature in a featureset."""

    def __init__(self, id: int, dtype: Optional[str] = None):
        self.id = id
        self.dtype = dtype


def feature(id: int) -> Any:
    """Create a feature descriptor with an integer ID."""
    return FeatureDescriptor(id=id)


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


def featureset(cls: type) -> type:
    """Decorator to register a class as a featureset."""
    features = []
    extractors = []

    annotations = getattr(cls, "__annotations__", {})

    for attr_name, attr_type in annotations.items():
        default = getattr(cls, attr_name, None)
        if isinstance(default, FeatureDescriptor):
            # Mirror dataset.py's nullable handling: PEP-604 unions like
            # `int | None` and `Optional[int]` get unwrapped to the base
            # dtype, with an `optional` flag set on the schema entry so
            # downstream stages (codegen IR, proto compiler) can re-attach
            # the nullable wrapper.
            type_name = _type_to_string(attr_type)
            default.dtype = type_name
            entry: dict[str, Any] = {
                "name": attr_name,
                "dtype": type_name,
                "id": default.id,
            }
            if _is_optional(attr_type):
                entry["optional"] = True
            features.append(entry)

    # Validate feature IDs are positive and unique
    seen_ids = {}
    for f in features:
        fid = f["id"]
        if not isinstance(fid, int) or fid <= 0:
            raise ValueError(
                f"Feature '{f['name']}' in featureset '{cls.__name__}' has invalid id={fid}; "
                f"feature IDs must be positive integers (> 0)"
            )
        if fid in seen_ids:
            raise ValueError(
                f"Duplicate feature id={fid} in featureset '{cls.__name__}': "
                f"used by both '{seen_ids[fid]}' and '{f['name']}'"
            )
        seen_ids[fid] = f["name"]

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
                "source_code": textwrap.dedent(inspect.getsource(attr)),
            })

    # Validate extractor input/output feature references
    feature_names = {f["name"] for f in features}
    for ext in extractors:
        for inp in ext["inputs"]:
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
    }
    _FEATURESET_REGISTRY[cls.__name__] = schema
    cls._featureset_meta = schema
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


def extractor_inputs(*feature_names: str) -> Callable:
    """Decorator to specify input features for an extractor."""

    def wrapper(func: Callable) -> Callable:
        func._extractor_inputs = list(feature_names)
        return func

    return wrapper


def extractor_outputs(*feature_names: str) -> Callable:
    """Decorator to specify output features for an extractor."""

    def wrapper(func: Callable) -> Callable:
        func._extractor_outputs = list(feature_names)
        return func

    return wrapper
