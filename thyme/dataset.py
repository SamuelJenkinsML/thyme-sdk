import inspect
import json
from copy import deepcopy
from dataclasses import dataclass, fields
from typing import Any, get_origin, get_args, Union


class Field:
    """Descriptor for dataset fields. Tracks key and timestamp metadata."""

    def __init__(self, key: bool = False, timestamp: bool = False):
        self.key = key
        self.timestamp = timestamp


def field(key: bool = False, timestamp: bool = False) -> Field:
    return Field(key=key, timestamp=timestamp)


def _type_to_string(annotation: Any) -> str:
    """Map Python type annotation to string representation."""
    if annotation is None:
        return "None"
    # Handle Optional[X] / Union[X, None] -> unwrap to X for display
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin is Union:
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return _type_to_string(non_none[0])
    # Built-in and simple types
    if hasattr(annotation, "__name__"):
        return annotation.__name__
    return str(annotation)


def _is_optional(annotation: Any) -> bool:
    """Check if annotation is Optional[X] (Union[X, None])."""
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin is Union:
        return type(None) in args
    return False


def _validate_dataset_fields(cls: type) -> None:
    """Validate field constraints: exactly one key, exactly one timestamp, key not Optional."""
    dataclass_fields = fields(cls)
    key_fields = []
    timestamp_fields = []

    for f in dataclass_fields:
        default = f.default
        if isinstance(default, Field):
            if default.key:
                key_fields.append((f.name, f.type))
            if default.timestamp:
                timestamp_fields.append(f.name)

    if len(key_fields) == 0:
        raise ValueError("Dataset must have exactly one key field")
    if len(key_fields) > 1:
        raise ValueError(
            f"Dataset must have exactly one key field, found: {[k for k, _ in key_fields]}"
        )
    if len(timestamp_fields) == 0:
        raise ValueError("Dataset must have exactly one timestamp field")
    if len(timestamp_fields) > 1:
        raise ValueError(
            f"Dataset must have exactly one timestamp field, found: {timestamp_fields}"
        )

    key_field_name, key_field_type = key_fields[0]
    if _is_optional(key_field_type):
        raise ValueError(
            f"Key field '{key_field_name}' cannot be Optional"
        )


_DATASET_REGISTRY: dict[str, dict] = {}
_PIPELINE_REGISTRY: dict[tuple[str, str], dict] = {}


def _build_schema(cls: type, index: bool, version: int) -> dict:
    """Build full schema from class annotations and field descriptors."""
    schema_fields = []
    dataclass_fields = fields(cls)

    for f in dataclass_fields:
        default = f.default
        key = False
        timestamp = False
        if isinstance(default, Field):
            key = default.key
            timestamp = default.timestamp

        type_str = _type_to_string(f.type)
        field_schema: dict[str, Any] = {"name": f.name, "type": type_str}
        if key:
            field_schema["key"] = True
        if timestamp:
            field_schema["timestamp"] = True
        if _is_optional(f.type):
            field_schema["optional"] = True
        schema_fields.append(field_schema)

    return {
        "name": cls.__name__,
        "version": version,
        "index": index,
        "fields": schema_fields,
        "dependencies": [],
        "expectations": [],
    }


def _discover_expectations(cls: type) -> None:
    """Scan a dataset class for @expectations methods and store specs in the registry."""
    for name in dir(cls):
        if name.startswith("_"):
            continue
        try:
            method = getattr(cls, name)
        except AttributeError:
            continue
        if not callable(method) or not getattr(method, "_is_expectations", False):
            continue
        specs = method(cls)
        if isinstance(specs, list):
            _DATASET_REGISTRY[cls.__name__]["expectations"] = specs


def _discover_pipelines(cls: type) -> None:
    """Scan a dataset class for @pipeline methods and register them in _PIPELINE_REGISTRY."""
    from thyme.pipeline import Pipeline

    for name in dir(cls):
        if name.startswith("_"):
            continue
        try:
            method = getattr(cls, name)
        except AttributeError:
            continue
        if not callable(method) or not getattr(method, "_is_pipeline", False):
            continue
        version = getattr(method, "_pipeline_version", 1)
        input_datasets = getattr(method, "_pipeline_inputs", [])
        try:
            pl = Pipeline(method, version, input_datasets)
            operators = pl.get_operators()
        except Exception:
            operators = []
        try:
            source_code = inspect.getsource(method)
        except OSError:
            source_code = ""
        pipeline_meta = {
            "name": method.__name__,
            "version": version,
            "input_datasets": input_datasets,
            "output_dataset": cls.__name__,
            "operators": operators,
            "source_code": source_code,
        }
        key = (cls.__name__, method.__name__)
        _PIPELINE_REGISTRY[key] = pipeline_meta


def dataset(index: bool = False, version: int = 1):
    """Decorator to register a class as a dataset with schema metadata."""

    def wrapper(cls):
        # Apply dataclass first so we can use fields()
        cls = dataclass(cls)
        _validate_dataset_fields(cls)
        schema = _build_schema(cls, index=index, version=version)
        _DATASET_REGISTRY[cls.__name__] = schema
        cls._dataset_meta = schema
        _discover_expectations(cls)
        _discover_pipelines(cls)
        return cls

    return wrapper


def get_registered_datasets() -> dict[str, dict]:
    """Return a deep copy of the dataset registry."""
    return deepcopy(_DATASET_REGISTRY)


def get_registered_pipelines() -> list[dict]:
    """Return list of pipeline metadata dicts (output_dataset, name, operators, etc.)."""
    return list(deepcopy(_PIPELINE_REGISTRY).values())


def clear_registry() -> None:
    """Clear all registries used for commit (datasets, pipelines, featuresets, sources)."""
    _DATASET_REGISTRY.clear()
    _PIPELINE_REGISTRY.clear()
    from thyme.featureset import clear_featureset_registry
    from thyme.connectors import clear_source_registry

    clear_featureset_registry()
    clear_source_registry()


def serialize_definitions() -> str:
    """Return JSON-serialized representation of all registered datasets."""
    return json.dumps(get_registered_datasets(), indent=2)


def get_commit_payload() -> dict:
    """Return full commit payload for control plane: datasets, pipelines, featuresets, sources (API-shaped)."""
    from thyme.commit_payload import build_commit_request_for_api
    from thyme.featureset import get_registered_featuresets
    from thyme.connectors import get_registered_sources

    return build_commit_request_for_api(
        list(get_registered_datasets().values()),
        get_registered_pipelines(),
        list(get_registered_featuresets().values()),
        list(get_registered_sources().values()),
    )
