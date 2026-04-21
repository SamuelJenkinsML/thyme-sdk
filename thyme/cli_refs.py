"""Helpers for CLI reference resolution and entity collection.

Extracted from thyme/cli.py so the resolver logic is testable without the
Typer command machinery.
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Literal

import polars as pl


def resolve_ref(
    ref: str,
    *,
    module_path: Path | None = None,
    expect: Literal["featureset", "dataset", "any"] = "featureset",
) -> type:
    """Resolve a ``module:ClassName`` reference to a Python class.

    When ``module_path`` is provided, load the module from that file path
    instead of a dotted import. This mirrors how ``thyme commit`` handles
    either ``-m pkg.mod`` or a positional file path.

    Args:
        ref: The reference string (must contain a single ``:``).
        module_path: Optional file path; when set, ``ref`` still carries the
            class name after the colon but the module on the left is ignored
            in favour of the file.
        expect: Validate that the resolved class carries the matching
            decorator metadata: ``_featureset_meta`` or ``_dataset_meta``.
            Pass ``"any"`` to skip validation.

    Raises:
        ValueError: When ``ref`` has no colon or the class lacks the expected
            decorator.
        AttributeError: When the module doesn't expose the requested class.
        FileNotFoundError: When ``module_path`` points at a missing file.
    """
    if ":" not in ref:
        raise ValueError(
            f"Invalid reference '{ref}' — expected 'module:Class' (e.g. 'myproject.features:UserFeatures')"
        )

    module_part, _, class_part = ref.partition(":")
    if not class_part:
        raise ValueError(f"Invalid reference '{ref}' — class name after ':' is empty")

    if module_path is not None:
        module = _import_from_path(Path(module_path))
    else:
        module = importlib.import_module(module_part)

    cls = getattr(module, class_part)

    if expect == "featureset" and getattr(cls, "_featureset_meta", None) is None:
        raise ValueError(
            f"'{class_part}' is not a registered featureset. "
            f"Did you decorate it with @featureset?"
        )
    if expect == "dataset" and getattr(cls, "_dataset_meta", None) is None:
        raise ValueError(
            f"'{class_part}' is not a registered dataset. "
            f"Did you decorate it with @dataset?"
        )

    return cls


def _import_from_path(file_path: Path):
    import importlib.util

    resolved = file_path.resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"Module file not found: {file_path}")

    parent = str(resolved.parent)
    if parent not in sys.path:
        sys.path.insert(0, parent)

    module_name = resolved.stem
    spec = importlib.util.spec_from_file_location(module_name, resolved)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module from {file_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def collect_entities(values: list[str]) -> list[str]:
    """Flatten repeated ``-e`` values into a single list.

    Accepts three forms per input value:

    - Plain: ``"user_1"``
    - Comma-split: ``"user_1,user_2,user_3"``
    - File reference: ``"@path/to/ids.txt"`` (newline-delimited, blank lines ignored)
    """
    out: list[str] = []
    for value in values:
        if value.startswith("@"):
            path = Path(value[1:])
            if not path.exists():
                raise FileNotFoundError(f"Entity file not found: {path}")
            for line in path.read_text().splitlines():
                stripped = line.strip()
                if stripped:
                    out.append(stripped)
        elif "," in value:
            out.extend(part.strip() for part in value.split(",") if part.strip())
        else:
            out.append(value)
    return out


_SUPPORTED_SUFFIXES = {".parquet", ".csv", ".tsv", ".jsonl", ".json", ".ndjson"}


def read_entities_dataframe(path: Path) -> pl.DataFrame:
    """Read an entities+timestamps file into a Polars DataFrame.

    Format is inferred from the file extension. Supports Parquet, CSV, TSV,
    and newline-delimited JSON. Unknown extensions raise ``ValueError``.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pl.read_parquet(path)
    if suffix == ".csv":
        return pl.read_csv(path)
    if suffix == ".tsv":
        return pl.read_csv(path, separator="\t")
    if suffix in {".jsonl", ".ndjson"}:
        return pl.read_ndjson(path)
    if suffix == ".json":
        return pl.read_json(path)
    raise ValueError(
        f"unsupported file extension '{suffix}' for {path}; "
        f"expected one of {sorted(_SUPPORTED_SUFFIXES)}"
    )
