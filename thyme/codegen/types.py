"""Mapping from Thyme dtype strings to Python type-annotation strings.

Kept separate from `thyme.types` (which maps to Polars dtypes) because the
codegen emitter needs string annotations (e.g. ``"datetime.datetime"``) rather
than live Polars objects.
"""

from __future__ import annotations


_THYME_TO_PY_ANNOTATION: dict[str, str] = {
    "int": "int",
    "float": "float",
    "str": "str",
    "bool": "bool",
    "datetime": "datetime.datetime",
}


def thyme_type_to_python_annotation(dtype: str) -> str:
    """Convert a Thyme dtype string to a Python type-annotation string.

    Raises:
        ValueError: When the dtype is not a supported scalar.
    """
    try:
        return _THYME_TO_PY_ANNOTATION[dtype]
    except KeyError:
        raise ValueError(
            f"Unknown Thyme dtype: '{dtype}'. "
            f"Valid dtypes: {sorted(_THYME_TO_PY_ANNOTATION.keys())}"
        )


def annotation_requires_datetime_import(annotation: str) -> bool:
    """Return True if the given annotation references the ``datetime`` module."""
    return annotation.startswith("datetime.")
