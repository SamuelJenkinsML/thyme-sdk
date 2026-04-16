"""Bidirectional type mapping between Thyme, Polars, and Arrow types."""

from __future__ import annotations

import polars as pl


_THYME_TO_POLARS: dict[str, pl.DataType] = {
    "int": pl.Int64,
    "float": pl.Float64,
    "str": pl.Utf8,
    "bool": pl.Boolean,
    "datetime": pl.Datetime("us", "UTC"),
}


def thyme_type_to_polars(type_str: str) -> pl.DataType:
    """Convert a Thyme type string to a Polars DataType.

    Raises ValueError for unknown types.
    """
    try:
        return _THYME_TO_POLARS[type_str]
    except KeyError:
        raise ValueError(
            f"Unknown Thyme type: '{type_str}'. "
            f"Valid types: {sorted(_THYME_TO_POLARS.keys())}"
        )


def schema_from_featureset(fs_meta: dict) -> pl.Schema:
    """Build a Polars Schema from featureset registry metadata.

    Args:
        fs_meta: A featureset dict from _FEATURESET_REGISTRY, containing
                 a "features" list with {"name", "dtype", "id"} entries.
    """
    return pl.Schema(
        {f["name"]: thyme_type_to_polars(f["dtype"]) for f in fs_meta["features"]}
    )


def schema_from_dataset(ds_meta: dict) -> pl.Schema:
    """Build a Polars Schema from dataset registry metadata.

    Args:
        ds_meta: A dataset dict from _DATASET_REGISTRY, containing
                 a "fields" list with {"name", "type"} entries.
    """
    return pl.Schema(
        {f["name"]: thyme_type_to_polars(f["type"]) for f in ds_meta["fields"]}
    )
