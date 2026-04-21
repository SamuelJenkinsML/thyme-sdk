"""ThymeResult — universal wrapper for query results.

Internal storage is a Polars DataFrame. Convenience methods convert to
pandas, Arrow, dicts, or numpy on demand via lazy imports.
"""

from __future__ import annotations

from typing import Any

import polars as pl


class ThymeResult:
    """Wraps feature query results with multi-format export.

    Args:
        df: The underlying Polars DataFrame.
        metadata: Optional dict with context (mode, entity_type, etc.).
        query_run_id: Server-assigned identifier for the query run, surfaced
            via the ``X-Query-Run-Id`` response header. ``None`` when the
            backend didn't include the header (older query-server) or the
            call didn't produce a query run (e.g. ``inspect``).
    """

    def __init__(
        self,
        df: pl.DataFrame,
        metadata: dict[str, Any] | None = None,
        query_run_id: str | None = None,
    ):
        self._df = df
        self._metadata = metadata or {}
        self._query_run_id = query_run_id

    @property
    def metadata(self) -> dict[str, Any]:
        return self._metadata

    @property
    def query_run_id(self) -> str | None:
        return self._query_run_id

    def to_polars(self) -> pl.DataFrame:
        """Return the underlying Polars DataFrame (zero-copy)."""
        return self._df

    def to_pandas(self) -> Any:
        """Convert to a pandas DataFrame (lazy-imports pandas)."""
        try:
            import pandas  # noqa: F401
        except ImportError:
            raise ImportError(
                "pandas is required for .to_pandas(). "
                "Install it with: pip install thyme-sdk[pandas]"
            )
        return self._df.to_pandas()

    def to_dict(self) -> list[dict[str, Any]]:
        """Convert to a list of dicts (one per row)."""
        return self._df.to_dicts()

    def to_arrow(self) -> Any:
        """Convert to a PyArrow Table (lazy-imports pyarrow)."""
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            raise ImportError(
                "pyarrow is required for .to_arrow(). "
                "Install it with: pip install thyme-sdk[arrow]"
            )
        return self._df.to_arrow()

    def __len__(self) -> int:
        return len(self._df)

    def __getitem__(self, key: str) -> pl.Series:
        return self._df[key]

    def __repr__(self) -> str:
        return f"ThymeResult({len(self._df)} rows)\n{self._df}"
