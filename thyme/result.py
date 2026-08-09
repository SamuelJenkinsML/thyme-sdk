"""ThymeResult — universal wrapper for query results.

Backs onto either an eager Polars DataFrame (every online query) or a **lazy**
source (a DuckDB relation, from an offline pull over the Iceberg store). A
training pull can be tens of millions of rows, so the lazy form must be able to
stream to Parquet or hand back Arrow batches without the whole frame ever
existing in memory.

One return type rather than several, and the caller picks materialisation.
Arrow is the interchange throughout, so `.to_polars()` and `.to_arrow()` are
cheap and nobody has to guess which frame library the user wants.
"""

from __future__ import annotations

import os
from typing import Any, Generic, Iterator, TypeVar

import polars as pl

_T = TypeVar("_T")

#: Rows per batch when streaming. Large enough that per-batch overhead is
#: irrelevant, small enough that a batch is comfortably sized in memory.
DEFAULT_BATCH_SIZE = 100_000


class ThymeResult(Generic[_T]):
    """Wraps feature query results with multi-format export.

    Args:
        source: Either a Polars DataFrame (eager, the online path) or a DuckDB
            relation (lazy, the offline path).
        metadata: Optional dict with context (mode, entity_type, etc.).
        query_run_id: Server-assigned identifier for the query run, surfaced
            via the ``X-Query-Run-Id`` response header. ``None`` when the
            backend didn't include the header (older query-server) or the
            call didn't produce a query run (e.g. ``inspect``).
    """

    def __init__(
        self,
        source: Any,
        metadata: dict[str, Any] | None = None,
        query_run_id: str | None = None,
        connection: Any | None = None,
    ):
        self._metadata = metadata or {}
        self._query_run_id = query_run_id
        # A DuckDB relation is only valid while its connection lives, and
        # `query_offline` opens the connection inside the call. Without this
        # reference the connection becomes garbage the moment the function
        # returns, and the result is dead on arrival.
        self._connection = connection

        if isinstance(source, pl.DataFrame):
            self._relation = None
            self._materialised: pl.DataFrame | None = source
        else:
            # A DuckDB relation. Not materialised until someone asks for a
            # frame; `.sink_parquet()` and `.iter_batches()` never do.
            self._relation = source
            self._materialised = None

    @property
    def metadata(self) -> dict[str, Any]:
        return self._metadata

    @property
    def query_run_id(self) -> str | None:
        return self._query_run_id

    @property
    def is_lazy(self) -> bool:
        """Whether this result is backed by a lazy source rather than a frame."""
        return self._relation is not None

    @property
    def columns(self) -> list[str]:
        """Column names, without materialising a lazy result."""
        if self._materialised is not None:
            return self._materialised.columns
        return list(self._relation.columns)

    @property
    def _df(self) -> pl.DataFrame:
        """The frame, materialising the lazy source once and caching it.

        Cached because a training pull is expensive enough that running the scan
        twice by accident is a real cost, not just a slowdown.
        """
        if self._materialised is None:
            self._materialised = self._relation.pl()
        return self._materialised

    def to_polars(self) -> pl.DataFrame:
        """Return the result as a Polars DataFrame, materialising if lazy."""
        return self._df

    def to_lazy(self) -> pl.LazyFrame:
        """Return a Polars LazyFrame, so further work stays unevaluated."""
        if self._materialised is not None:
            return self._materialised.lazy()
        return self._relation.pl().lazy()

    def to_duckdb(self) -> Any:
        """The underlying DuckDB relation, for callers who want to keep going in SQL.

        Raises for an eager result: there is no relation to hand back, and
        silently building one would hide the cost.
        """
        if self._relation is None:
            raise TypeError(
                "this result is already materialised; .to_duckdb() is only "
                "available for a lazy offline pull."
            )
        return self._relation

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
        if self._materialised is not None:
            return self._materialised.to_arrow()
        # `.arrow()` returns a RecordBatchReader on current DuckDB, not a Table.
        return self._relation.to_arrow_table()

    def iter_batches(self, batch_size: int = DEFAULT_BATCH_SIZE) -> Iterator[Any]:
        """Yield Arrow record batches, bounding memory to one batch at a time.

        The streaming counterpart to :meth:`to_arrow`. For a lazy result nothing
        is cached, so a 20M-row pull never lands in memory whole.
        """
        if self._materialised is not None:
            yield from self._materialised.to_arrow().to_batches(max_chunksize=batch_size)
            return
        reader = self._relation.to_arrow_reader(batch_size)
        for batch in reader:
            yield batch

    def sink_parquet(self, path: str | os.PathLike) -> None:
        """Write to Parquet without materialising.

        This is the path a large training pull should take: DuckDB streams
        straight to the file, so peak memory is a batch rather than the result.
        """
        if self._relation is not None:
            self._relation.write_parquet(str(path))
            return
        self._materialised.write_parquet(str(path))

    def __len__(self) -> int:
        if self._materialised is not None:
            return len(self._materialised)
        # A count aggregate rather than pulling every column to measure height.
        return int(self._relation.aggregate("count(*) AS n").fetchone()[0])

    def __getitem__(self, key: str) -> pl.Series:
        return self._df[key]

    def __repr__(self) -> str:
        if self._materialised is None:
            # Deliberately does not count rows or fetch data: `repr()` in a
            # notebook cell is the easy way to pull 20M rows by accident.
            return f"ThymeResult(lazy, columns={self.columns})"
        return f"ThymeResult({len(self._materialised)} rows)\n{self._materialised}"
