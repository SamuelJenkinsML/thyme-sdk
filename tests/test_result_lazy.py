"""ThymeResult over a lazy source (TH-312).

A training pull can be tens of millions of rows, so the result must be able to
stream to Parquet or hand back batches without ever holding the whole frame in
memory. The eager constructor stays exactly as it was — every existing caller
passes a `pl.DataFrame` and calls `.to_polars()`.

The subtle requirement is that *inspecting* a lazy result must not silently
materialise it. `repr()` in a notebook cell is the obvious way to accidentally
pull 20M rows into memory.
"""

import gc

import duckdb
import polars as pl
import pytest

from thyme.result import ThymeResult


@pytest.fixture
def con():
    connection = duckdb.connect()
    connection.execute(
        "CREATE TABLE t AS SELECT * FROM (VALUES "
        "('c123', 7.0), ('c456', 9.0), ('c789', 42.0)) AS v(entity_id, score)"
    )
    yield connection
    connection.close()


@pytest.fixture
def relation(con):
    return con.sql("SELECT * FROM t ORDER BY entity_id")


class TestEagerIsUnchanged:
    """Every existing caller constructs with a DataFrame."""

    def test_dataframe_constructor_still_works(self):
        df = pl.DataFrame({"entity_id": ["c123"], "score": [7.0]})
        result = ThymeResult(df)

        assert result.to_polars().equals(df)
        assert len(result) == 1
        assert result["score"][0] == 7.0
        assert result.to_dict() == [{"entity_id": "c123", "score": 7.0}]

    def test_eager_result_is_not_lazy(self):
        result = ThymeResult(pl.DataFrame({"a": [1]}))
        assert result.is_lazy is False


class TestLazyMaterialisation:
    def test_to_polars_materialises(self, relation):
        result = ThymeResult(relation)

        df = result.to_polars()

        assert isinstance(df, pl.DataFrame)
        assert df["entity_id"].to_list() == ["c123", "c456", "c789"]

    def test_is_lazy_reports_the_source_kind(self, relation):
        assert ThymeResult(relation).is_lazy is True

    def test_to_lazy_returns_a_polars_lazyframe(self, relation):
        lf = ThymeResult(relation).to_lazy()

        assert isinstance(lf, pl.LazyFrame)
        assert lf.collect()["score"].to_list() == [7.0, 9.0, 42.0]

    def test_materialising_twice_reuses_the_first_result(self, relation):
        # given a lazy result read once
        result = ThymeResult(relation)
        first = result.to_polars()

        # when read again
        second = result.to_polars()

        # then the scan is not repeated -- a training pull is expensive enough
        # that accidentally running it twice is a real cost
        assert first is second

    def test_to_arrow_returns_a_table(self, relation):
        table = ThymeResult(relation).to_arrow()

        assert table.num_rows == 3
        assert set(table.column_names) == {"entity_id", "score"}


class TestStreaming:
    """The paths that must never hold the whole frame."""

    def test_iter_batches_yields_record_batches(self, relation):
        batches = list(ThymeResult(relation).iter_batches(batch_size=2))

        assert len(batches) >= 1
        assert sum(b.num_rows for b in batches) == 3

    def test_iter_batches_does_not_materialise_the_frame(self, relation):
        # given a lazy result
        result = ThymeResult(relation)

        # when streamed
        list(result.iter_batches(batch_size=1))

        # then nothing was cached -- streaming exists precisely to avoid the
        # frame ever existing in memory
        assert result._materialised is None

    def test_sink_parquet_writes_without_materialising(self, relation, tmp_path):
        # given a lazy result
        result = ThymeResult(relation)
        out = tmp_path / "features.parquet"

        # when written
        result.sink_parquet(out)

        # then the file is correct and the frame was never held
        assert out.exists()
        assert pl.read_parquet(out)["score"].to_list() == [7.0, 9.0, 42.0]
        assert result._materialised is None

    def test_sink_parquet_works_for_eager_results_too(self, tmp_path):
        # given an eager result, so callers need not care which they hold
        result = ThymeResult(pl.DataFrame({"a": [1, 2]}))
        out = tmp_path / "eager.parquet"

        result.sink_parquet(out)

        assert pl.read_parquet(out)["a"].to_list() == [1, 2]


class TestConnectionLifetime:
    """A relation is only valid while its connection lives.

    `query_offline` opens the connection inside the call, so without an explicit
    reference it becomes garbage the moment the function returns and the result
    is dead on arrival.
    """

    def test_result_keeps_its_connection_alive(self):
        # given a result built the way query_offline builds one, with no local
        # reference to the connection surviving the call
        def build() -> ThymeResult:
            connection = duckdb.connect()
            connection.execute("CREATE TABLE t AS SELECT 1 AS a, 2.0 AS b")
            return ThymeResult(connection.sql("SELECT * FROM t"), connection=connection)

        result = build()
        gc.collect()

        # then it is still readable after the builder's frame is gone
        assert result.to_polars()["a"].to_list() == [1]


class TestInspectionDoesNotMaterialise:
    """`repr()` in a notebook is the easy way to pull 20M rows by accident."""

    def test_repr_of_a_lazy_result_does_not_materialise(self, relation):
        result = ThymeResult(relation)

        text = repr(result)

        assert result._materialised is None
        assert "lazy" in text.lower()

    def test_repr_of_an_eager_result_still_shows_the_frame(self):
        result = ThymeResult(pl.DataFrame({"a": [1]}))

        assert "1 rows" in repr(result)

    def test_len_of_a_lazy_result_counts_without_materialising(self, relation):
        result = ThymeResult(relation)

        assert len(result) == 3
        # A count is a cheap aggregate; pulling every column to answer it is not.
        assert result._materialised is None

    def test_columns_are_available_without_materialising(self, relation):
        result = ThymeResult(relation)

        assert result.columns == ["entity_id", "score"]
        assert result._materialised is None
