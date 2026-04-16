"""Tests for thyme.result — ThymeResult universal wrapper."""

import polars as pl
import pytest

from thyme.result import ThymeResult


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestThymeResultConstruction:
    """Given a Polars DataFrame, when constructing ThymeResult, then it wraps correctly."""

    def test_construct_from_polars_dataframe(self):
        df = pl.DataFrame({"a": [1, 2], "b": [3.0, 4.0]})
        result = ThymeResult(df)
        assert len(result) == 2

    def test_construct_with_metadata(self):
        df = pl.DataFrame({"x": [1]})
        result = ThymeResult(df, metadata={"mode": "online", "entity_type": "User"})
        assert result.metadata["mode"] == "online"
        assert result.metadata["entity_type"] == "User"

    def test_default_metadata_is_empty_dict(self):
        df = pl.DataFrame({"x": [1]})
        result = ThymeResult(df)
        assert result.metadata == {}


# ---------------------------------------------------------------------------
# to_polars
# ---------------------------------------------------------------------------


class TestToPolars:
    """Given a ThymeResult, when calling to_polars(), then the original DataFrame is returned."""

    def test_to_polars_returns_dataframe(self):
        df = pl.DataFrame({"a": [1, 2, 3]})
        result = ThymeResult(df)
        assert result.to_polars().equals(df)

    def test_to_polars_preserves_types(self):
        df = pl.DataFrame({
            "i": pl.Series([1, 2], dtype=pl.Int64),
            "f": pl.Series([1.5, 2.5], dtype=pl.Float64),
            "s": pl.Series(["a", "b"], dtype=pl.Utf8),
        })
        result = ThymeResult(df)
        out = result.to_polars()
        assert out.schema["i"] == pl.Int64
        assert out.schema["f"] == pl.Float64
        assert out.schema["s"] == pl.Utf8


# ---------------------------------------------------------------------------
# to_dict
# ---------------------------------------------------------------------------


class TestToDict:
    """Given a ThymeResult, when calling to_dict(), then list of dicts is returned."""

    def test_to_dict_basic(self):
        df = pl.DataFrame({"name": ["alice", "bob"], "age": [30, 25]})
        result = ThymeResult(df)
        dicts = result.to_dict()
        assert dicts == [{"name": "alice", "age": 30}, {"name": "bob", "age": 25}]

    def test_to_dict_empty_dataframe(self):
        df = pl.DataFrame({"x": pl.Series([], dtype=pl.Int64)})
        result = ThymeResult(df)
        assert result.to_dict() == []

    def test_to_dict_single_row(self):
        df = pl.DataFrame({"score": [4.5]})
        result = ThymeResult(df)
        assert result.to_dict() == [{"score": 4.5}]


# ---------------------------------------------------------------------------
# to_pandas
# ---------------------------------------------------------------------------


class TestToPandas:
    """Given a ThymeResult, when calling to_pandas(), then a pandas DataFrame is returned."""

    def test_to_pandas_returns_pandas_dataframe(self):
        pd = pytest.importorskip("pandas")
        df = pl.DataFrame({"a": [1, 2], "b": [3.0, 4.0]})
        result = ThymeResult(df)
        pdf = result.to_pandas()
        assert isinstance(pdf, pd.DataFrame)
        assert list(pdf.columns) == ["a", "b"]
        assert len(pdf) == 2

    def test_to_pandas_preserves_values(self):
        pytest.importorskip("pandas")
        df = pl.DataFrame({"x": [10, 20, 30]})
        result = ThymeResult(df)
        pdf = result.to_pandas()
        assert pdf["x"].tolist() == [10, 20, 30]


# ---------------------------------------------------------------------------
# to_arrow
# ---------------------------------------------------------------------------


class TestToArrow:
    """Given a ThymeResult, when calling to_arrow(), then a PyArrow Table is returned."""

    def test_to_arrow_returns_arrow_table(self):
        pa = pytest.importorskip("pyarrow")
        df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        result = ThymeResult(df)
        table = result.to_arrow()
        assert isinstance(table, pa.Table)
        assert table.num_rows == 2
        assert table.column_names == ["a", "b"]


# ---------------------------------------------------------------------------
# __len__
# ---------------------------------------------------------------------------


class TestLen:
    def test_len_returns_row_count(self):
        df = pl.DataFrame({"a": [1, 2, 3, 4, 5]})
        result = ThymeResult(df)
        assert len(result) == 5

    def test_len_empty(self):
        df = pl.DataFrame({"a": pl.Series([], dtype=pl.Int64)})
        result = ThymeResult(df)
        assert len(result) == 0


# ---------------------------------------------------------------------------
# __getitem__
# ---------------------------------------------------------------------------


class TestGetItem:
    def test_getitem_returns_series(self):
        df = pl.DataFrame({"x": [1, 2], "y": [3, 4]})
        result = ThymeResult(df)
        series = result["x"]
        assert isinstance(series, pl.Series)
        assert series.to_list() == [1, 2]

    def test_getitem_missing_column_raises(self):
        df = pl.DataFrame({"x": [1]})
        result = ThymeResult(df)
        with pytest.raises(Exception):
            result["nonexistent"]


# ---------------------------------------------------------------------------
# __repr__
# ---------------------------------------------------------------------------


class TestRepr:
    def test_repr_contains_data(self):
        df = pl.DataFrame({"feature": [1.0, 2.0]})
        result = ThymeResult(df)
        r = repr(result)
        assert "feature" in r
        assert "ThymeResult" in r
