"""Tests for thyme.cli_refs — CLI reference resolution and entity collection."""
from __future__ import annotations

from pathlib import Path

import pytest


class TestResolveRef:
    """Given a MODULE:CLASS reference, when resolved, it returns the class."""

    def test_resolves_dotted_module_and_class(self):
        # given
        from thyme.cli_refs import resolve_ref

        # when
        cls = resolve_ref("tests.fixtures.sample_features:UserFeatures", module_path=None)

        # then
        from tests.fixtures.sample_features import UserFeatures
        assert cls is UserFeatures

    def test_errors_when_colon_is_missing(self):
        # given
        from thyme.cli_refs import resolve_ref

        # when / then
        with pytest.raises(ValueError, match="expected 'module:Class'"):
            resolve_ref("tests.fixtures.sample_features.UserFeatures", module_path=None)

    def test_errors_when_class_not_found(self):
        # given
        from thyme.cli_refs import resolve_ref

        # when / then
        with pytest.raises(AttributeError):
            resolve_ref("tests.fixtures.sample_features:NoSuchClass", module_path=None)

    def test_errors_when_not_a_featureset(self):
        # given
        from thyme.cli_refs import resolve_ref

        # when / then
        with pytest.raises(ValueError, match="@featureset"):
            resolve_ref(
                "tests.fixtures.sample_features:NotAFeatureset",
                module_path=None,
                expect="featureset",
            )

    def test_resolves_dataset_with_expect_dataset(self):
        # given
        from thyme.cli_refs import resolve_ref

        # when
        cls = resolve_ref(
            "tests.fixtures.sample_features:Purchase",
            module_path=None,
            expect="dataset",
        )

        # then
        from tests.fixtures.sample_features import Purchase
        assert cls is Purchase


class TestCollectEntities:
    """Given one or more --entity inputs, when collected, they flatten to a list."""

    def test_empty_returns_empty_list(self):
        from thyme.cli_refs import collect_entities
        assert collect_entities([]) == []

    def test_single_value_passes_through(self):
        from thyme.cli_refs import collect_entities
        assert collect_entities(["user_1"]) == ["user_1"]

    def test_repeated_values_preserve_order(self):
        from thyme.cli_refs import collect_entities
        assert collect_entities(["a", "b", "c"]) == ["a", "b", "c"]

    def test_comma_split_within_value(self):
        from thyme.cli_refs import collect_entities
        assert collect_entities(["a,b,c"]) == ["a", "b", "c"]

    def test_trims_whitespace_from_comma_values(self):
        from thyme.cli_refs import collect_entities
        assert collect_entities([" a , b "]) == ["a", "b"]

    def test_reads_file_via_at_prefix(self, tmp_path: Path):
        # given
        from thyme.cli_refs import collect_entities
        ids_file = tmp_path / "ids.txt"
        ids_file.write_text("user_1\nuser_2\n\nuser_3\n")  # blank line ignored

        # when
        got = collect_entities([f"@{ids_file}"])

        # then
        assert got == ["user_1", "user_2", "user_3"]

    def test_mixed_file_and_flags(self, tmp_path: Path):
        # given
        from thyme.cli_refs import collect_entities
        ids_file = tmp_path / "ids.txt"
        ids_file.write_text("from_file\n")

        # when
        got = collect_entities(["cli_1", f"@{ids_file}", "cli_2,cli_3"])

        # then
        assert got == ["cli_1", "from_file", "cli_2", "cli_3"]

    def test_missing_file_raises(self, tmp_path: Path):
        from thyme.cli_refs import collect_entities
        with pytest.raises(FileNotFoundError):
            collect_entities([f"@{tmp_path}/does-not-exist.txt"])


class TestReadEntitiesDataFrame:
    """Given a Parquet/CSV/JSONL file, when read, it returns a Polars DataFrame."""

    def test_reads_csv(self, tmp_path: Path):
        # given
        import polars as pl
        from thyme.cli_refs import read_entities_dataframe
        p = tmp_path / "rows.csv"
        p.write_text("entity_id,timestamp\nuser_1,2026-04-01T00:00:00Z\n")

        # when
        df = read_entities_dataframe(p)

        # then
        assert isinstance(df, pl.DataFrame)
        assert df.columns == ["entity_id", "timestamp"]
        assert len(df) == 1

    def test_reads_parquet(self, tmp_path: Path):
        # given
        import polars as pl
        from thyme.cli_refs import read_entities_dataframe
        p = tmp_path / "rows.parquet"
        pl.DataFrame({"entity_id": ["u1", "u2"], "timestamp": ["t1", "t2"]}).write_parquet(p)

        # when
        df = read_entities_dataframe(p)

        # then
        assert len(df) == 2
        assert df.columns == ["entity_id", "timestamp"]

    def test_reads_jsonl(self, tmp_path: Path):
        # given
        from thyme.cli_refs import read_entities_dataframe
        p = tmp_path / "rows.jsonl"
        p.write_text('{"entity_id":"u1","timestamp":"t1"}\n{"entity_id":"u2","timestamp":"t2"}\n')

        # when
        df = read_entities_dataframe(p)

        # then
        assert len(df) == 2

    def test_unknown_extension_raises(self, tmp_path: Path):
        from thyme.cli_refs import read_entities_dataframe
        p = tmp_path / "rows.xls"
        p.write_text("ignored")
        with pytest.raises(ValueError, match="unsupported"):
            read_entities_dataframe(p)
