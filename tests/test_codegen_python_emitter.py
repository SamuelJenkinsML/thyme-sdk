"""Tests for thyme.codegen.python — .pyi stub emission.

Structural assertions use ``ast.parse()`` rather than raw string match so
whitespace/import-order drift doesn't churn the tests. One smoke test runs
``ast.parse()`` on the golden fixture too as a syntax sanity check.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from thyme.codegen.ir import (
    CodegenIR,
    DatasetIR,
    FeatureIR,
    FeaturesetIR,
)
from thyme.codegen.python import emit_python_stubs


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_ir() -> CodegenIR:
    """A realistic IR: one featureset (3 features including datetime), one dataset."""
    return CodegenIR(
        featuresets=(
            FeaturesetIR(
                name="UserFeatures",
                features=(
                    FeatureIR(
                        name="user_id", python_annotation="int", feature_id=1
                    ),
                    FeatureIR(
                        name="total_spend", python_annotation="float", feature_id=2
                    ),
                    FeatureIR(
                        name="last_seen",
                        python_annotation="datetime.datetime",
                        feature_id=3,
                    ),
                ),
            ),
        ),
        datasets=(
            DatasetIR(
                name="Purchase",
                fields=(
                    FeatureIR(
                        name="user_id", python_annotation="int", feature_id=0
                    ),
                    FeatureIR(
                        name="amount", python_annotation="float", feature_id=0
                    ),
                    FeatureIR(
                        name="event_time",
                        python_annotation="datetime.datetime",
                        feature_id=0,
                    ),
                ),
            ),
        ),
    )


def _parse(path: Path) -> ast.Module:
    return ast.parse(path.read_text())


def _class_defs(tree: ast.Module) -> dict[str, ast.ClassDef]:
    return {node.name: node for node in tree.body if isinstance(node, ast.ClassDef)}


def _annotated_fields(cls: ast.ClassDef) -> dict[str, str]:
    """Return {attr_name: annotation_source} for AnnAssign nodes in the class body."""
    out: dict[str, str] = {}
    for node in cls.body:
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            out[node.target.id] = ast.unparse(node.annotation)
    return out


def _overload_methods(cls: ast.ClassDef) -> list[str]:
    """Return the names of methods decorated with @overload on this class."""
    names: list[str] = []
    for node in cls.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for dec in node.decorator_list:
                if isinstance(dec, ast.Name) and dec.id == "overload":
                    names.append(node.name)
                    break
    return names


# ---------------------------------------------------------------------------
# Emission happy path
# ---------------------------------------------------------------------------


class TestEmitPythonStubs:
    """Given an IR, when emitting, then the expected .pyi files exist and
    parse into the expected AST shape."""

    def test_emits_one_file_per_featureset_and_dataset_plus_init(
        self, sample_ir: CodegenIR, tmp_path: Path
    ) -> None:
        written = emit_python_stubs(sample_ir, tmp_path)

        names = sorted(p.name for p in written)
        assert names == [
            "__init__.pyi",
            "purchase.pyi",
            "user_features.pyi",
        ]
        for p in written:
            assert p.exists()

    def test_featureset_row_class_has_typed_fields(
        self, sample_ir: CodegenIR, tmp_path: Path
    ) -> None:
        emit_python_stubs(sample_ir, tmp_path)

        tree = _parse(tmp_path / "user_features.pyi")
        classes = _class_defs(tree)
        assert "UserFeatures" in classes
        fields = _annotated_fields(classes["UserFeatures"])
        assert fields == {
            "user_id": "int",
            "total_spend": "float",
            "last_seen": "datetime.datetime",
        }

    def test_datetime_feature_triggers_datetime_import(
        self, sample_ir: CodegenIR, tmp_path: Path
    ) -> None:
        emit_python_stubs(sample_ir, tmp_path)

        source = (tmp_path / "user_features.pyi").read_text()
        assert "import datetime" in source

    def test_featureset_file_declares_typed_client_overloads(
        self, sample_ir: CodegenIR, tmp_path: Path
    ) -> None:
        emit_python_stubs(sample_ir, tmp_path)

        tree = _parse(tmp_path / "user_features.pyi")
        classes = _class_defs(tree)
        assert "ThymeClient" in classes

        overloads = _overload_methods(classes["ThymeClient"])
        # Each of the four client methods gets a narrowing overload for the
        # featureset in this stub file.
        assert overloads.count("query") == 1
        assert overloads.count("query_batch") == 1
        assert overloads.count("query_offline") == 1

    def test_dataset_file_declares_lookup_overload(
        self, sample_ir: CodegenIR, tmp_path: Path
    ) -> None:
        emit_python_stubs(sample_ir, tmp_path)

        tree = _parse(tmp_path / "purchase.pyi")
        classes = _class_defs(tree)
        assert "Purchase" in classes
        assert "ThymeClient" in classes
        overloads = _overload_methods(classes["ThymeClient"])
        assert overloads == ["lookup"]

    def test_dataset_file_declares_row_fields(
        self, sample_ir: CodegenIR, tmp_path: Path
    ) -> None:
        emit_python_stubs(sample_ir, tmp_path)
        tree = _parse(tmp_path / "purchase.pyi")
        fields = _annotated_fields(_class_defs(tree)["Purchase"])
        assert fields == {
            "user_id": "int",
            "amount": "float",
            "event_time": "datetime.datetime",
        }

    def test_mockcontext_overloads_emitted_alongside_client(
        self, sample_ir: CodegenIR, tmp_path: Path
    ) -> None:
        emit_python_stubs(sample_ir, tmp_path)
        tree = _parse(tmp_path / "user_features.pyi")
        classes = _class_defs(tree)
        assert "MockContext" in classes
        mock_overloads = _overload_methods(classes["MockContext"])
        assert "query" in mock_overloads
        assert "query_offline" in mock_overloads

    def test_init_reexports_row_and_dataset_classes(
        self, sample_ir: CodegenIR, tmp_path: Path
    ) -> None:
        emit_python_stubs(sample_ir, tmp_path)
        init = (tmp_path / "__init__.pyi").read_text()
        # PEP 484 explicit re-export form: `from .x import Y as Y`.
        assert "from .user_features import UserFeatures as UserFeatures" in init
        assert "from .purchase import Purchase as Purchase" in init

    def test_every_emitted_file_parses_as_python(
        self, sample_ir: CodegenIR, tmp_path: Path
    ) -> None:
        written = emit_python_stubs(sample_ir, tmp_path)
        for path in written:
            ast.parse(path.read_text())  # no SyntaxError

    def test_optional_dataset_field_emits_union_none(
        self, tmp_path: Path
    ) -> None:
        ir = CodegenIR(
            featuresets=(),
            datasets=(
                DatasetIR(
                    name="Sparse",
                    fields=(
                        FeatureIR(
                            name="id", python_annotation="int", feature_id=0
                        ),
                        FeatureIR(
                            name="note",
                            python_annotation="str",
                            feature_id=0,
                            optional=True,
                        ),
                    ),
                ),
            ),
        )
        emit_python_stubs(ir, tmp_path)
        fields = _annotated_fields(_class_defs(_parse(tmp_path / "sparse.pyi"))["Sparse"])
        assert fields["id"] == "int"
        assert fields["note"] == "str | None"


# ---------------------------------------------------------------------------
# Negative paths
# ---------------------------------------------------------------------------


class TestEmitPythonStubsNegatives:
    def test_empty_dir_without_force_is_fine(
        self, sample_ir: CodegenIR, tmp_path: Path
    ) -> None:
        emit_python_stubs(sample_ir, tmp_path)
        assert (tmp_path / "user_features.pyi").exists()

    def test_nonempty_dir_without_force_raises(
        self, sample_ir: CodegenIR, tmp_path: Path
    ) -> None:
        (tmp_path / "existing.pyi").write_text("# preexisting\n")

        with pytest.raises(FileExistsError, match="--force"):
            emit_python_stubs(sample_ir, tmp_path)

    def test_force_prunes_stale_pyi_files(
        self, sample_ir: CodegenIR, tmp_path: Path
    ) -> None:
        # given a stale stub left over from a previous run
        stale = tmp_path / "old_featureset.pyi"
        stale.write_text("# stale\n")
        # plus an unrelated non-.pyi file that must be preserved
        other = tmp_path / "README.md"
        other.write_text("keep me\n")

        # when
        emit_python_stubs(sample_ir, tmp_path, force=True)

        # then: stale .pyi gone; non-.pyi preserved; new stubs written
        assert not stale.exists()
        assert other.exists()
        assert (tmp_path / "user_features.pyi").exists()

    def test_out_dir_is_created_if_missing(
        self, sample_ir: CodegenIR, tmp_path: Path
    ) -> None:
        target = tmp_path / "nested" / "stubs"
        emit_python_stubs(sample_ir, target)
        assert (target / "user_features.pyi").exists()
