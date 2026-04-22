"""End-to-end tests for the ``thyme codegen python`` CLI command."""

from __future__ import annotations

import ast
from pathlib import Path

from typer.testing import CliRunner

from thyme.cli import app


runner = CliRunner()
FIXTURE = Path(__file__).parent / "fixtures" / "sample_features.py"


def test_codegen_python_from_path_writes_expected_files(tmp_path: Path) -> None:
    out = tmp_path / "stubs"

    result = runner.invoke(
        app,
        ["codegen", "python", "--path", str(FIXTURE), "--out", str(out)],
    )

    assert result.exit_code == 0, result.output
    assert (out / "user_features.pyi").exists()
    assert (out / "purchase.pyi").exists()
    assert (out / "__init__.pyi").exists()


def test_codegen_python_output_parses_and_has_expected_shape(
    tmp_path: Path,
) -> None:
    out = tmp_path / "stubs"
    result = runner.invoke(
        app,
        ["codegen", "python", "--path", str(FIXTURE), "--out", str(out)],
    )
    assert result.exit_code == 0, result.output

    fs_tree = ast.parse((out / "user_features.pyi").read_text())
    class_defs = {n.name: n for n in fs_tree.body if isinstance(n, ast.ClassDef)}
    assert "UserFeatures" in class_defs
    assert "ThymeClient" in class_defs
    assert "MockContext" in class_defs


def test_codegen_python_requires_module_or_path(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        ["codegen", "python", "--out", str(tmp_path / "stubs")],
    )
    assert result.exit_code == 1
    assert "Provide either" in result.output


def test_codegen_python_rejects_both_module_and_path(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        [
            "codegen",
            "python",
            "-m",
            "tests.fixtures.sample_features",
            "--path",
            str(FIXTURE),
            "--out",
            str(tmp_path / "stubs"),
        ],
    )
    assert result.exit_code == 1
    assert "not both" in result.output


def test_codegen_python_errors_when_no_definitions_found(tmp_path: Path) -> None:
    # A module with no @dataset / @featureset decorators
    empty = tmp_path / "empty_module.py"
    empty.write_text("# nothing registered here\n")

    result = runner.invoke(
        app,
        ["codegen", "python", "--path", str(empty), "--out", str(tmp_path / "stubs")],
    )
    assert result.exit_code == 1
    assert "no featuresets or datasets" in result.output.lower()


def test_codegen_python_refuses_to_overwrite_without_force(
    tmp_path: Path,
) -> None:
    out = tmp_path / "stubs"
    out.mkdir()
    (out / "existing.pyi").write_text("# pre-existing\n")

    result = runner.invoke(
        app,
        ["codegen", "python", "--path", str(FIXTURE), "--out", str(out)],
    )
    assert result.exit_code == 1
    assert "--force" in result.output


def test_codegen_python_force_overwrites_and_prunes(tmp_path: Path) -> None:
    out = tmp_path / "stubs"
    out.mkdir()
    stale = out / "stale_thing.pyi"
    stale.write_text("# stale\n")

    result = runner.invoke(
        app,
        [
            "codegen",
            "python",
            "--path",
            str(FIXTURE),
            "--out",
            str(out),
            "--force",
        ],
    )
    assert result.exit_code == 0, result.output
    assert not stale.exists()
    assert (out / "user_features.pyi").exists()


def test_codegen_python_handles_import_errors(tmp_path: Path) -> None:
    broken = tmp_path / "broken.py"
    broken.write_text("raise RuntimeError('boom')\n")

    result = runner.invoke(
        app,
        ["codegen", "python", "--path", str(broken), "--out", str(tmp_path / "out")],
    )
    assert result.exit_code == 1
    assert "importing" in result.output.lower()
