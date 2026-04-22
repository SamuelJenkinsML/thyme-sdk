"""Tests for thyme.codegen.types — Thyme dtype to Python-annotation mapping."""

import pytest

from thyme.codegen.types import thyme_type_to_python_annotation


class TestThymeTypeToPythonAnnotation:
    """Given a Thyme dtype string, when mapping to a Python annotation, then
    the correct string is returned."""

    @pytest.mark.parametrize(
        "dtype,expected",
        [
            ("int", "int"),
            ("float", "float"),
            ("str", "str"),
            ("bool", "bool"),
            ("datetime", "datetime.datetime"),
        ],
    )
    def test_scalar_dtype_maps_to_python_annotation(
        self, dtype: str, expected: str
    ) -> None:
        assert thyme_type_to_python_annotation(dtype) == expected

    def test_unknown_dtype_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Unknown Thyme dtype"):
            thyme_type_to_python_annotation("complex128")

    def test_empty_string_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Unknown Thyme dtype"):
            thyme_type_to_python_annotation("")
