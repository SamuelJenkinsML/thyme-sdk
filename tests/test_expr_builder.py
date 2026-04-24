"""Expression-builder TDD tests: ``col()``, ``lit()``, arithmetic, comparison,
logic, null handling. The builder produces ``Predicate`` / ``Derivation`` proto
messages for the pipeline write-path filter/assign operators."""

import pytest

from thyme.expr import col, lit
from thyme.gen import expr_pb2


class TestCol:
    def test_col_produces_column_ref(self):
        # Given a column reference
        # When the expression is serialized
        # Then the Derivation holds a column_ref oneof set to the name
        d = col("amount").to_proto()
        assert d.WhichOneof("kind") == "column_ref"
        assert d.column_ref == "amount"


class TestLit:
    def test_int_literal(self):
        d = lit(42).to_proto()
        assert d.literal.WhichOneof("kind") == "int_value"
        assert d.literal.int_value == 42

    def test_float_literal(self):
        d = lit(3.14).to_proto()
        assert d.literal.WhichOneof("kind") == "float_value"
        assert d.literal.float_value == pytest.approx(3.14)

    def test_str_literal(self):
        d = lit("hello").to_proto()
        assert d.literal.WhichOneof("kind") == "string_value"
        assert d.literal.string_value == "hello"

    def test_bool_literal_not_int(self):
        # Given True is a bool subclass of int, the builder must pick bool_value
        d = lit(True).to_proto()
        assert d.literal.WhichOneof("kind") == "bool_value"
        assert d.literal.bool_value is True

    def test_unsupported_type_raises(self):
        with pytest.raises(TypeError):
            lit(object())


class TestComparison:
    def test_gt_col_lit(self):
        p = (col("amount") > 0).to_proto()
        assert p.WhichOneof("kind") == "compare"
        assert p.compare.op == expr_pb2.CMP_GT
        assert p.compare.lhs.column_ref == "amount"
        assert p.compare.rhs.literal.int_value == 0

    @pytest.mark.parametrize(
        "op_fn,expected",
        [
            (lambda a, b: a < b, expr_pb2.CMP_LT),
            (lambda a, b: a != b, expr_pb2.CMP_NEQ),
            (lambda a, b: a == b, expr_pb2.CMP_EQ),
            (lambda a, b: a <= b, expr_pb2.CMP_LTE),
            (lambda a, b: a >= b, expr_pb2.CMP_GTE),
        ],
    )
    def test_all_comparison_ops(self, op_fn, expected):
        p = op_fn(col("x"), 1).to_proto()
        assert p.compare.op == expected

    def test_col_neq_string(self):
        p = (col("merchant_type") != "test").to_proto()
        assert p.compare.op == expr_pb2.CMP_NEQ
        assert p.compare.rhs.literal.string_value == "test"


class TestArithmetic:
    def test_mul_two_cols(self):
        d = (col("a") * col("b")).to_proto()
        assert d.arith.op == expr_pb2.ARITH_MUL
        assert d.arith.lhs.column_ref == "a"
        assert d.arith.rhs.column_ref == "b"

    def test_col_plus_scalar(self):
        d = (col("a") + 1).to_proto()
        assert d.arith.op == expr_pb2.ARITH_ADD
        assert d.arith.rhs.literal.int_value == 1

    def test_scalar_minus_col_uses_rsub(self):
        d = (1 - col("a")).to_proto()
        assert d.arith.op == expr_pb2.ARITH_SUB
        assert d.arith.lhs.literal.int_value == 1
        assert d.arith.rhs.column_ref == "a"

    def test_nested_arith(self):
        # (a * b) + c  — lhs is the mul, rhs is the col
        d = (col("a") * col("b") + col("c")).to_proto()
        assert d.arith.op == expr_pb2.ARITH_ADD
        assert d.arith.lhs.arith.op == expr_pb2.ARITH_MUL
        assert d.arith.rhs.column_ref == "c"

    def test_division(self):
        d = (col("spend_7d") / 7.0).to_proto()
        assert d.arith.op == expr_pb2.ARITH_DIV
        assert d.arith.rhs.literal.float_value == pytest.approx(7.0)


class TestLogic:
    def test_and(self):
        p = ((col("a") > 0) & (col("b") != "test")).to_proto()
        assert p.WhichOneof("kind") == "logic"
        assert p.logic.kind == expr_pb2.LOGIC_AND
        assert len(p.logic.operands) == 2

    def test_or(self):
        p = ((col("a") > 0) | (col("b") < 1)).to_proto()
        assert p.logic.kind == expr_pb2.LOGIC_OR

    def test_not(self):
        p = (~(col("a") > 0)).to_proto()
        assert p.logic.kind == expr_pb2.LOGIC_NOT
        assert len(p.logic.operands) == 1


class TestNull:
    def test_is_null_on_col(self):
        p = col("x").is_null().to_proto()
        assert p.WhichOneof("kind") == "is_null"
        assert p.is_null.column == "x"

    def test_fill_null(self):
        d = col("x").fill_null(0.0).to_proto()
        assert d.WhichOneof("kind") == "fill_null"
        assert d.fill_null.value.column_ref == "x"
        assert d.fill_null.default.float_value == pytest.approx(0.0)

    def test_is_null_on_arith_raises(self):
        # is_null only makes sense on a bare column reference in v1
        with pytest.raises(TypeError):
            (col("a") * col("b")).is_null()
