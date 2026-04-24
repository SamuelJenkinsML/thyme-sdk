"""Fluent builder that serialises to ``Predicate`` / ``Derivation`` protos.

The public entry points are ``col(name)`` and ``lit(value)``. Both return
``Expr`` instances that support arithmetic and comparison operators; the result
of a comparison is a ``PredicateExpr`` which in turn supports ``&``, ``|``,
``~``. Both wrappers expose ``to_proto()`` for downstream serialisation."""

from __future__ import annotations

from typing import Union

from thyme.gen import expr_pb2

_Scalar = Union[int, float, str, bool]
_ExprLike = Union["Expr", _Scalar]


def col(name: str) -> "Expr":
    """Reference a column by name."""
    d = expr_pb2.Derivation()
    d.column_ref = name
    return Expr(d)


def lit(value: _Scalar) -> "Expr":
    """Wrap a Python scalar as a typed literal."""
    d = expr_pb2.Derivation()
    _set_literal(d.literal, value)
    return Expr(d)


class Expr:
    """A scalar expression — either a column reference, a literal, an
    arithmetic tree, or a ``fill_null`` wrapper. Use operator overloads to
    build compound expressions."""

    __slots__ = ("_derivation",)

    def __init__(self, derivation: expr_pb2.Derivation):
        self._derivation = derivation

    def to_proto(self) -> expr_pb2.Derivation:
        return self._derivation

    # Arithmetic operators
    def __add__(self, other: _ExprLike) -> "Expr":
        return _arith(expr_pb2.ARITH_ADD, self, _to_expr(other))

    def __sub__(self, other: _ExprLike) -> "Expr":
        return _arith(expr_pb2.ARITH_SUB, self, _to_expr(other))

    def __mul__(self, other: _ExprLike) -> "Expr":
        return _arith(expr_pb2.ARITH_MUL, self, _to_expr(other))

    def __truediv__(self, other: _ExprLike) -> "Expr":
        return _arith(expr_pb2.ARITH_DIV, self, _to_expr(other))

    def __radd__(self, other: _ExprLike) -> "Expr":
        return _arith(expr_pb2.ARITH_ADD, _to_expr(other), self)

    def __rsub__(self, other: _ExprLike) -> "Expr":
        return _arith(expr_pb2.ARITH_SUB, _to_expr(other), self)

    def __rmul__(self, other: _ExprLike) -> "Expr":
        return _arith(expr_pb2.ARITH_MUL, _to_expr(other), self)

    def __rtruediv__(self, other: _ExprLike) -> "Expr":
        return _arith(expr_pb2.ARITH_DIV, _to_expr(other), self)

    # Comparison operators — return PredicateExpr
    def __eq__(self, other: _ExprLike) -> "PredicateExpr":  # type: ignore[override]
        return _compare(expr_pb2.CMP_EQ, self, _to_expr(other))

    def __ne__(self, other: _ExprLike) -> "PredicateExpr":  # type: ignore[override]
        return _compare(expr_pb2.CMP_NEQ, self, _to_expr(other))

    def __lt__(self, other: _ExprLike) -> "PredicateExpr":
        return _compare(expr_pb2.CMP_LT, self, _to_expr(other))

    def __le__(self, other: _ExprLike) -> "PredicateExpr":
        return _compare(expr_pb2.CMP_LTE, self, _to_expr(other))

    def __gt__(self, other: _ExprLike) -> "PredicateExpr":
        return _compare(expr_pb2.CMP_GT, self, _to_expr(other))

    def __ge__(self, other: _ExprLike) -> "PredicateExpr":
        return _compare(expr_pb2.CMP_GTE, self, _to_expr(other))

    __hash__ = None  # type: ignore[assignment]

    # Null handling
    def is_null(self) -> "PredicateExpr":
        if self._derivation.WhichOneof("kind") != "column_ref":
            raise TypeError(
                "is_null() is only supported on a column reference in v1; "
                "wrap intermediate expressions in a column first"
            )
        p = expr_pb2.Predicate()
        p.is_null.column = self._derivation.column_ref
        return PredicateExpr(p)

    def fill_null(self, default: _Scalar) -> "Expr":
        d = expr_pb2.Derivation()
        d.fill_null.value.CopyFrom(self._derivation)
        _set_literal(d.fill_null.default, default)
        return Expr(d)


class PredicateExpr:
    """A boolean-valued predicate. Supports ``&``, ``|``, ``~``."""

    __slots__ = ("_predicate",)

    def __init__(self, predicate: expr_pb2.Predicate):
        self._predicate = predicate

    def to_proto(self) -> expr_pb2.Predicate:
        return self._predicate

    def __and__(self, other: "PredicateExpr") -> "PredicateExpr":
        return _logic(expr_pb2.LOGIC_AND, [self, other])

    def __or__(self, other: "PredicateExpr") -> "PredicateExpr":
        return _logic(expr_pb2.LOGIC_OR, [self, other])

    def __invert__(self) -> "PredicateExpr":
        return _logic(expr_pb2.LOGIC_NOT, [self])

    __hash__ = None  # type: ignore[assignment]


def _to_expr(value: _ExprLike) -> Expr:
    if isinstance(value, Expr):
        return value
    return lit(value)


def _set_literal(msg: expr_pb2.Literal, value: _Scalar) -> None:
    # bool is a subclass of int — check it first or bool literals land in int_value.
    if isinstance(value, bool):
        msg.bool_value = value
    elif isinstance(value, int):
        msg.int_value = value
    elif isinstance(value, float):
        msg.float_value = value
    elif isinstance(value, str):
        msg.string_value = value
    else:
        raise TypeError(f"unsupported literal type: {type(value).__name__}")


def _arith(op: int, lhs: Expr, rhs: Expr) -> Expr:
    d = expr_pb2.Derivation()
    d.arith.op = op
    d.arith.lhs.CopyFrom(lhs._derivation)
    d.arith.rhs.CopyFrom(rhs._derivation)
    return Expr(d)


def _compare(op: int, lhs: Expr, rhs: Expr) -> PredicateExpr:
    p = expr_pb2.Predicate()
    p.compare.op = op
    p.compare.lhs.CopyFrom(lhs._derivation)
    p.compare.rhs.CopyFrom(rhs._derivation)
    return PredicateExpr(p)


def _logic(kind: int, operands: list[PredicateExpr]) -> PredicateExpr:
    p = expr_pb2.Predicate()
    p.logic.kind = kind
    for operand in operands:
        p.logic.operands.append(operand._predicate)
    return PredicateExpr(p)
