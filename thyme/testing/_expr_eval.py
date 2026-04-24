"""Single-record evaluator for ``Predicate`` / ``Derivation`` protos.

Mirrors ``crates/engine/src/expr.rs``. Used by ``MockContext`` so test-side
parity is preserved with the production Rust interpreter.
"""

from __future__ import annotations

from typing import Any

from thyme.gen import expr_pb2


def eval_derivation(derivation: expr_pb2.Derivation, record: dict[str, Any]) -> Any:
    kind = derivation.WhichOneof("kind")
    if kind == "column_ref":
        return record.get(derivation.column_ref)
    if kind == "literal":
        return _eval_literal(derivation.literal)
    if kind == "arith":
        lhs = eval_derivation(derivation.arith.lhs, record)
        rhs = eval_derivation(derivation.arith.rhs, record)
        if lhs is None or rhs is None:
            return None
        op = derivation.arith.op
        if op == expr_pb2.ARITH_ADD:
            return lhs + rhs
        if op == expr_pb2.ARITH_SUB:
            return lhs - rhs
        if op == expr_pb2.ARITH_MUL:
            return lhs * rhs
        if op == expr_pb2.ARITH_DIV:
            return lhs / rhs
        raise ValueError(f"unknown arith op: {op}")
    if kind == "fill_null":
        value = eval_derivation(derivation.fill_null.value, record)
        if value is None:
            return _eval_literal(derivation.fill_null.default)
        return value
    raise ValueError(f"unknown derivation kind: {kind!r}")


def eval_predicate(predicate: expr_pb2.Predicate, record: dict[str, Any]) -> bool:
    kind = predicate.WhichOneof("kind")
    if kind == "compare":
        lhs = eval_derivation(predicate.compare.lhs, record)
        rhs = eval_derivation(predicate.compare.rhs, record)
        if lhs is None or rhs is None:
            return False
        op = predicate.compare.op
        if op == expr_pb2.CMP_EQ:
            return lhs == rhs
        if op == expr_pb2.CMP_NEQ:
            return lhs != rhs
        if op == expr_pb2.CMP_LT:
            return lhs < rhs
        if op == expr_pb2.CMP_LTE:
            return lhs <= rhs
        if op == expr_pb2.CMP_GT:
            return lhs > rhs
        if op == expr_pb2.CMP_GTE:
            return lhs >= rhs
        raise ValueError(f"unknown compare op: {op}")
    if kind == "is_null":
        return record.get(predicate.is_null.column) is None
    if kind == "logic":
        logic = predicate.logic
        if logic.kind == expr_pb2.LOGIC_AND:
            return all(eval_predicate(o, record) for o in logic.operands)
        if logic.kind == expr_pb2.LOGIC_OR:
            return any(eval_predicate(o, record) for o in logic.operands)
        if logic.kind == expr_pb2.LOGIC_NOT:
            # NOT is unary: take the first operand only
            if not logic.operands:
                raise ValueError("LOGIC_NOT requires exactly one operand")
            return not eval_predicate(logic.operands[0], record)
        raise ValueError(f"unknown logic kind: {logic.kind}")
    raise ValueError(f"unknown predicate kind: {kind!r}")


def _eval_literal(lit: expr_pb2.Literal) -> Any:
    kind = lit.WhichOneof("kind")
    if kind == "int_value":
        return lit.int_value
    if kind == "float_value":
        return lit.float_value
    if kind == "string_value":
        return lit.string_value
    if kind == "bool_value":
        return lit.bool_value
    raise ValueError(f"literal has no value set: {kind!r}")


def apply_pre_ops(events: list[dict], pre_ops: list[dict]) -> list[dict]:
    """Apply a chain of filter/assign ops in order. Returns a new list.

    Filters drop records; assigns mutate each record in place (shallow-copied)
    with the derived column value. None-valued assigns are kept as None to
    mirror polars/pandas null-propagation semantics.
    """
    current: list[dict] = [dict(r) for r in events]
    for op in pre_ops:
        if "filter" in op:
            predicate = op["filter"]["predicate"]
            current = [r for r in current if eval_predicate(predicate, r)]
        elif "assign" in op:
            spec = op["assign"]
            column = spec["column"]
            value_expr = spec["value"]
            for r in current:
                r[column] = eval_derivation(value_expr, r)
    return current
