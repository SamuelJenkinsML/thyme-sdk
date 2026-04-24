"""Closed-form expression builder for pipeline filter + assign operators.

Produces ``Predicate`` / ``Derivation`` proto messages consumed by the write-path
Rust interpreter in ``crates/engine/src/expr.rs``. Deliberately narrow — see
``proto/thyme/expr.proto`` for the schema. Grow both together."""

from thyme.expr._builder import Expr, PredicateExpr, col, lit

__all__ = ["Expr", "PredicateExpr", "col", "lit"]
