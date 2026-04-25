"""TDD tests for pipeline-level ``.filter()`` and ``.assign()`` operators.
These consume Predicate/Derivation protos produced by ``thyme.expr`` and emit
operator specs that the compiler serialises into the pipeline DAG."""

import pytest

from thyme.compiler import compile_pipeline
from thyme.expr import col
from thyme.gen import expr_pb2
from thyme.pipeline import Count, PipelineNode, Sum


class TestPipelineFilter:
    def test_filter_adds_pre_op(self):
        # Given a pipeline node
        # When filter is applied with a predicate
        # Then a pre_ops entry carries the predicate proto
        node = PipelineNode("ClickEvent").filter(col("dwell_time_sec") > 0.0)
        assert len(node._pre_ops) == 1
        op = node._pre_ops[0]
        assert "filter" in op
        predicate: expr_pb2.Predicate = op["filter"]["predicate"]
        assert predicate.compare.op == expr_pb2.CMP_GT
        assert predicate.compare.lhs.column_ref == "dwell_time_sec"

    def test_filter_type_error_on_non_predicate(self):
        with pytest.raises(TypeError):
            PipelineNode("X").filter("not a predicate")  # type: ignore[arg-type]

    def test_filter_chains_before_groupby_aggregate(self):
        node = (
            PipelineNode("Order")
            .filter(col("amount") > 0)
            .groupby("user_id")
            .aggregate(total=Sum(of="amount", window="24h"))
        )
        # groupby/aggregate preserve pre_ops so downstream serialization sees them
        assert len(node._pre_ops) == 1
        assert node._pre_ops[0]["filter"]["predicate"].compare.lhs.column_ref == "amount"


class TestPipelineAssign:
    def test_assign_adds_pre_op(self):
        node = PipelineNode("Order").assign(amount_usd=col("amount") * col("rate"))
        assert len(node._pre_ops) == 1
        op = node._pre_ops[0]
        assert "assign" in op
        spec = op["assign"]
        assert spec["column"] == "amount_usd"
        d: expr_pb2.Derivation = spec["value"]
        assert d.arith.op == expr_pb2.ARITH_MUL
        assert d.arith.lhs.column_ref == "amount"

    def test_assign_multiple_columns_in_one_call(self):
        node = PipelineNode("Order").assign(
            a=col("x") + 1,
            b=col("y") * 2,
        )
        assert len(node._pre_ops) == 2
        cols = [op["assign"]["column"] for op in node._pre_ops]
        assert sorted(cols) == ["a", "b"]

    def test_assign_type_error_on_non_expr(self):
        with pytest.raises(TypeError):
            PipelineNode("X").assign(y="literal")  # type: ignore[arg-type]


class TestCompilePipeline:
    def _build_meta(self, node: PipelineNode, operators: list[dict]) -> dict:
        return {
            "name": "test_pipe",
            "version": 1,
            "input_datasets": [node.dataset_name],
            "output_dataset": "Out",
            "operators": operators,
            "source_code": "def test_pipe(...): ...",
        }

    def test_compile_emits_filter_operator(self):
        node = PipelineNode("ClickEvent").filter(col("dwell_time_sec") > 0.0)
        ops = node.to_operators()
        meta = self._build_meta(node, ops)
        pipe = compile_pipeline(meta)
        # First operator should be Filter with predicate set
        assert len(pipe.operators) >= 1
        first = pipe.operators[0]
        assert first.WhichOneof("op") == "filter"
        assert first.filter.HasField("predicate")
        assert first.filter.predicate.compare.op == expr_pb2.CMP_GT

    def test_compile_emits_assign_operator(self):
        node = PipelineNode("Order").assign(amount_usd=col("amount") * col("rate"))
        ops = node.to_operators()
        meta = self._build_meta(node, ops)
        pipe = compile_pipeline(meta)
        first = pipe.operators[0]
        assert first.WhichOneof("op") == "assign"
        assert first.assign.column == "amount_usd"
        assert first.assign.value.arith.op == expr_pb2.ARITH_MUL

    def test_compile_preserves_pre_ops_before_aggregate(self):
        node = (
            PipelineNode("Order")
            .filter(col("amount") > 0)
            .groupby("user_id")
            .aggregate(c=Count(window="1h"))
        )
        ops = node.to_operators()
        meta = self._build_meta(node, ops)
        pipe = compile_pipeline(meta)
        kinds = [op.WhichOneof("op") for op in pipe.operators]
        # Filter before aggregate
        assert kinds == ["filter", "aggregate"]
