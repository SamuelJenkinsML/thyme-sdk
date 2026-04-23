"""Unit tests for pipeline-level ``.transform()`` — the opt-in Polars UDF
escape hatch. These cover source capture, signature validation, and wire-form
emission. The actual UDF execution is exercised end-to-end in test_e2e."""

import polars as pl
import pytest

from thyme.commit_payload import _wire_operator
from thyme.compiler import compile_pipeline
from thyme.pipeline import Count, PipelineNode


def tag_amount_band(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.when(pl.col("amount") < 10).then(pl.lit("small"))
          .when(pl.col("amount") < 100).then(pl.lit("medium"))
          .otherwise(pl.lit("large")).alias("amount_band"))


def identity(df: pl.DataFrame) -> pl.DataFrame:
    return df


class TestTransformCapturesSource:
    def test_transform_appends_pre_op(self):
        node = PipelineNode("Order").transform(
            tag_amount_band, output_columns={"amount_band": str}
        )
        assert len(node._pre_ops) == 1
        op = node._pre_ops[0]
        assert "transform" in op
        spec = op["transform"]
        pycode = spec["pycode"]
        assert pycode["entry_point"] == "_thyme_polars_wrapper"
        assert "def tag_amount_band" in pycode["source_code"]
        assert "import polars as pl" in pycode["source_code"]
        assert spec["output_columns"] == {"amount_band": "str"}

    def test_transform_wrapper_references_user_fn_name(self):
        node = PipelineNode("Order").transform(identity)
        src = node._pre_ops[0]["transform"]["pycode"]["source_code"]
        assert "identity(df)" in src
        assert "def _thyme_polars_wrapper" in src

    def test_transform_chains_before_aggregate(self):
        node = (
            PipelineNode("Order")
            .transform(identity)
            .groupby("user_id")
            .aggregate(c=Count(window="1h"))
        )
        assert len(node._pre_ops) == 1
        ops = node.to_operators()
        kinds = [next(iter(op.keys())) for op in ops]
        assert kinds == ["transform", "aggregate"]


class TestTransformValidation:
    def test_rejects_lambda(self):
        with pytest.raises(TypeError, match="lambdas are not supported"):
            PipelineNode("X").transform(lambda df: df)

    def test_rejects_non_callable(self):
        with pytest.raises(TypeError, match="expects a callable"):
            PipelineNode("X").transform("not a function")  # type: ignore[arg-type]

    def test_rejects_closure(self):
        threshold = 10.0

        def filter_above(df: pl.DataFrame) -> pl.DataFrame:
            return df.filter(pl.col("amount") > threshold)

        with pytest.raises(TypeError, match="closures over non-global state"):
            PipelineNode("X").transform(filter_above)


class TestTransformWireForm:
    def test_wire_operator_extracts_pycode(self):
        node = PipelineNode("Order").transform(identity)
        wire = _wire_operator(node._pre_ops[0])
        assert "transform" in wire
        assert "pycode" in wire["transform"]
        assert wire["transform"]["pycode"]["entry_point"] == "_thyme_polars_wrapper"
        # _wire key should be stripped out of the serialised form.
        assert "_wire" not in wire["transform"]


class TestTransformCompiles:
    def _build_meta(self, node: PipelineNode, operators: list[dict]) -> dict:
        return {
            "name": "test_pipe",
            "version": 1,
            "input_datasets": [node.dataset_name],
            "output_dataset": "Out",
            "operators": operators,
            "source_code": "def test_pipe(...): ...",
        }

    def test_compile_emits_transform_operator(self):
        node = PipelineNode("Order").transform(identity)
        ops = node.to_operators()
        meta = self._build_meta(node, ops)
        pipe = compile_pipeline(meta)
        assert len(pipe.operators) >= 1
        first = pipe.operators[0]
        assert first.WhichOneof("op") == "transform"
        assert first.transform.pycode.entry_point == "_thyme_polars_wrapper"
        assert "def identity" in first.transform.pycode.source_code

    def test_compile_preserves_order_filter_transform_aggregate(self):
        from thyme.expr import col

        node = (
            PipelineNode("Order")
            .filter(col("amount") > 0)
            .transform(identity)
            .groupby("user_id")
            .aggregate(c=Count(window="1h"))
        )
        ops = node.to_operators()
        meta = self._build_meta(node, ops)
        pipe = compile_pipeline(meta)
        kinds = [op.WhichOneof("op") for op in pipe.operators]
        assert kinds == ["filter", "transform", "aggregate"]


class TestTransformWrapperBehavior:
    """The generated wrapper is pure Python and can be exec'd inline to verify
    the list[dict] <-> pl.DataFrame adaptation without spinning up the engine."""

    def _exec_wrapper(self, node: PipelineNode):
        src = node._pre_ops[0]["transform"]["pycode"]["source_code"]
        namespace: dict = {}
        exec(src, namespace)
        return namespace["_thyme_polars_wrapper"]

    def test_wrapper_roundtrip_preserves_records(self):
        node = PipelineNode("Order").transform(identity)
        wrapper = self._exec_wrapper(node)
        records = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
        out = wrapper(records)
        assert out == records

    def test_wrapper_applies_user_transform(self):
        node = PipelineNode("Order").transform(tag_amount_band)
        wrapper = self._exec_wrapper(node)
        records = [
            {"amount": 5, "user_id": "u1"},
            {"amount": 50, "user_id": "u2"},
            {"amount": 500, "user_id": "u3"},
        ]
        out = wrapper(records)
        bands = [r["amount_band"] for r in out]
        assert bands == ["small", "medium", "large"]

    def test_wrapper_empty_batch_passthrough(self):
        node = PipelineNode("Order").transform(identity)
        wrapper = self._exec_wrapper(node)
        assert wrapper([]) == []

    def test_wrapper_rejects_non_dataframe_return(self):
        def bad_transform(df: pl.DataFrame) -> pl.DataFrame:
            return df.to_dicts()  # type: ignore[return-value]

        node = PipelineNode("Order").transform(bad_transform)
        wrapper = self._exec_wrapper(node)
        with pytest.raises(TypeError, match="must return pl.DataFrame"):
            wrapper([{"a": 1}])
