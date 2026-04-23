import inspect
import textwrap
from typing import Any, Callable, Dict, List, Optional

from thyme.expr import Expr, PredicateExpr
from thyme.expr._wire import proto_to_wire

# Wrapper template shipped alongside user UDF source. The engine sends a JSON
# array of records per batch to the Python worker; the worker calls this
# entry point, which adapts list[dict] <-> pl.DataFrame <-> list[dict] so the
# user's function can express itself in native Polars.
_POLARS_WRAPPER_TEMPLATE = """
import polars as pl

{user_source}

def _thyme_polars_wrapper(records):
    if not records:
        return records
    df = pl.DataFrame(records)
    result = {user_fn_name}(df)
    if not isinstance(result, pl.DataFrame):
        raise TypeError(
            "Polars UDF '{user_fn_name}' must return pl.DataFrame, got "
            + type(result).__name__
        )
    return result.to_dicts()
""".strip() + "\n"

_UDF_ENTRY_POINT = "_thyme_polars_wrapper"


def _capture_udf_source(fn: Callable) -> str:
    """Capture and dedent the user function's source. Raises TypeError on
    constructs we can't ship to the engine (lambdas, closures)."""
    name = getattr(fn, "__name__", None)
    if not name or name == "<lambda>":
        raise TypeError(
            ".transform() requires a named top-level function "
            "(lambdas are not supported because their source cannot be captured)"
        )
    if getattr(fn, "__closure__", None):
        raise TypeError(
            f".transform() rejects closures over non-global state "
            f"(function '{name}' has captured variables); define all "
            "dependencies as module-level constants or function arguments"
        )
    try:
        raw = inspect.getsource(fn)
    except (OSError, TypeError) as e:
        raise TypeError(
            f".transform() could not capture source for '{name}': {e}. "
            "UDFs must be defined at import-time in a real source file."
        )
    return textwrap.dedent(raw)


def _build_udf_pycode(fn: Callable) -> Dict[str, str]:
    """Generate the pycode payload shipped to the engine: user source plus a
    wrapper that adapts list[dict] <-> pl.DataFrame."""
    user_source = _capture_udf_source(fn)
    source_code = _POLARS_WRAPPER_TEMPLATE.format(
        user_source=user_source,
        user_fn_name=fn.__name__,
    )
    return {"source_code": source_code, "entry_point": _UDF_ENTRY_POINT}


class AggOp:
    """Base class for aggregation operators."""

    def __init__(self, of: str = "", window: str = ""):
        self.of = of
        self.window = window


class Avg(AggOp):
    """Average aggregation over a time window."""

    @property
    def agg_type(self) -> str:
        return "avg"


class Count(AggOp):
    """Count aggregation over a time window."""

    @property
    def agg_type(self) -> str:
        return "count"


class Sum(AggOp):
    """Sum aggregation over a time window."""

    @property
    def agg_type(self) -> str:
        return "sum"


class Min(AggOp):
    """Min aggregation over a time window."""

    @property
    def agg_type(self) -> str:
        return "min"


class Max(AggOp):
    """Max aggregation over a time window."""

    @property
    def agg_type(self) -> str:
        return "max"


class ApproxPercentile(AggOp):
    """Approximate percentile distribution over a time window using t-Digest.

    Stores a sketch of the value distribution, enabling percentile rank
    queries at read time. The materialized feature value is the percentile
    rank (0.0 to 1.0) of the latest event value within the distribution.
    """

    def __init__(self, of: str = "", window: str = "", precision: int = 100):
        super().__init__(of=of, window=window)
        self.precision = precision

    @property
    def agg_type(self) -> str:
        return "approx_percentile"


class PipelineNode:
    """Represents a step in a pipeline DAG."""

    def __init__(self, dataset_name: str):
        self.dataset_name = dataset_name
        self._group_keys: List[str] = []
        self._agg_specs: List[dict] = []
        self._join_specs: List[dict] = []
        # Pre-aggregate operators (filter, assign) applied before groupby/aggregate,
        # in insertion order. Each entry has a single key: "filter" or "assign".
        self._pre_ops: List[dict] = []

    def _clone(self) -> "PipelineNode":
        node = PipelineNode(self.dataset_name)
        node._group_keys = list(self._group_keys)
        node._agg_specs = list(self._agg_specs)
        node._join_specs = list(self._join_specs)
        node._pre_ops = list(self._pre_ops)
        return node

    def filter(self, predicate: PredicateExpr) -> "PipelineNode":
        """Drop records that do not satisfy the predicate (evaluated in the engine)."""
        if not isinstance(predicate, PredicateExpr):
            raise TypeError(
                f"filter() expects a PredicateExpr from thyme.expr, got {type(predicate).__name__}"
            )
        node = self._clone()
        # Store the raw proto for MockContext evaluation and also a wire-shaped
        # dict for JSON transmission to the definition service.
        proto = predicate.to_proto()
        node._pre_ops.append({
            "filter": {
                "predicate": proto,
                "_wire": {"predicate": proto_to_wire(proto)},
            },
        })
        return node

    def transform(
        self,
        fn: Callable,
        *,
        output_columns: Optional[Dict[str, type]] = None,
    ) -> "PipelineNode":
        """Apply a Polars UDF to each batch of records (opt-in escape hatch for
        logic that expressions can't express).

        The function must be a named top-level callable with signature
        ``(pl.DataFrame) -> pl.DataFrame`` and must preserve row count (use
        ``.filter(...)`` for length-changing operations in Stage 1). Source is
        captured via ``inspect.getsource`` and shipped to the engine's Python
        worker pool; the engine pods must have ``THYME_ENGINE_PYTHON_WORKERS``
        set for UDF pipelines to run.

        ``output_columns`` is a schema hint for the columns the UDF adds or
        overwrites (e.g. ``{"amount_band": str}``). Stage 1 stores the hint
        for downstream tooling; runtime schema validation is Stage 2.
        """
        if not callable(fn):
            raise TypeError(
                f".transform() expects a callable, got {type(fn).__name__}"
            )
        pycode = _build_udf_pycode(fn)
        op_entry = {
            "transform": {
                "pycode": pycode,
                "output_columns": {k: getattr(t, "__name__", str(t)) for k, t in (output_columns or {}).items()},
                "_wire": {"pycode": pycode},
            },
        }
        node = self._clone()
        node._pre_ops.append(op_entry)
        return node

    def assign(self, **kwargs: Expr) -> "PipelineNode":
        """Add or overwrite columns with derivation expressions.

        Each kwarg ``name=expr`` becomes a separate Assign op emitted in
        declaration order.
        """
        node = self._clone()
        for column, expr in kwargs.items():
            if not isinstance(expr, Expr):
                raise TypeError(
                    f"assign({column}=...) expects an Expr from thyme.expr, got {type(expr).__name__}"
                )
            proto = expr.to_proto()
            node._pre_ops.append({
                "assign": {
                    "column": column,
                    "value": proto,
                    "_wire": {"column": column, "value": proto_to_wire(proto)},
                },
            })
        return node

    def join(self, right_dataset: Any, *, on: str, fields: List[str] | None = None) -> "PipelineNode":
        """Temporal join: enrich each left record with the most recent right-side state."""
        node = self._clone()
        right_name = right_dataset.__name__ if hasattr(right_dataset, "__name__") else str(right_dataset)
        node._join_specs.append({
            "right_dataset": right_name,
            "left_key_field": on,
            "select_fields": fields or [],
        })
        return node

    def groupby(self, *keys: str) -> "PipelineNode":
        node = self._clone()
        node._group_keys = list(keys)
        return node

    def aggregate(self, **kwargs: AggOp) -> "PipelineNode":
        node = self._clone()
        for output_field, op in kwargs.items():
            if not isinstance(op, AggOp):
                raise TypeError(f"Expected AggOp, got {type(op).__name__}")
            spec = {
                "type": op.agg_type,
                "field": op.of,
                "window": op.window,
                "output_field": output_field,
            }
            if hasattr(op, "precision"):
                spec["precision"] = op.precision
            node._agg_specs.append(spec)
        return node

    def to_operators(self) -> List[dict]:
        """Serialise this node into an ordered list of operator dicts.

        Emit order: pre_ops (filter/assign in insertion order) → joins → aggregate.
        """
        operators: List[dict] = list(self._pre_ops)
        for join_spec in self._join_specs:
            operators.append({"temporal_join": join_spec})
        if self._agg_specs:
            operators.append({
                "aggregate": {
                    "keys": self._group_keys,
                    "specs": self._agg_specs,
                },
            })
        return operators


class Pipeline:
    """Metadata for a pipeline method on a dataset."""

    def __init__(self, func: Callable, version: int, input_datasets: List[str]):
        self.func = func
        self.name = func.__name__
        self.version = version
        self.input_datasets = input_datasets
        self.source_code = inspect.getsource(func)
        self._node: PipelineNode | None = None

    def get_operators(self) -> List[dict]:
        """Execute the pipeline function to extract operator specs."""
        if self._node is None:
            input_node = PipelineNode(self.input_datasets[0] if self.input_datasets else "")
            # Support both plain functions and bound/unbound methods.
            raw = getattr(self.func, "__func__", self.func)
            self._node = raw(None, input_node)
        if self._node:
            return self._node.to_operators()
        return []


def pipeline(version: int = 1):
    """Decorator for pipeline methods on a dataset class."""

    def wrapper(func: Callable) -> Callable:
        func._is_pipeline = True
        func._pipeline_version = version
        return func

    return wrapper


def inputs(*dataset_classes: Any) -> Callable:
    """Decorator to specify input datasets for a pipeline."""

    def wrapper(func: Callable) -> Callable:
        func._pipeline_inputs = [
            cls.__name__ if hasattr(cls, "__name__") else str(cls)
            for cls in dataset_classes
        ]
        return func

    return wrapper
