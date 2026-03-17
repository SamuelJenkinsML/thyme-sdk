import inspect
from typing import Any, Callable, List


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


class PipelineNode:
    """Represents a step in a pipeline DAG."""

    def __init__(self, dataset_name: str):
        self.dataset_name = dataset_name
        self._group_keys: List[str] = []
        self._agg_specs: List[dict] = []

    def groupby(self, *keys: str) -> "PipelineNode":
        node = PipelineNode(self.dataset_name)
        node._group_keys = list(keys)
        return node

    def aggregate(self, **kwargs: AggOp) -> "PipelineNode":
        node = PipelineNode(self.dataset_name)
        node._group_keys = list(self._group_keys)
        for output_field, op in kwargs.items():
            if not isinstance(op, AggOp):
                raise TypeError(f"Expected AggOp, got {type(op).__name__}")
            node._agg_specs.append({
                "type": op.agg_type,
                "field": op.of,
                "window": op.window,
                "output_field": output_field,
            })
        return node


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
        if self._node and self._node._agg_specs:
            return [{
                "aggregate": {
                    "keys": self._node._group_keys,
                    "specs": self._node._agg_specs,
                }
            }]
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
