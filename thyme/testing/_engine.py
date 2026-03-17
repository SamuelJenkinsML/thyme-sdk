"""In-memory pipeline engine — mirrors crates/engine/src/operators.rs semantics."""

from __future__ import annotations

import dataclasses
from datetime import datetime, timezone
from typing import Any

from thyme.testing._expectations import ExpectationViolation, check_expectations
from thyme.testing._window import parse_window_duration


# ---------------------------------------------------------------------------
# Internal data structures
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class _EventEntry:
    timestamp_secs: int
    values: dict[str, float]


class _GroupState:
    """Accumulates events for one group key in one pipeline."""

    def __init__(self) -> None:
        self.events: list[_EventEntry] = []

    def add_event(self, timestamp_secs: int, values: dict[str, float]) -> None:
        self.events.append(_EventEntry(timestamp_secs=timestamp_secs, values=values))

    def compute_aggregate(self, agg_spec: dict, reference_time: int) -> float:
        """Compute one aggregate over events within the window.

        Mirrors the Rust engine's semantics:
        - count: number of events in window
        - sum: sum of field values in window
        - avg: sum / count (0.0 if empty)
        - min: minimum field value in window (0.0 if empty)
        - max: maximum field value in window (0.0 if empty)
        """
        window_secs = parse_window_duration(agg_spec["window"])
        cutoff = (reference_time - window_secs) if window_secs > 0 else 0

        in_window = [e for e in self.events if e.timestamp_secs >= cutoff]

        agg_type = agg_spec["type"]
        field_name = agg_spec.get("field", "")

        if agg_type == "count":
            return float(len(in_window))

        values = [e.values.get(field_name, 0.0) for e in in_window]
        if not values:
            return 0.0

        if agg_type == "sum":
            return sum(values)
        elif agg_type == "avg":
            return sum(values) / len(values)
        elif agg_type == "min":
            return min(values)
        elif agg_type == "max":
            return max(values)
        else:
            raise ValueError(f"Unknown aggregation type: {agg_type}")


# ---------------------------------------------------------------------------
# Timestamp parsing
# ---------------------------------------------------------------------------


def _parse_timestamp_secs(ts: Any) -> int:
    """Convert a timestamp value to epoch seconds.

    Accepts: datetime objects, ISO 8601 strings, or numeric epoch seconds.
    """
    if isinstance(ts, datetime):
        return int(ts.timestamp())
    if isinstance(ts, (int, float)):
        return int(ts)
    if isinstance(ts, str):
        s = ts.strip()
        # ISO 8601 with Z suffix
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    raise ValueError(f"Cannot parse timestamp: {ts!r}")


# ---------------------------------------------------------------------------
# MockContext
# ---------------------------------------------------------------------------


class MockContext:
    """In-memory pipeline simulator. No infrastructure required.

    Usage::

        ctx = MockContext()
        violations = ctx.add_events(Review, [
            {"restaurant_id": "r1", "rating": 4.8, "timestamp": "2026-03-15T10:00:00Z"},
        ])
        result = ctx.get_aggregates(RestaurantRatingStats, "r1")
        features = ctx.query(RestaurantFeatures, "r1")
    """

    def __init__(self) -> None:
        # (output_dataset, pipeline_name, group_key_str) -> _GroupState
        self._group_states: dict[tuple[str, str, str], _GroupState] = {}

    def add_events(
        self,
        dataset_class: type,
        events: list[dict],
    ) -> list[ExpectationViolation]:
        """Ingest events for a dataset. Processes through all registered pipelines.

        Returns expectation violations (events are still processed — observational only).
        """
        from thyme.dataset import _DATASET_REGISTRY, _PIPELINE_REGISTRY

        ds_name = dataset_class.__name__
        ds_meta = _DATASET_REGISTRY.get(ds_name)
        if ds_meta is None:
            raise ValueError(f"Dataset '{ds_name}' not found in registry. Did you decorate it with @dataset?")

        # Find timestamp field name
        ts_field = None
        for f in ds_meta["fields"]:
            if f.get("timestamp"):
                ts_field = f["name"]

        # Evaluate expectations
        violations = check_expectations(ds_meta.get("expectations", []), events)

        # Find pipelines that consume this dataset
        pipelines = []
        for (_out_ds, _pipe_name), pipe_meta in _PIPELINE_REGISTRY.items():
            if ds_name in pipe_meta.get("input_datasets", []):
                pipelines.append(pipe_meta)

        # Process events through each pipeline
        for pipe_meta in pipelines:
            operators = pipe_meta.get("operators", [])
            if not operators:
                continue

            for op in operators:
                agg_op = op.get("aggregate")
                if agg_op is None:
                    continue

                group_keys = agg_op["keys"]
                agg_specs = agg_op["specs"]
                output_dataset = pipe_meta["output_dataset"]
                pipe_name = pipe_meta["name"]

                for event in events:
                    # Build group key string
                    group_key_str = ":".join(str(event.get(k, "")) for k in group_keys)

                    # Parse timestamp
                    ts_val = event.get(ts_field) if ts_field else None
                    ts_secs = _parse_timestamp_secs(ts_val) if ts_val is not None else 0

                    # Collect all field values referenced by agg specs
                    values: dict[str, float] = {}
                    for spec in agg_specs:
                        field_name = spec.get("field", "")
                        if field_name and field_name in event:
                            val = event[field_name]
                            if isinstance(val, (int, float)):
                                values[field_name] = float(val)

                    # Store in group state
                    state_key = (output_dataset, pipe_name, group_key_str)
                    if state_key not in self._group_states:
                        self._group_states[state_key] = _GroupState()
                    self._group_states[state_key].add_event(ts_secs, values)

        return violations

    def get_aggregates(
        self,
        dataset_class: type,
        entity_id: str,
    ) -> dict[str, Any]:
        """Return raw aggregated values for an entity from all pipelines on the dataset.

        Returns {output_field: value} dict. Missing entities return zeros.
        """
        from thyme.dataset import _PIPELINE_REGISTRY

        output_ds = dataset_class.__name__
        result: dict[str, Any] = {}

        for (_out_ds, _pipe_name), pipe_meta in _PIPELINE_REGISTRY.items():
            if pipe_meta["output_dataset"] != output_ds:
                continue

            pipe_name = pipe_meta["name"]
            operators = pipe_meta.get("operators", [])

            for op in operators:
                agg_op = op.get("aggregate")
                if agg_op is None:
                    continue

                agg_specs = agg_op["specs"]

                # The entity_id IS the group key for single-key groupby
                state_key = (output_ds, pipe_name, entity_id)
                group = self._group_states.get(state_key)

                if group is None or not group.events:
                    for spec in agg_specs:
                        result[spec["output_field"]] = 0.0
                    continue

                # Reference time = max timestamp in this group
                ref_time = max(e.timestamp_secs for e in group.events)

                for spec in agg_specs:
                    result[spec["output_field"]] = group.compute_aggregate(spec, ref_time)

        return result

    def query(
        self,
        featureset_class: type,
        entity_id: str,
    ) -> dict[str, Any]:
        """Query features for an entity, running extractors.

        - Seeds features from pipeline aggregates (for extractors with deps)
        - Runs pure-transform extractors (no deps) directly
        """
        from thyme.featureset import _FEATURESET_REGISTRY

        fs_name = featureset_class.__name__
        fs_meta = _FEATURESET_REGISTRY.get(fs_name)
        if fs_meta is None:
            raise ValueError(f"Featureset '{fs_name}' not found in registry.")

        features: dict[str, Any] = {}
        extractors = fs_meta.get("extractors", [])

        # Phase 1: Process extractors with deps — seed from pipeline aggregates
        for ext in extractors:
            if not ext.get("deps"):
                continue
            for dep_name in ext["deps"]:
                # Find the dataset class in the registry and get aggregates
                aggregates = self._get_aggregates_by_name(dep_name, entity_id)
                # Map aggregate output fields to extractor output features
                for output_feat in ext["outputs"]:
                    if output_feat in aggregates:
                        features[output_feat] = aggregates[output_feat]

        # Phase 2: Run pure-transform extractors (no deps)
        for ext in extractors:
            if ext.get("deps"):
                continue

            # Get the actual method from the featureset class
            method = getattr(featureset_class, ext["name"], None)
            if method is None:
                continue

            # Gather input values
            input_vals = [features.get(name, 0.0) for name in ext["inputs"]]

            # Call the extractor: (cls, ts, *inputs)
            result = method(None, None, *input_vals)

            # Store outputs
            outputs = ext["outputs"]
            if len(outputs) == 1:
                features[outputs[0]] = result
            else:
                # Multi-output: result should be iterable
                for i, out_name in enumerate(outputs):
                    features[out_name] = result[i]

        return features

    def _get_aggregates_by_name(
        self,
        dataset_name: str,
        entity_id: str,
    ) -> dict[str, Any]:
        """Like get_aggregates but takes a dataset name string instead of a class."""
        from thyme.dataset import _PIPELINE_REGISTRY

        result: dict[str, Any] = {}

        for (_out_ds, _pipe_name), pipe_meta in _PIPELINE_REGISTRY.items():
            if pipe_meta["output_dataset"] != dataset_name:
                continue

            pipe_name = pipe_meta["name"]
            operators = pipe_meta.get("operators", [])

            for op in operators:
                agg_op = op.get("aggregate")
                if agg_op is None:
                    continue

                agg_specs = agg_op["specs"]
                state_key = (dataset_name, pipe_name, entity_id)
                group = self._group_states.get(state_key)

                if group is None or not group.events:
                    for spec in agg_specs:
                        result[spec["output_field"]] = 0.0
                    continue

                ref_time = max(e.timestamp_secs for e in group.events)
                for spec in agg_specs:
                    result[spec["output_field"]] = group.compute_aggregate(spec, ref_time)

        return result
