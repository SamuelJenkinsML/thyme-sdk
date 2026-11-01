"""In-memory pipeline engine — mirrors crates/engine/src/operators.rs semantics."""

from __future__ import annotations

import dataclasses
from datetime import datetime, timezone
from typing import Any

from thyme.testing._expectations import ExpectationViolation, check_expectations
from thyme.testing._expr_eval import apply_pre_ops, eval_predicate
from thyme.testing._window import parse_window_duration


# ---------------------------------------------------------------------------
# Internal data structures
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class _EventEntry:
    timestamp: float
    values: dict[str, float]
    # Output fields of the specs whose predicate this event passed. The engine
    # decides once at ingest, so a later change to the event cannot alter it.
    matched: frozenset[str]

    @property
    def timestamp_secs(self) -> int:
        return int(self.timestamp)


class _GroupState:
    """Accumulates events for one group key in one pipeline."""

    def __init__(self) -> None:
        self.events: list[_EventEntry] = []

    def add_event(
        self, timestamp: float, values: dict[str, float], matched: frozenset[str],
    ) -> None:
        self.events.append(
            _EventEntry(timestamp=timestamp, values=values, matched=matched)
        )

    def compute_aggregate(
        self, agg_spec: dict, reference_time: int, *, upper_bound: float | None = None,
    ) -> float | str | list | None:
        """Compute one aggregate over events within the window.

        Mirrors the Rust engine's semantics. A window of W seconds holds the W
        whole seconds ending at reference_time, so an event exactly W back has
        expired. An empty window is unbounded. The spec's predicate gates every
        aggregation type. Only count, sum and avg follow the engine exactly at
        the window's edge: it retires the others by max_lateness, which this
        executor does not model.
        - count: number of events in window
        - sum: sum of field values in window
        - avg: sum / count (0.0 if empty)
        - min: minimum field value in window (0.0 if empty)
        - max: maximum field value in window (0.0 if empty)
        - last: most-recent in-window value by event time (None if empty)
        - last_k: the k most-recent values, newest-first (dedup/dropnull honored)

        Args:
            agg_spec: Aggregation specification dict.
            reference_time: The reference time for windowing (epoch seconds).
            upper_bound: If set, only include events with timestamp <= upper_bound.
                         Used for point-in-time correct offline queries. Keeps
                         its fraction: an event later in the read's own second
                         has not happened yet.
        """
        window_secs = parse_window_duration(agg_spec["window"])

        in_window = self.events
        if window_secs > 0:
            in_window = [
                e for e in in_window if e.timestamp_secs > reference_time - window_secs
            ]
        if upper_bound is not None:
            in_window = [e for e in in_window if e.timestamp <= upper_bound]

        in_window = [e for e in in_window if agg_spec["output_field"] in e.matched]

        agg_type = agg_spec["type"]
        field_name = agg_spec.get("field", "")

        if agg_type == "count":
            return float(len(in_window))

        if agg_type in ("last", "last_k"):
            # Newest-first by (event time, insertion order); preserve raw values.
            ordered = sorted(
                enumerate(in_window),
                key=lambda p: (p[1].timestamp_secs, p[0]),
                reverse=True,
            )
            dropnull = agg_spec.get("dropnull", False)
            raw = []
            for _, e in ordered:
                v = e.values.get(field_name, None)
                if dropnull and v is None:
                    continue
                raw.append(v)
            if agg_type == "last":
                return raw[0] if raw else None
            k = agg_spec.get("k", 100)
            if agg_spec.get("dedup", False):
                out: list = []
                for v in raw:
                    if v not in out:
                        out.append(v)
                    if len(out) == k:
                        break
                return out
            return raw[:k]

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
        elif agg_type == "approx_percentile":
            # For testing: compute exact percentile rank of the latest value
            # in the distribution of all values in the window.
            if len(values) <= 1:
                return 0.5
            latest_value = values[-1]
            sorted_vals = sorted(values)
            rank = sum(1 for v in sorted_vals if v < latest_value)
            return rank / len(sorted_vals)
        else:
            raise ValueError(f"Unknown aggregation type: {agg_type}")


# ---------------------------------------------------------------------------
# Timestamp parsing
# ---------------------------------------------------------------------------


def _parse_timestamp(ts: Any) -> float:
    """Convert a timestamp value to epoch seconds, keeping any fraction.

    Accepts: datetime objects, ISO 8601 strings, or numeric epoch seconds.
    """
    if isinstance(ts, datetime):
        return ts.timestamp()
    if isinstance(ts, (int, float)):
        return float(ts)
    if isinstance(ts, str):
        s = ts.strip()
        # ISO 8601 with Z suffix
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
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
        # Polars-backed storage: (output_dataset, pipeline_name) -> pl.DataFrame
        # Columns: [group_key, timestamp_secs, *value_fields]
        self._polars_states: dict[tuple[str, str], list[dict]] = {}

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

            # Apply pre-aggregate ops (filter, assign) in declaration order
            # before any aggregation step sees the events. Mirrors the Rust
            # engine's process_record path.
            pre_ops = [op for op in operators if "filter" in op or "assign" in op]
            processed_events = apply_pre_ops(events, pre_ops) if pre_ops else events

            for op in operators:
                agg_op = op.get("aggregate")
                if agg_op is None:
                    continue

                group_keys = agg_op["keys"]
                agg_specs = agg_op["specs"]
                output_dataset = pipe_meta["output_dataset"]
                pipe_name = pipe_meta["name"]

                for event in processed_events:
                    # Build group key string
                    group_key_str = ":".join(str(event.get(k, "")) for k in group_keys)

                    # Parse timestamp
                    ts_val = event.get(ts_field) if ts_field else None
                    timestamp = _parse_timestamp(ts_val) if ts_val is not None else 0.0

                    # Collect all field values referenced by agg specs. Numeric
                    # values are coerced to float (sum/avg/min/max); Last/LastK
                    # keep the raw value (e.g. strings) so it round-trips.
                    values: dict[str, object] = {}
                    for spec in agg_specs:
                        field_name = spec.get("field", "")
                        if field_name and field_name in event:
                            val = event[field_name]
                            if isinstance(val, bool):
                                values[field_name] = val
                            elif isinstance(val, (int, float)):
                                values[field_name] = float(val)
                            else:
                                values[field_name] = val

                    matched = frozenset(
                        spec["output_field"] for spec in agg_specs
                        if spec.get("predicate") is None
                        or eval_predicate(spec["predicate"], event)
                    )

                    # Store in group state (legacy dict-based)
                    state_key = (output_dataset, pipe_name, group_key_str)
                    if state_key not in self._group_states:
                        self._group_states[state_key] = _GroupState()
                    self._group_states[state_key].add_event(timestamp, values, matched)

                    # Store in Polars layer
                    polars_key = (output_dataset, pipe_name)
                    if polars_key not in self._polars_states:
                        self._polars_states[polars_key] = []
                    polars_row = {
                        "group_key": group_key_str,
                        "timestamp_secs": int(timestamp),
                    }
                    polars_row.update(values)
                    self._polars_states[polars_key].append(polars_row)

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

    def add_events_df(
        self,
        dataset_class: type,
        events: Any,
    ) -> list[ExpectationViolation]:
        """Ingest events from a Polars DataFrame. Delegates to add_events.

        Args:
            dataset_class: The dataset class to ingest into.
            events: A Polars DataFrame with the same columns as list[dict] events.

        Returns:
            List of expectation violations.
        """
        return self.add_events(dataset_class, events.to_dicts())

    def get_aggregates_df(
        self,
        dataset_class: type,
    ) -> Any:
        """Return aggregated values for ALL entities as a Polars DataFrame.

        Returns a DataFrame with columns: [entity_id, *output_fields].
        """
        import polars as pl

        output_ds = dataset_class.__name__
        rows: list[dict[str, Any]] = []
        seen_entities: set[str] = set()

        # Collect all entity IDs for this dataset
        for (out_ds, pipe_name, group_key_str), group in self._group_states.items():
            if out_ds != output_ds:
                continue
            seen_entities.add(group_key_str)

        # Compute aggregates for each entity
        for entity_id in sorted(seen_entities):
            agg = self.get_aggregates(dataset_class, entity_id)
            agg["entity_id"] = entity_id
            rows.append(agg)

        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows)

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

            # Store outputs. Match the engine's wrapper semantics
            # (crates/query-server/src/python.rs): a dict return is indexed by
            # output name; a scalar return on a single-output extractor is
            # auto-wrapped; a tuple/list return on a multi-output extractor
            # is treated as positional.
            outputs = ext["outputs"]
            if isinstance(result, dict):
                for out_name in outputs:
                    features[out_name] = result[out_name]
            elif len(outputs) == 1:
                features[outputs[0]] = result
            else:
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

    def _get_aggregates_at_time(
        self,
        dataset_name: str,
        entity_id: str,
        at_timestamp: float,
    ) -> dict[str, Any]:
        """Like _get_aggregates_by_name but with point-in-time semantics.

        Only considers events with timestamp <= at_timestamp.
        Uses at_timestamp's second as the reference time for windowing.
        """
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

                for spec in agg_specs:
                    result[spec["output_field"]] = group.compute_aggregate(
                        spec, int(at_timestamp), upper_bound=at_timestamp,
                    )

        return result

    def _query_at_time(
        self,
        featureset_class: type,
        entity_id: str,
        at_timestamp: float,
    ) -> dict[str, Any]:
        """Like query() but with point-in-time semantics."""
        from thyme.featureset import _FEATURESET_REGISTRY

        fs_name = featureset_class.__name__
        fs_meta = _FEATURESET_REGISTRY.get(fs_name)
        if fs_meta is None:
            raise ValueError(f"Featureset '{fs_name}' not found in registry.")

        features: dict[str, Any] = {}
        extractors = fs_meta.get("extractors", [])

        # Phase 1: extractors with deps — seed from PIT aggregates
        for ext in extractors:
            if not ext.get("deps"):
                continue
            for dep_name in ext["deps"]:
                aggregates = self._get_aggregates_at_time(
                    dep_name, entity_id, at_timestamp,
                )
                for output_feat in ext["outputs"]:
                    if output_feat in aggregates:
                        features[output_feat] = aggregates[output_feat]

        # Phase 2: pure-transform extractors (no deps)
        for ext in extractors:
            if ext.get("deps"):
                continue

            method = getattr(featureset_class, ext["name"], None)
            if method is None:
                continue

            input_vals = [features.get(name, 0.0) for name in ext["inputs"]]
            result = method(None, None, *input_vals)

            # Match the engine's wrapper (crates/query-server/src/python.rs):
            # dict → index by name; scalar on single-output → auto-wrap;
            # tuple/list on multi-output → positional.
            outputs = ext["outputs"]
            if isinstance(result, dict):
                for out_name in outputs:
                    features[out_name] = result[out_name]
            elif len(outputs) == 1:
                features[outputs[0]] = result
            else:
                for i, out_name in enumerate(outputs):
                    features[out_name] = result[i]

        return features

    def query_offline(
        self,
        featureset_class: type,
        entities: list[dict],
        *,
        entity_column: str = "entity_id",
        timestamp_column: str = "timestamp",
    ) -> Any:
        """Point-in-time correct batch feature extraction.

        Args:
            featureset_class: A @featureset-decorated class.
            entities: List of dicts with entity_column and timestamp_column keys.
            entity_column: Column name for entity IDs.
            timestamp_column: Column name for timestamps.

        Returns:
            ThymeResult with entity/timestamp columns plus feature columns.
        """
        import polars as pl
        from thyme.result import ThymeResult

        rows: list[dict[str, Any]] = []
        for entity in entities:
            eid = entity[entity_column]
            ts_val = entity[timestamp_column]
            at_timestamp = _parse_timestamp(ts_val)

            features = self._query_at_time(featureset_class, eid, at_timestamp)
            row = {entity_column: eid, timestamp_column: ts_val}
            row.update(features)
            rows.append(row)

        if not rows:
            df = pl.DataFrame()
        else:
            df = pl.DataFrame(rows)

        return ThymeResult(df, metadata={"mode": "offline"})
