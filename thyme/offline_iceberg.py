"""The as-of read over the Iceberg offline store (TH-312).

Training data and EDA read the Iceberg tables **directly**, not through
query-server. `POST /features/offline` resolves a spine with a serial per-row
loop inside the serving process, which is a correct point-in-time API and not a
path for 10^5-10^8 rows.

Here the whole spine is resolved by **one** DuckDB `ASOF JOIN` per entity type.
That inverts the cost model measured in TH-310: a single-row Iceberg seek opens
~15 objects to the shipped Parquet reader's ~1, but those opens amortise across
a scan, and a scan is what a training pull actually is.

DuckDB rather than PyIceberg, and this is not a preference. PyIceberg is `N` for
"Read with equality deletes" in the Iceberg support matrix, and Polars delegates
its Iceberg scan to PyIceberg — so after a backfill, either would return
superseded rows *quietly*. DuckDB has its own C++ implementation and applies
them. A `scan_iceberg` anywhere in this path is a correctness bug.

Two properties decide whether a large pull takes minutes or hours, and both
regress silently, so `tests/test_offline_iceberg.py` asserts them directly:
project only the needed columns, and never call Python per row.
"""

from __future__ import annotations

from typing import Any, Literal

# Columns the sink writes that a training pull must never read.
#
# `state_value` is the raw StateValue proto (~200-500 B/row) and dwarfs the
# flattened feature doubles (~40 B/row all told). Parquet is columnar, so
# skipping it skips its bytes entirely — on a ~700M row history that is the
# difference between a ~28 GB scan and a ~170-380 GB one, for no effort beyond
# not writing `SELECT *`.
#
# Note the asymmetry with the Rust reader, which *must* return `state_value`
# because its contract is byte parity with RocksDB. The two paths optimise for
# different things, which is one more reason not to route training pulls through
# query-server. Names mirror offline-sink/internal/sink/schema.go.
BLOB_COLUMNS = frozenset(
    {"state_value", "sketches", "dimensions", "json_features", "op", "event_time", "known_at"}
)

#: The entity key and ordering key in every offline table.
ENTITY_ID_COLUMN = "entity_id"
TS_COLUMN = "ts"

#: The Iceberg partition source: the table is `partition by days(event_time)`.
#: Pruning must reference this column, not `ts` — a predicate on the string
#: cannot prune partitions, and partition pruning is what turns a full-history
#: scan into a bounded one.
PARTITION_COLUMN = "event_time"

#: Commit-time knowledge clock written by offline-sink (nullable on pre-existing tables).
KNOWN_AT_COLUMN = "known_at"

# Safety margin on the upper prune bound, and it is load-bearing.
#
# `event_time` is the *parsed* form of `ts`, but the join orders on the raw
# string, and the two orders disagree. "…T09:14:02.5Z" sorts BELOW "…T09:14:02Z"
# byte-wise (`.` = 0x2E < `Z` = 0x5A) while being 500 ms later in time. So a row
# that byte-order accepts as a valid as-of answer can sit slightly *after* the
# spine's newest timestamp in real time, and pruning on `event_time` alone would
# drop it.
#
# One second covers it: the divergence is confined to the sub-second component,
# since everything above it is fixed-width and byte order matches time order
# there. Widening the prune window by a second costs nothing — it is at most one
# extra partition at the boundary, and only when the spine ends on a day edge.
_PRUNE_MARGIN = "1 second"

#: How far back history is read by default.
#:
#: An as-of read must find the newest row at or before each spine timestamp, so
#: an entity that last updated two years ago would otherwise force two years of
#: scan. Bounding it makes scan volume a function of the spine's span rather
#: than of the table's whole history.
#:
#: It is a semantic choice, not only a tuning one: with a bound, the answer is
#: "the value as of `ts`, **provided it was updated within the window**", and an
#: entity dormant for longer resolves to null rather than to its last known
#: value. Pass ``max_lookback=None`` to opt out and read all history.
DEFAULT_MAX_LOOKBACK = "90 days"

# The columns the sink writes for an aggregation's share; see
# offline-sink/internal/decode.
SHARE_SUM = "__share_sum"
SHARE_COUNT = "__share_count"
WINDOWED_TYPES = ("count", "sum", "avg")
NUMERIC_DTYPES = ("int", "float", "bool")

# Last and LastK have no column: the sink keeps their value as JSON text in this
# map, a scalar for a Last and a newest-first array for a LastK.
LATEST_TYPES = ("last", "last_k")
JSON_FEATURES_COLUMN = "json_features"
_DUCKDB_TYPES = {"str": "VARCHAR", "int": "BIGINT", "float": "DOUBLE", "bool": "BOOLEAN"}


def _quote_ident(name: str) -> str:
    """Quote a SQL identifier, doubling any embedded quote."""
    return '"' + name.replace('"', '""') + '"'


def stored_columns(featureset_meta: dict) -> dict[str, list[tuple[str, str]]]:
    """Where a featureset's stored features live: output dataset -> (feature, column).

    The sink names a table after a pipeline's output dataset, so a featureset's
    own name is not a table, and one featureset can read several. A feature
    with no LOOKUP extractor is not stored: it is derived, sent with the
    request, or the entity key. The store holds numeric features as columns
    and Last and LastK in `json_features`; anything else has nothing to ask for.
    """
    numeric = {
        f["name"]
        for f in featureset_meta.get("features", [])
        if f.get("dtype") in NUMERIC_DTYPES
    }
    by_dataset: dict[str, list[tuple[str, str]]] = {}
    for extractor in featureset_meta.get("extractors", []):
        info = extractor.get("lookup_info")
        if extractor.get("kind") != "LOOKUP" or not info:
            continue
        feature, field, dataset = extractor["outputs"][0], info["field_name"], info["dataset_name"]
        if field in BLOB_COLUMNS:
            continue
        # By the pipeline, not by dtype: a Last of a float is numeric and still
        # has no column.
        if field in latest_columns(dataset) or feature in numeric:
            by_dataset.setdefault(dataset, []).append((feature, field))
    return by_dataset


def windowed_columns(dataset: str) -> dict[str, tuple[str, int]]:
    """`dataset`'s windowed Count, Sum and Avg columns: name -> (type, window seconds)."""
    from thyme.dataset import _PIPELINE_REGISTRY
    from thyme.testing._window import parse_window_duration

    return {
        spec["output_field"]: (spec["type"], parse_window_duration(spec["window"]))
        for pipeline in _PIPELINE_REGISTRY.values()
        if pipeline["output_dataset"] == dataset
        for op in pipeline["operators"]
        for spec in op.get("aggregate", {}).get("specs", [])
        if spec["type"] in WINDOWED_TYPES and spec.get("window")
    }


def latest_columns(dataset: str) -> dict[str, str | None]:
    """`dataset`'s Last and LastK columns: name -> the DuckDB type the pull
    returns, or None to return the JSON text.

    The type is the aggregated source field's, a list of it for a LastK. A field
    the pipeline made itself has no declared type.
    """
    from thyme.dataset import _DATASET_REGISTRY, _PIPELINE_REGISTRY

    out: dict[str, str | None] = {}
    for pipeline in _PIPELINE_REGISTRY.values():
        if pipeline["output_dataset"] != dataset:
            continue
        declared = {
            f["name"]: f.get("type")
            for name in pipeline.get("input_datasets", [])
            for f in _DATASET_REGISTRY.get(name, {}).get("fields", [])
        }
        for op in pipeline["operators"]:
            for spec in op.get("aggregate", {}).get("specs", []):
                if spec["type"] not in LATEST_TYPES:
                    continue
                element = _DUCKDB_TYPES.get(declared.get(spec.get("field")))
                if element is not None and spec["type"] == "last_k":
                    element += "[]"
                out[spec["output_field"]] = element
    return out


def derived_features(featureset_meta: dict) -> list[str]:
    """Features computed by an extractor rather than read from a table."""
    return [
        f["name"]
        for f in featureset_meta.get("features", [])
        if f.get("deps") and not f.get("request")
    ]


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _quote_table(table: str) -> str:
    return ".".join(_quote_ident(part) for part in table.split("."))


def _check_table(
    con: Any, dataset: str, table: str, columns: list[str], windows: dict, latest: dict
) -> bool:
    """Refuse a table the pull cannot read correctly, by name.

    Reading stored totals where the table has shares would not fail: a late row
    has no total, so its contribution would be lost with no error.
    Returns whether the table carries the `known_at` column.
    """
    try:
        have = {row[0] for row in con.execute(f"DESCRIBE {_quote_table(table)}").fetchall()}
    except Exception as e:
        raise ValueError(
            f"no offline table {table} for dataset {dataset!r}; "
            f"the sink creates it when the pipeline first writes: {e}"
        ) from e
    wanted = [c for c in columns if c not in latest]
    wanted += [c for c, _ in _share_columns({c: w for c, w in windows.items() if c in columns})]
    if set(columns) & set(latest):
        wanted.append(JSON_FEATURES_COLUMN)
    missing = sorted(set(wanted) - have)
    if missing:
        raise ValueError(f"table {table} has no column {missing} for dataset {dataset!r}")
    unknown = sorted(
        c for c in columns if c not in windows and {c + SHARE_SUM, c + SHARE_COUNT} & have
    )
    if unknown:
        raise ValueError(
            f"table {table} holds shares for {unknown} but no pipeline in this process "
            f"declares {dataset!r}'s windows; import the module that defines it"
        )
    return KNOWN_AT_COLUMN in have


def resolve_featureset(
    con: Any,
    *,
    featureset_meta: dict,
    catalog: Any,
    spine: Any,
    entity_column: str,
    timestamp_column: str,
    max_lookback: str | None = DEFAULT_MAX_LOOKBACK,
    as_of: Literal["event", "known"] = "event",
) -> Any:
    """Resolve a spine against every table a featureset reads; returns a relation.

    The result has the spine's two columns and one column per stored feature,
    under the feature's name. One dataset stays unevaluated. Several are read
    here and joined, because each dataset's as-of join replaces the same spine
    table.
    """
    by_dataset = stored_columns(featureset_meta)
    if not by_dataset:
        raise ValueError(
            f"featureset {featureset_meta['name']!r} has no stored features to read."
        )

    keys = f"{_quote_ident(entity_column)}, {_quote_ident(timestamp_column)}"
    parts: list[str] = []
    for dataset, pairs in sorted(by_dataset.items()):
        table = catalog.table(dataset)
        columns = sorted({column for _, column in pairs})
        windows = windowed_columns(dataset)
        latest = latest_columns(dataset)
        has_known_at = _check_table(con, dataset, table, columns, windows, latest)
        relation = resolve_spine(
            con,
            table=table,
            feature_columns=columns,
            spine=spine,
            entity_column=entity_column,
            timestamp_column=timestamp_column,
            max_lookback=max_lookback,
            windows=windows,
            latest=latest,
            as_of=as_of,
            has_known_at=has_known_at,
        )
        named = ", ".join(f"{_quote_ident(c)} AS {_quote_ident(f)}" for f, c in pairs)
        if len(by_dataset) == 1:
            return relation.project(f"{keys}, {named}")

        parts.append(f"thyme_part_{len(parts)}")
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE {_quote_ident(parts[-1])} AS "
            f"SELECT DISTINCT {keys}, {named} FROM ({relation.sql_query()})"
        )

    joins = " ".join(f"LEFT JOIN {_quote_ident(p)} USING ({keys})" for p in parts)
    return con.sql(f"SELECT * FROM thyme_spine {joins}")


def resolve_spine(
    con: Any,
    *,
    table: str,
    feature_columns: list[str],
    spine: Any,
    entity_column: str,
    timestamp_column: str,
    max_lookback: str | None = DEFAULT_MAX_LOOKBACK,
    windows: dict[str, tuple[str, int]] | None = None,
    latest: dict[str, str | None] | None = None,
    as_of: Literal["event", "known"] = "event",
    has_known_at: bool | None = None,
) -> Any:
    """Resolve a whole spine against one offline table, returning a relation.

    The spine arrives as Arrow and is **copied into a DuckDB table** before the
    join. That copy is not incidental — see below.

    Returns a DuckDB relation, unevaluated: nothing is read until the caller
    materialises or streams it.

    ## Why the spine is materialised

    Joining straight off the registered Arrow view looks strictly better: no
    copy, and a 20M-row spine costs a view over memory the caller already holds.
    It is 560x slower.

    DuckDB will not use its native ASOF operator when a side is an Arrow scan;
    the plan degrades to `NESTED_LOOP_JOIN` feeding a `HASH_GROUP_BY` with
    `arg_max_null`. Measured on a 500k-row history with a 200k-row spine:

        arrow spine + parquet history     21.9s      9,122 rows/sec
        table spine + parquet history      0.04s  5,137,513 rows/sec

    The copy costs one pass over two narrow columns — for 20M rows, a few
    hundred MB and a second or so. It buys back three orders of magnitude, and
    it is invisible in any test small enough to run quickly, which is why
    `tests/bench_offline_pull.py` exists.
    """
    spine_relation = "thyme_spine"
    arrow_view = "thyme_spine_arrow"
    con.register(arrow_view, spine)
    con.execute(f'CREATE OR REPLACE TEMP TABLE "{spine_relation}" AS SELECT * FROM "{arrow_view}"')
    con.unregister(arrow_view)
    sql = build_asof_sql(
        table=table,
        feature_columns=feature_columns,
        entity_column=entity_column,
        timestamp_column=timestamp_column,
        spine_relation=spine_relation,
        max_lookback=max_lookback,
        windows=windows,
        latest=latest,
        as_of=as_of,
        has_known_at=has_known_at,
    )
    return con.sql(sql)


def build_asof_sql(
    table: str,
    feature_columns: list[str],
    *,
    entity_column: str,
    timestamp_column: str,
    spine_relation: str = "spine",
    max_lookback: str | None = DEFAULT_MAX_LOOKBACK,
    windows: dict[str, tuple[str, int]] | None = None,
    latest: dict[str, str | None] | None = None,
    as_of: Literal["event", "known"] = "event",
    has_known_at: bool | None = None,
) -> str:
    """Build the as-of join that resolves an entire spine in one scan.

    When ``as_of="known"``, resolves bitemporally so each spine row sees only
    rows with ``COALESCE(known_at, event_time) <= T``: windowed Count/Sum/Avg
    subtract an IEJoin correction term for rows whose ``event_time <= T < known_at``
    (handling both late arrivals and multi-version updates in one pass), while
    non-windowed columns take a fast-path ASOF join with a targeted fallback
    only for spine points whose newest row was not yet known at ``T``.
    """
    if as_of not in ("event", "known"):
        raise ValueError(f"invalid as_of {as_of!r}: expected 'event' or 'known'")
    if not feature_columns:
        raise ValueError(
            "feature_columns is empty: an as-of read with no projected feature "
            "would scan the table to return nothing."
        )
    leaked = sorted(set(feature_columns) & BLOB_COLUMNS)
    if leaked:
        raise ValueError(
            f"refusing to project blob column(s) {leaked}: they dwarf the "
            f"feature columns and are never needed on the training path."
        )

    use_known_col = (as_of == "known") if has_known_at is None else has_known_at

    tbl = _quote_table(table)
    spine = _quote_ident(spine_relation)
    s_entity = _quote_ident(entity_column)
    s_ts = _quote_ident(timestamp_column)
    h_entity = _quote_ident(ENTITY_ID_COLUMN)
    h_ts = _quote_ident(TS_COLUMN)

    windows = {c: w for c, w in (windows or {}).items() if c in feature_columns}
    shares = [c for c, _ in _share_columns(windows)]
    latest = {c: t for c, t in (latest or {}).items() if c in feature_columns}
    non_window_cols = [c for c in feature_columns if c not in windows]

    # Projected explicitly, never `*` -- see BLOB_COLUMNS. The map is read one
    # key at a time for the same reason.
    base_projected_cols = list(feature_columns) + list(shares)
    projected = ", ".join(
        f"h.{_quote_ident(JSON_FEATURES_COLUMN)}[{_quote_literal(c)}] AS {_quote_ident(c)}"
        if c in latest
        else f"h.{_quote_ident(c)}"
        for c in base_projected_cols
    )
    need_event_time = bool(windows) or as_of == "known" or use_known_col
    if need_event_time:
        projected += f", h.{_quote_ident(PARTITION_COLUMN)}"

    part = _quote_ident(PARTITION_COLUMN)
    lower_bound = ""
    if max_lookback is not None:
        lower_bound = (
            f"\n      AND h.{part} >= "
            f"(SELECT min_event_time FROM bounds) - INTERVAL '{max_lookback}'"
        )

    if as_of == "event" and not use_known_col:
        return f"""\
WITH bounds AS (
    SELECT
        MAX({s_ts}) AS max_ts,
        MIN({s_ts})::TIMESTAMPTZ AS min_event_time,
        MAX({s_ts})::TIMESTAMPTZ + INTERVAL '{_PRUNE_MARGIN}' AS max_event_time
    FROM {spine}
),
history AS (
    SELECT h.{h_entity}, h.{h_ts}, {projected}
    FROM {tbl} h
    WHERE h.{part} <= (SELECT max_event_time FROM bounds){lower_bound}
){_window_ctes(windows, spine, s_entity, s_ts, h_entity, part)}
SELECT s.{s_entity}, s.{s_ts}, {", ".join(_column(c, windows, latest) for c in feature_columns)}
FROM {spine} s
ASOF LEFT JOIN history h
  ON s.{s_entity} = h.{h_entity}
 AND s.{s_ts} >= h.{h_ts}{_window_joins(windows, s_entity, s_ts)}
"""

    known_expr = (
        f"COALESCE(h.{_quote_ident(KNOWN_AT_COLUMN)}, h.{part})"
        if use_known_col
        else f"h.{part}"
    )
    carry_cols = [ENTITY_ID_COLUMN, TS_COLUMN] + base_projected_cols + [PARTITION_COLUMN]
    carry_select = ", ".join(_quote_ident(c) for c in carry_cols) + ", __k"

    if as_of == "event":
        # Append-only versioned table read at event time: pick latest known_at per (entity_id, ts).
        return f"""\
WITH bounds AS (
    SELECT
        MAX({s_ts}) AS max_ts,
        MIN({s_ts})::TIMESTAMPTZ AS min_event_time,
        MAX({s_ts})::TIMESTAMPTZ + INTERVAL '{_PRUNE_MARGIN}' AS max_event_time
    FROM {spine}
),
raw_history AS (
    SELECT h.{h_entity}, h.{h_ts}, {projected}, {known_expr} AS __k
    FROM {tbl} h
    WHERE h.{part} <= (SELECT max_event_time FROM bounds){lower_bound}
),
history AS (
    SELECT {carry_select}
    FROM raw_history
    QUALIFY row_number() OVER (PARTITION BY {h_entity}, {h_ts} ORDER BY __k DESC NULLS LAST) = 1
){_window_ctes(windows, spine, s_entity, s_ts, h_entity, part)}
SELECT s.{s_entity}, s.{s_ts}, {", ".join(_column(c, windows, latest) for c in feature_columns)}
FROM {spine} s
ASOF LEFT JOIN history h
  ON s.{s_entity} = h.{h_entity}
 AND s.{s_ts} >= h.{h_ts}{_window_joins(windows, s_entity, s_ts)}
"""

    # Knowledge-time path (`as_of="known"`).
    q = _quote_ident
    ctes = [
        f"""bounds AS (
    SELECT
        MAX({s_ts}) AS max_ts,
        MIN({s_ts})::TIMESTAMPTZ AS min_event_time,
        MAX({s_ts})::TIMESTAMPTZ + INTERVAL '{_PRUNE_MARGIN}' AS max_event_time
    FROM {spine}
)""",
        f"""raw_history AS (
    SELECT h.{h_entity}, h.{h_ts}, {projected}, {known_expr} AS __k
    FROM {tbl} h
    WHERE h.{part} <= (SELECT max_event_time FROM bounds){lower_bound}
)""",
        f"""history AS (
    SELECT {carry_select}
    FROM raw_history
    QUALIFY row_number() OVER (PARTITION BY {h_entity}, {h_ts} ORDER BY __k DESC NULLS LAST) = 1
)""",
        f"""points AS (
    SELECT DISTINCT {s_entity} AS e, {s_ts} AS ts, {s_ts}::TIMESTAMPTZ AS at_ts FROM {spine}
)""",
    ]

    if non_window_cols:
        nw_select = ", ".join(f"h.{q(c)} AS {q(c)}" for c in non_window_cols)
        ctes.append(
            f"""asof_latest AS (
    SELECT p.e, p.ts, p.at_ts, h.{h_ts} AS __h_ts, h.__k AS __h_k, {nw_select}
    FROM points p
    ASOF LEFT JOIN history h
      ON p.e = h.{h_entity}
     AND p.ts >= h.{h_ts}
)"""
        )
        ctes.append(
            """affected_points AS (
    SELECT e, ts, at_ts
    FROM asof_latest
    WHERE __h_ts IS NOT NULL AND __h_k > at_ts
)"""
        )
        kf_aggs = ", ".join(
            f"arg_max(r.{q(c)}, struct_pack(t := r.{h_ts}, k := r.__k)) AS {q(c)}"
            for c in non_window_cols
        )
        ctes.append(
            f"""known_fallback AS (
    SELECT p.e, p.ts, MAX(r.{h_ts}) AS __kf_ts, {kf_aggs}
    FROM affected_points p
    JOIN raw_history r
      ON r.{h_entity} = p.e
     AND r.{h_ts} <= p.ts
     AND r.__k <= p.at_ts
    GROUP BY p.e, p.ts
)"""
        )
    else:
        # No non-windowed columns: existence of any known row by T reduces to
        # MIN(__k) <= T per entity because known_at >= event_time always holds.
        ctes.append(
            f"""entity_first_known AS (
    SELECT {h_entity} AS e, MIN(__k) AS __min_k
    FROM raw_history
    GROUP BY {h_entity}
)"""
        )

    if windows:
        share_specs = _share_columns(windows)
        per_time = ", ".join(f"SUM(CAST(h.{q(c)} AS {_EXACT[x]})) AS {q(c)}" for c, x in share_specs)
        running = ", ".join(f"SUM({q(c)}) OVER w AS {q(c)}" for c, _ in share_specs)
        cols = ", ".join(f"r.{q(c)}" for c, _ in share_specs)
        win_lengths = sorted({w for _, w in windows.values()})
        min_win = win_lengths[0]

        # Exact O(N log N) bitemporal window identity (using known_at >= event_time):
        #   SUM(t in [lo, T] AND k <= T)
        #     = SUM(k <= T) - SUM(t < lo) + SUM(t < lo AND k > T)
        # Both leading sums are native ASOF joins; the residual (cell2) contains
        # only rows delayed longer than the window itself (k - t > min_win - 1).
        step_exprs = ", ".join(
            f"(COALESCE(CAST({q(c)} AS {_EXACT[x]}), 0) - "
            f"COALESCE(LAG(CAST({q(c)} AS {_EXACT[x]})) OVER (PARTITION BY {h_entity}, {h_ts} ORDER BY __k ASC), 0)) AS {q(c)}"
            for c, x in share_specs
        )
        nonzero_filter = " OR ".join(f"{q(c)} <> 0" for c, _ in share_specs)
        ctes.append(
            f"""version_steps AS (
    SELECT {h_entity} AS e, {part} AS t, __k AS k, {step_exprs}
    FROM raw_history
)"""
        )
        per_k_sum = ", ".join(f"SUM({q(c)}) AS {q(c)}" for c, _ in share_specs)
        ctes.append(
            f"""per_k AS (
    SELECT e, k, {per_k_sum} FROM version_steps GROUP BY e, k
)"""
        )
        ctes.append(
            f"""running_k AS (
    SELECT e, k, {running} FROM per_k
    WINDOW w AS (PARTITION BY e ORDER BY k ROWS UNBOUNDED PRECEDING)
)"""
        )
        ctes.append(
            f"""hi_k AS (
    SELECT p.e, p.ts, {cols} FROM points p ASOF LEFT JOIN running_k r ON r.e = p.e AND p.at_ts >= r.k
)"""
        )
        ctes.append(
            f"""per_time AS (
    SELECT h.{h_entity} AS e, h.{part} AS t, {per_time} FROM history h GROUP BY ALL
)"""
        )
        ctes.append(
            f"""running AS (
    SELECT e, t, {running} FROM per_time
    WINDOW w AS (PARTITION BY e ORDER BY t ROWS UNBOUNDED PRECEDING)
)"""
        )
        for w in win_lengths:
            ctes.append(
                f"""lo{w} AS (
    SELECT p.e, p.ts, {cols} FROM points p
    ASOF LEFT JOIN running r ON r.e = p.e AND to_timestamp(floor(epoch(p.at_ts)) + {1 - w}) > r.t
)"""
            )

        ctes.append(
            f"""ultra_late AS (
    SELECT e, t, k, {", ".join(q(c) for c, _ in share_specs)}
    FROM version_steps
    WHERE epoch(k) - epoch(t) > {min_win - 1} AND ({nonzero_filter})
)"""
        )
        cell2_aggs = []
        for col_name, (kind, w) in windows.items():
            for suffix in _NEEDS[kind]:
                scol = col_name + suffix
                alias = f"{scol}__w{w}"
                cell2_aggs.append(
                    f"SUM(CASE WHEN u.t < to_timestamp(floor(epoch(p.at_ts)) + {1 - w}) THEN u.{q(scol)} ELSE 0 END) AS {q(alias)}"
                )
        ctes.append(
            f"""cell2 AS (
    SELECT p.e, p.ts, {", ".join(cell2_aggs)}
    FROM points p
    JOIN ultra_late u
      ON u.e = p.e
     AND u.t < to_timestamp(floor(epoch(p.at_ts)) + {1 - min_win})
     AND u.k > p.at_ts
    GROUP BY p.e, p.ts
)"""
        )

    joins = []
    if non_window_cols:
        joins.append(f"LEFT JOIN asof_latest a ON a.e = s.{s_entity} AND a.ts = s.{s_ts}")
        joins.append(f"LEFT JOIN known_fallback kf ON kf.e = s.{s_entity} AND kf.ts = s.{s_ts}")
        exists_cond = "CASE WHEN a.__h_ts IS NULL THEN FALSE WHEN a.__h_k > a.at_ts THEN kf.__kf_ts IS NOT NULL ELSE TRUE END"
    else:
        joins.append(f"LEFT JOIN entity_first_known efk ON efk.e = s.{s_entity}")
        exists_cond = f"efk.__min_k IS NOT NULL AND efk.__min_k <= s.{s_ts}::TIMESTAMPTZ"

    if windows:
        for j in ["hi_k"] + [f"lo{w}" for w in sorted({w for _, w in windows.values()})] + ["cell2"]:
            joins.append(f"LEFT JOIN {j} ON {j}.e = s.{s_entity} AND {j}.ts = s.{s_ts}")

    select_cols = [f"s.{s_entity}", f"s.{s_ts}"]
    for c in feature_columns:
        if c in windows:
            kind, w = windows[c]
            n_col = c + SHARE_COUNT
            s_col = c + SHARE_SUM
            n_expr = (
                f"CAST(COALESCE(hi_k.{q(n_col)}, 0) - COALESCE(lo{w}.{q(n_col)}, 0) "
                f"+ COALESCE(cell2.{q(f'{n_col}__w{w}')}, 0) AS DOUBLE)"
            )
            s_expr = (
                f"CAST(COALESCE(hi_k.{q(s_col)}, 0) - COALESCE(lo{w}.{q(s_col)}, 0) "
                f"+ COALESCE(cell2.{q(f'{s_col}__w{w}')}, 0) AS DOUBLE)"
            )
            if kind == "count":
                val = n_expr
            elif kind == "sum":
                val = s_expr
            else:
                val = f"CASE WHEN {n_expr} > 0 THEN {s_expr} / {n_expr} ELSE 0.0 END"
            select_cols.append(f"CASE WHEN NOT ({exists_cond}) THEN NULL ELSE {val} END AS {q(c)}")
        else:
            raw_val = f"CASE WHEN a.__h_ts IS NULL THEN NULL WHEN a.__h_k > a.at_ts THEN kf.{q(c)} ELSE a.{q(c)} END"
            if latest.get(c) is not None:
                select_cols.append(f"CAST(CAST({raw_val} AS JSON) AS {latest[c]}) AS {q(c)}")
            else:
                select_cols.append(f"{raw_val} AS {q(c)}")

    return "WITH " + ",\n".join(ctes) + f"\nSELECT {', '.join(select_cols)}\nFROM {spine} s\n" + "\n".join(joins) + "\n"


# What each aggregation reads of its share, and the exact type it is totalled in.
_NEEDS = {"count": (SHARE_COUNT,), "sum": (SHARE_SUM,), "avg": (SHARE_SUM, SHARE_COUNT)}
_EXACT = {SHARE_COUNT: "BIGINT", SHARE_SUM: "DECIMAL(38, 12)"}


def _share_columns(windows: dict) -> list[tuple[str, str]]:
    """Each share column the windows read, with its suffix."""
    return [(c + x, x) for c, (kind, _) in windows.items() for x in _NEEDS[kind]]


def _window_ctes(windows: dict, spine: str, s_entity: str, s_ts: str, h_entity: str, part: str) -> str:
    """Running totals of the shares in event-time order, per entity. A window
    is then two as-of lookups: the total at T less the total before the
    window's first second. The totals are exact decimals, so the difference
    loses nothing however long the history is."""
    if not windows:
        return ""
    q = _quote_ident
    shares = _share_columns(windows)
    per_time = ", ".join(f"SUM(CAST(h.{q(c)} AS {_EXACT[x]})) AS {q(c)}" for c, x in shares)
    running = ", ".join(f"SUM({q(c)}) OVER w AS {q(c)}" for c, _ in shares)
    cols = ", ".join(f"r.{q(c)}" for c, _ in shares)
    # One row per event time first, so a streaming ROWS frame gives every
    # event at that time the same total.
    return f""",
per_time AS (
    SELECT h.{h_entity} AS e, h.{part} AS t, {per_time} FROM history h GROUP BY ALL
),
running AS (
    SELECT e, t, {running} FROM per_time
    WINDOW w AS (PARTITION BY e ORDER BY t ROWS UNBOUNDED PRECEDING)
),
-- The lookups go on the distinct spine points alone: an as-of join sorts
-- everything it carries, so it must carry nothing else.
points AS (
    SELECT DISTINCT {s_entity} AS e, {s_ts} AS ts, {s_ts}::TIMESTAMPTZ AS at FROM {spine}
),
hi AS (
    SELECT p.e, p.ts, {cols} FROM points p ASOF LEFT JOIN running r ON r.e = p.e AND p.at >= r.t
){"".join(f""",
lo{w} AS (
    SELECT p.e, p.ts, {cols} FROM points p
    ASOF LEFT JOIN running r ON r.e = p.e AND to_timestamp(floor(epoch(p.at)) + {1 - w}) > r.t
)""" for w in sorted({w for _, w in windows.values()}))}"""


def _window_joins(windows: dict, s_entity: str, s_ts: str) -> str:
    if not windows:
        return ""
    return "".join(
        f"\nLEFT JOIN {j} ON {j}.e = s.{s_entity} AND {j}.ts = s.{s_ts}"
        for j in ["hi"] + [f"lo{w}" for w in sorted({w for _, w in windows.values()})]
    )


def _column(c: str, windows: dict, latest: dict) -> str:
    q = _quote_ident
    if latest.get(c) is not None:
        return f"CAST(CAST(h.{q(c)} AS JSON) AS {latest[c]}) AS {q(c)}"
    if c not in windows:
        return f"h.{q(c)}"
    kind, w = windows[c]
    n, total = (
        f"CAST(COALESCE(hi.{q(c + x)}, 0) - COALESCE(lo{w}.{q(c + x)}, 0) AS DOUBLE)"
        for x in (SHARE_COUNT, SHARE_SUM)
    )
    value = n if kind == "count" else total
    if kind == "avg":
        value = f"CASE WHEN {n} > 0 THEN {total} / {n} ELSE 0.0 END"
    # An entity with no row by T has no window to report.
    return f"CASE WHEN h.{q(TS_COLUMN)} IS NULL THEN NULL ELSE {value} END AS {q(c)}"
