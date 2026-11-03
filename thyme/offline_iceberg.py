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

from typing import Any

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
    {"state_value", "sketches", "dimensions", "json_features", "op", "event_time"}
)

#: The entity key and ordering key in every offline table.
ENTITY_ID_COLUMN = "entity_id"
TS_COLUMN = "ts"

#: The Iceberg partition source: the table is `partition by days(event_time)`.
#: Pruning must reference this column, not `ts` — a predicate on the string
#: cannot prune partitions, and partition pruning is what turns a full-history
#: scan into a bounded one.
PARTITION_COLUMN = "event_time"

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
) -> None:
    """Refuse a table the pull cannot read correctly, by name.

    Reading stored totals where the table has shares would not fail: a late row
    has no total, so its contribution would be lost with no error.
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


def resolve_featureset(
    con: Any,
    *,
    featureset_meta: dict,
    catalog: Any,
    spine: Any,
    entity_column: str,
    timestamp_column: str,
    max_lookback: str | None = DEFAULT_MAX_LOOKBACK,
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
        _check_table(con, dataset, table, columns, windows, latest)
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
) -> str:
    """Build the as-of join that resolves an entire spine in one scan.

    Args:
        table: Fully-qualified Iceberg table, e.g. ``thyme.user_order_stats``.
        feature_columns: Table columns to project. Never include blob columns;
            see :data:`BLOB_COLUMNS`.
        entity_column: Entity-id column **in the spine**.
        timestamp_column: Timestamp column **in the spine**.
        spine_relation: Relation holding the spine. Registered from Arrow by the
            caller, so the spine is never serialised through SQL.
        max_lookback: A DuckDB interval bounding how far back history is read.
            Defaults to :data:`DEFAULT_MAX_LOOKBACK`; pass ``None`` for all of
            history. See the note on unbounded scans below.
        windows: Columns that are windowed Count, Sum or Avg, as
            :func:`windowed_columns` returns them. Each is added up from the
            rows' shares at the spine row's own time instead of read.
        latest: Columns that are a Last or LastK, as :func:`latest_columns`
            returns them. Each is read from its key of `json_features`.

    ## Ordering is on the raw timestamp string

    The engine stores the source record's timestamp **verbatim**, and RocksDB
    keys on ``{type}:{id}:{that string}``, so ``seek_for_prev`` compares *bytes*
    lexicographically. This join must compare them the same way, and does:
    both sides stay ``VARCHAR`` and DuckDB's comparison is byte-wise.

    That is not the same as chronological order. ``"…T09:14:02.1Z"`` sorts
    *below* ``"…T09:14:02Z"`` because ``.`` (0x2E) < ``Z`` (0x5A), while being
    100 ms later. Parsing to an instant would silently return a different row
    than the online store — the bug class that made TH-147's first cut drop
    history.

    ## Bounding the scan

    The upper bound is free and always safe: an as-of read never looks forward,
    so history above the spine's newest timestamp cannot be an answer.

    The lower bound is not free. To find the newest row at or before each spine
    timestamp, an entity that last updated two years ago requires reading two
    years. ``max_lookback`` caps that, and in doing so changes the semantics to
    "the value as of ``ts``, provided it was updated within the window". Left
    unset, history is unbounded and so is the scan.
    """
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

    tbl = _quote_table(table)
    spine = _quote_ident(spine_relation)
    s_entity = _quote_ident(entity_column)
    s_ts = _quote_ident(timestamp_column)
    h_entity = _quote_ident(ENTITY_ID_COLUMN)
    h_ts = _quote_ident(TS_COLUMN)

    windows = {c: w for c, w in (windows or {}).items() if c in feature_columns}
    shares = [c for c, _ in _share_columns(windows)]
    latest = {c: t for c, t in (latest or {}).items() if c in feature_columns}
    # Projected explicitly, never `*` -- see BLOB_COLUMNS. The map is read one
    # key at a time for the same reason.
    projected = ", ".join(
        f"h.{_quote_ident(JSON_FEATURES_COLUMN)}[{_quote_literal(c)}] AS {_quote_ident(c)}"
        if c in latest
        else f"h.{_quote_ident(c)}"
        for c in feature_columns + shares
    )
    if windows:
        projected += f", h.{_quote_ident(PARTITION_COLUMN)}"

    part = _quote_ident(PARTITION_COLUMN)
    lower_bound = ""
    if max_lookback is not None:
        lower_bound = (
            f"\n      AND h.{part} >= "
            f"(SELECT min_event_time FROM bounds) - INTERVAL '{max_lookback}'"
        )

    return f"""\
WITH bounds AS (
    SELECT
        MAX({s_ts}) AS max_ts,
        MIN({s_ts})::TIMESTAMPTZ AS min_event_time,
        MAX({s_ts})::TIMESTAMPTZ + INTERVAL '{_PRUNE_MARGIN}' AS max_event_time
    FROM {spine}
),
history AS (
    -- Pruning is on event_time, the partition column (`days(event_time)`): a
    -- predicate on the `ts` string cannot prune partitions, and pruning is what
    -- makes this scan cheap. The margin on the upper bound is load-bearing --
    -- see _PRUNE_MARGIN. The bounds are TIMESTAMPTZ because the column is:
    -- a naive cast reads the spine's "...Z" in the session's zone, and east
    -- of UTC that prunes away the newest rows and resolves to a stale one.
    --
    -- The bounds arrive as scalar subqueries, not as a comma join against
    -- `bounds`. That looks equivalent -- bounds is one row -- but an inequality
    -- between two relations plans as a NESTED_LOOP_JOIN, which cost 87s of CPU
    -- on a 500k-row history. A scalar subquery is evaluated once and folded in
    -- as a constant, which is what the pruning wanted in the first place.
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
