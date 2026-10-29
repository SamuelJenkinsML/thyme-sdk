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


def _quote_ident(name: str) -> str:
    """Quote a SQL identifier, doubling any embedded quote."""
    return '"' + name.replace('"', '""') + '"'


def feature_columns_for(featureset_meta: dict) -> list[str]:
    """The table columns a featureset needs, in declaration order.

    Excludes two kinds of feature that are not columns:

    - **request-time** features (TH-216), whose values arrive from the caller and
      were never written to the store;
    - **derived** features, computed by an extractor from other features. Their
      inputs are columns; they are not.

    Reading a column that does not exist is a hard error rather than a silent
    null, so being precise here is what keeps the projection honest.
    """
    columns: list[str] = []
    for feature in featureset_meta.get("features", []):
        if feature.get("request"):
            continue
        if feature.get("deps"):
            continue
        name = feature["name"]
        if name in BLOB_COLUMNS:
            continue
        columns.append(name)
    return columns


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


def derived_features(featureset_meta: dict) -> list[str]:
    """Features computed by an extractor rather than read from a table."""
    return [
        f["name"]
        for f in featureset_meta.get("features", [])
        if f.get("deps") and not f.get("request")
    ]


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

    tbl = ".".join(_quote_ident(part) for part in table.split("."))
    spine = _quote_ident(spine_relation)
    s_entity = _quote_ident(entity_column)
    s_ts = _quote_ident(timestamp_column)
    h_entity = _quote_ident(ENTITY_ID_COLUMN)
    h_ts = _quote_ident(TS_COLUMN)

    windows = {c: w for c, w in (windows or {}).items() if c in feature_columns}
    shares = [c for c, _ in _share_columns(windows)]
    # Projected explicitly, never `*` -- see BLOB_COLUMNS.
    projected = ", ".join(f"h.{_quote_ident(c)}" for c in feature_columns + shares)
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
SELECT s.{s_entity}, s.{s_ts}, {", ".join(_column(c, windows) for c in feature_columns)}
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


def _column(c: str, windows: dict) -> str:
    q = _quote_ident
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
