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
) -> Any:
    """Resolve a whole spine against one offline table, returning a relation.

    The spine is registered from Arrow rather than serialised into SQL, so a
    20M-row spine costs a view over memory the caller already holds.

    Returns a DuckDB relation, unevaluated: nothing is read until the caller
    materialises or streams it.
    """
    spine_relation = "thyme_spine"
    con.register(spine_relation, spine)
    sql = build_asof_sql(
        table=table,
        feature_columns=feature_columns,
        entity_column=entity_column,
        timestamp_column=timestamp_column,
        spine_relation=spine_relation,
        max_lookback=max_lookback,
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

    # Projected explicitly, never `*` -- see BLOB_COLUMNS.
    projected = ", ".join(f"h.{_quote_ident(c)}" for c in feature_columns)

    lower_bound = ""
    if max_lookback is not None:
        lower_bound = (
            f"\n      AND h.{_quote_ident(PARTITION_COLUMN)} "
            f">= bounds.min_event_time - INTERVAL '{max_lookback}'"
        )

    return f"""\
WITH bounds AS (
    SELECT
        MAX({s_ts}) AS max_ts,
        MIN({s_ts})::TIMESTAMP AS min_event_time,
        MAX({s_ts})::TIMESTAMP + INTERVAL '{_PRUNE_MARGIN}' AS max_event_time
    FROM {spine}
),
history AS (
    -- Pruning is on event_time, the partition column (`days(event_time)`): a
    -- predicate on the `ts` string cannot prune partitions, and pruning is what
    -- makes this scan cheap. The margin on the upper bound is load-bearing --
    -- see _PRUNE_MARGIN.
    SELECT h.{h_entity}, h.{h_ts}, {projected}
    FROM {tbl} h, bounds
    WHERE h.{_quote_ident(PARTITION_COLUMN)} <= bounds.max_event_time{lower_bound}
)
SELECT s.{s_entity}, s.{s_ts}, {", ".join(f"h.{_quote_ident(c)}" for c in feature_columns)}
FROM {spine} s
ASOF LEFT JOIN history h
  ON s.{s_entity} = h.{h_entity}
 AND s.{s_ts} >= h.{h_ts}
"""
