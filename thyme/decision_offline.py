"""Bulk Iceberg reads for decision records and outcomes via DuckDB.

Interactive lookups use ``GET /decisions`` on the query server. Backtests and
calibration reports read ``decision_records`` and ``decision_outcomes`` directly
from Iceberg via DuckDB so large histories and window-based outcome joins stay
vectorized and never bottleneck the live serving tier.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Sequence

import polars as pl

from thyme.decision import parse_window
from thyme.offline_catalog import CatalogConfig, connect


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _quote_table(table: str) -> str:
    return ".".join(_quote_ident(part) for part in table.split("."))


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _iso_utc(value: datetime | str) -> str:
    if isinstance(value, str):
        return value
    dt = value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def _featureset_name(featureset: str | type | None) -> str | None:
    if featureset is None:
        return None
    return featureset.__name__ if isinstance(featureset, type) else str(featureset)


def _table_columns(con: Any, table_sql: str) -> set[str]:
    return {row[0] for row in con.execute(f"DESCRIBE {table_sql}").fetchall()}


def _dim_expr(key: str) -> str:
    return f"dimensions[{_quote_literal(key)}]"


def _optional_col(columns: set[str], name: str, cast_as: str) -> str:
    if name in columns:
        return f"CAST({_quote_ident(name)} AS {cast_as}) AS {_quote_ident(name)}"
    return f"CAST(NULL AS {cast_as}) AS {_quote_ident(name)}"


def read_decision_records(
    featureset: str | type | None = None,
    *,
    start: datetime | str | None = None,
    end: datetime | str | None = None,
    entity_ids: Sequence[str] | None = None,
    catalog: CatalogConfig | None = None,
    con: Any = None,
    table: str | None = None,
) -> pl.DataFrame:
    """Read ``decision_records`` directly from Iceberg via DuckDB.

    The offline sink stores ``decision_id`` in the top-level ``entity_id`` column
    and the customer entity key in ``dimensions['entity_id']``; this projection
    normalizes every field back to the canonical decision record shape.
    """
    owns_con = con is None
    cfg = catalog or CatalogConfig.from_env()
    if con is None:
        con = connect(cfg)
    table_ref = table or cfg.table("decision_records")
    table_sql = _quote_table(table_ref)

    try:
        cols = _table_columns(con, table_sql)
        where: list[str] = []
        fs_name = _featureset_name(featureset)
        if fs_name is not None:
            where.append(f"{_dim_expr('featureset')} = {_quote_literal(fs_name)}")
        if entity_ids:
            literals = ", ".join(_quote_literal(str(e)) for e in entity_ids)
            where.append(f"{_dim_expr('entity_id')} IN ({literals})")
        if start is not None:
            where.append(f"event_time >= CAST({_quote_literal(_iso_utc(start))} AS TIMESTAMPTZ)")
        if end is not None:
            where.append(f"event_time <= CAST({_quote_literal(_iso_utc(end))} AS TIMESTAMPTZ)")
        where_sql = f" WHERE {' AND '.join(where)}" if where else ""

        sql = f"""
        SELECT
            entity_id AS decision_id,
            {_dim_expr('featureset')} AS featureset,
            {_dim_expr('entity_id')} AS entity_id,
            ts AS decided_at,
            {_dim_expr('replica_id')} AS replica_id,
            {_dim_expr('state')} AS state,
            {_dim_expr('state_hash')} AS state_hash,
            {_optional_col(cols, 'renderer_version', 'BIGINT')},
            {_dim_expr('feature_versions')} AS feature_versions,
            {_dim_expr('questions_hash')} AS questions_hash,
            {_dim_expr('model_alias')} AS model_alias,
            {_dim_expr('model_version')} AS model_version,
            {_dim_expr('judgments')} AS judgments,
            {_optional_col(cols, 'policy_version', 'BIGINT')},
            {_dim_expr('rule_fired')} AS rule_fired,
            {_dim_expr('action')} AS action,
            {_dim_expr('status')} AS status,
            {_optional_col(cols, 'latency_ms', 'BIGINT')},
            {_optional_col(cols, 'cost_usd', 'DOUBLE')},
            {_dim_expr('idempotency_key')} AS idempotency_key
        FROM {table_sql}
        {where_sql}
        ORDER BY decided_at ASC, decision_id ASC
        """
        arrow_tbl = con.sql(sql).arrow()
        return pl.from_arrow(arrow_tbl)
    finally:
        if owns_con:
            con.close()


def read_decision_outcomes(
    featureset: str | type | None = None,
    *,
    start: datetime | str | None = None,
    end: datetime | str | None = None,
    entity_ids: Sequence[str] | None = None,
    catalog: CatalogConfig | None = None,
    con: Any = None,
    table: str | None = None,
) -> pl.DataFrame:
    """Read ``decision_outcomes`` directly from Iceberg via DuckDB."""
    owns_con = con is None
    cfg = catalog or CatalogConfig.from_env()
    if con is None:
        con = connect(cfg)
    table_ref = table or cfg.table("decision_outcomes")
    table_sql = _quote_table(table_ref)

    try:
        cols = _table_columns(con, table_sql)
        where: list[str] = []
        fs_name = _featureset_name(featureset)
        if fs_name is not None:
            where.append(f"{_dim_expr('featureset')} = {_quote_literal(fs_name)}")
        if entity_ids:
            literals = ", ".join(_quote_literal(str(e)) for e in entity_ids)
            where.append(f"{_dim_expr('entity_id')} IN ({literals})")
        if start is not None:
            where.append(f"event_time >= CAST({_quote_literal(_iso_utc(start))} AS TIMESTAMPTZ)")
        if end is not None:
            where.append(f"event_time <= CAST({_quote_literal(_iso_utc(end))} AS TIMESTAMPTZ)")
        where_sql = f" WHERE {' AND '.join(where)}" if where else ""

        sql = f"""
        SELECT
            entity_id AS outcome_id,
            {_dim_expr('decision_id')} AS decision_id,
            {_dim_expr('featureset')} AS featureset,
            {_dim_expr('entity_id')} AS entity_id,
            {_dim_expr('outcome')} AS outcome,
            {_optional_col(cols, 'value', 'DOUBLE')},
            ts AS occurred_at,
            {_dim_expr('recorded_at')} AS recorded_at
        FROM {table_sql}
        {where_sql}
        ORDER BY occurred_at ASC, outcome_id ASC
        """
        arrow_tbl = con.sql(sql).arrow()
        return pl.from_arrow(arrow_tbl)
    finally:
        if owns_con:
            con.close()


def _to_polars(data: Any) -> pl.DataFrame:
    if isinstance(data, pl.DataFrame):
        return data
    if hasattr(data, "to_arrow"):
        return pl.from_arrow(data.to_arrow())
    return pl.DataFrame(list(data))


def attach_outcomes_duckdb(
    decisions: Any,
    outcomes: Any,
    *,
    window: str | timedelta | None = None,
    featureset: str | type | None = None,
    con: Any = None,
) -> pl.DataFrame:
    """Attach outcomes to decisions in DuckDB via direct ID + vectorized ASOF JOIN.

    Matches :func:`thyme.decision.attach_outcomes` semantics:
    - Non-empty ``outcome.decision_id`` attaches directly to that decision
      (regardless of ``window``).
    - Missing/null ``outcome.decision_id`` attaches to the latest decision for
      the same ``(featureset, entity_id)`` satisfying
      ``0 <= occurred_at - decided_at <= window``.
    """
    dec_df = _to_polars(decisions)
    out_df = _to_polars(outcomes)
    if out_df.height == 0:
        return out_df.with_columns(pl.lit(None, dtype=pl.String).alias("attached_decision_id"))

    fs_name = _featureset_name(featureset)
    if fs_name is not None and "featureset" in dec_df.columns:
        dec_df = dec_df.filter(pl.col("featureset") == fs_name)

    if dec_df.height == 0:
        return out_df.with_columns(pl.lit(None, dtype=pl.String).alias("attached_decision_id"))

    # Normalize optional columns so DuckDB SQL stays schema-stable on minimal fixtures.
    if "decision_id" not in out_df.columns:
        out_df = out_df.with_columns(pl.lit(None, dtype=pl.String).alias("decision_id"))
    if "featureset" not in out_df.columns:
        out_df = out_df.with_columns(pl.lit(fs_name, dtype=pl.String).alias("featureset"))

    win_secs = parse_window(window).total_seconds() if window is not None else None

    owns_con = con is None
    if con is None:
        import duckdb

        con = duckdb.connect()

    try:
        con.register("_thyme_dec_arrow", dec_df.to_arrow())
        con.register("_thyme_out_arrow", out_df.to_arrow())
        con.execute(
            """
            CREATE OR REPLACE TEMP TABLE _thyme_dec AS
            SELECT
                CAST(decision_id AS VARCHAR) AS decision_id,
                CAST(featureset AS VARCHAR) AS featureset,
                CAST(entity_id AS VARCHAR) AS entity_id,
                CAST(decided_at AS TIMESTAMPTZ) AS decided_ts
            FROM _thyme_dec_arrow
            ORDER BY featureset, entity_id, decided_ts
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TEMP TABLE _thyme_out AS
            SELECT
                ROW_NUMBER() OVER () AS _row_idx,
                *,
                CAST(occurred_at AS TIMESTAMPTZ) AS occurred_ts
            FROM _thyme_out_arrow
            """
        )
        con.unregister("_thyme_dec_arrow")
        con.unregister("_thyme_out_arrow")

        if win_secs is not None:
            sql = f"""
            WITH direct_match AS (
                SELECT
                    o._row_idx,
                    d.decision_id AS direct_id
                FROM _thyme_out o
                LEFT JOIN _thyme_dec d
                    ON o.decision_id IS NOT NULL
                    AND o.decision_id != ''
                    AND d.decision_id = o.decision_id
            ),
            window_match AS (
                SELECT
                    o._row_idx,
                    CASE
                        WHEN o.decision_id IS NOT NULL AND o.decision_id != '' THEN NULL
                        WHEN d.decision_id IS NOT NULL
                             AND (epoch(o.occurred_ts) - epoch(d.decided_ts)) <= {win_secs!r}
                        THEN d.decision_id
                        ELSE NULL
                    END AS window_id
                FROM _thyme_out o
                ASOF LEFT JOIN _thyme_dec d
                    ON o.featureset = d.featureset
                    AND o.entity_id = d.entity_id
                    AND o.occurred_ts >= d.decided_ts
            )
            SELECT
                o.* EXCLUDE (_row_idx, occurred_ts),
                COALESCE(dm.direct_id, wm.window_id) AS attached_decision_id
            FROM _thyme_out o
            JOIN direct_match dm ON o._row_idx = dm._row_idx
            JOIN window_match wm ON o._row_idx = wm._row_idx
            ORDER BY o._row_idx
            """
        else:
            sql = """
            SELECT
                o.* EXCLUDE (_row_idx, occurred_ts),
                d.decision_id AS attached_decision_id
            FROM _thyme_out o
            LEFT JOIN _thyme_dec d
                ON o.decision_id IS NOT NULL
                AND o.decision_id != ''
                AND d.decision_id = o.decision_id
            ORDER BY o._row_idx
            """
        arrow_tbl = con.sql(sql).arrow()
        return pl.from_arrow(arrow_tbl)
    finally:
        if owns_con:
            con.close()
