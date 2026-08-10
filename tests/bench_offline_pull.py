"""Throughput of an offline training pull (TH-312).

Not collected by pytest — `bench_` rather than `test_`. Run it directly:

    uv run python tests/bench_offline_pull.py --history 20_000_000 --spine 20_000_000

## What this measures, and why these three things

The reference point is Chalk's published ~20M rows in 5-6 minutes, i.e. about
**60k spine rows/sec**. That is a different architecture (C++/Velox, likely
distributed), so it is a bar rather than a like-for-like comparison. What is
comparable is whether single-node DuckDB over Iceberg-shaped Parquet lands in
the same order of magnitude.

Three numbers come out, and the last two matter more than the first because
they are the ones that regress silently:

1. **rows/sec** for a projected pull.
2. **Projected vs `SELECT *`.** The table carries the raw `StateValue` proto
   beside the flattened feature doubles. Reading it is the difference between a
   ~28 GB scan and a ~170-380 GB one on a realistic history, and Parquet being
   columnar means avoiding it costs nothing but not writing `SELECT *`.
3. **Python invocations.** Derived features run extractors. Row-wise at 20M rows
   is 30-60 minutes on its own, which disqualifies it at this size regardless of
   how fast the join is.

## What it does not measure

Object-store latency. The history is local Parquet, so this isolates scan and
join throughput from S3 round-trips — deliberately, because that is the part
the SDK controls. A real pull against S3 adds first-byte latency per file,
which is bounded by parallelism rather than by this code.
"""

from __future__ import annotations

import argparse
import random
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import duckdb
import polars as pl

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from thyme.offline_iceberg import build_asof_sql  # noqa: E402

#: Roughly the StateValue proto the sink writes per row.
STATE_VALUE_BYTES = 300

#: The demo's shape: a handful of doubles per entity type.
FEATURE_COLUMNS = [
    "avg_rating_30d",
    "max_rating_30d",
    "min_rating_30d",
    "review_count_30d",
    "review_count_7d",
    "total_rating_30d",
]


def _human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:,.1f} PB"


def _random_blobs(rng: random.Random, n: int) -> list[bytes]:
    """`n` distinct random blobs, drawn in one call and sliced.

    Distinct matters more than it looks. A pool of a few thousand repeated
    values is exactly what Parquet's dictionary encoder is for: a 20M-row table
    with 300-byte blobs landed at 216 MB — about 11 bytes a row — so the blob
    cost nothing to read and the projection comparison measured nothing. Real
    StateValue protos are distinct.

    One `randbytes` call rather than `n` of them: the per-call overhead
    dominates at this size, and generating the data must not cost more than the
    query being measured.
    """
    buf = rng.randbytes(n * STATE_VALUE_BYTES)
    view = memoryview(buf)
    return [
        bytes(view[i : i + STATE_VALUE_BYTES])
        for i in range(0, n * STATE_VALUE_BYTES, STATE_VALUE_BYTES)
    ]


def generate_history(path: Path, entities: int, versions: int, chunk: int = 500_000):
    """Write a history table with the sink's schema, in chunks.

    Chunked so peak memory stays flat as the table grows: at 500k rows a chunk
    is ~150 MB of blob plus Python object overhead, and the frame is released
    before the next one. An earlier 2M-row chunk was ~600 MB and, combined with
    a tmpfs workdir, was enough to OOM a 30 GB machine.
    """
    path.mkdir(parents=True, exist_ok=True)
    total = entities * versions
    written = 0
    part = 0
    # Random, and that is the whole point. A constant blob zstd-compresses to
    # nothing, so the table lands at a fraction of its real size and the
    # projection arm below measures noise — a real StateValue is a serialised
    # proto full of distinct doubles and does not compress like that.
    rng = random.Random(20260810)

    while written < total:
        n = min(chunk, total - written)
        idx = pl.int_range(written, written + n, dtype=pl.Int64)
        # entity_id cycles so each entity gets `versions` rows, and the version
        # index drives the timestamp — giving each entity a real history to
        # resolve as-of against rather than a single row.
        entity = idx % entities
        version = idx // entities

        frame = pl.select(
            entity_id=pl.format("e{}", entity),
            # RFC-3339 with microseconds, exactly as the engine emits it, and
            # kept as a string: the ordering key is the raw string and the join
            # must compare bytes, never a parsed instant.
            ts=pl.format(
                "2026-{}-{}T{}:00:00.000000Z",
                (version // 28 + 1).cast(pl.String).str.zfill(2),
                (version % 28 + 1).cast(pl.String).str.zfill(2),
                (version % 24).cast(pl.String).str.zfill(2),
            ),
            state_value=pl.Series("state_value", _random_blobs(rng, n), dtype=pl.Binary),
            **{c: (entity.cast(pl.Float64) % 97) / 7.0 for c in FEATURE_COLUMNS},
        ).with_columns(
            event_time=pl.col("ts").str.to_datetime(
                "%Y-%m-%dT%H:%M:%S%.6fZ", time_zone="UTC"
            ),
        )
        frame.write_parquet(path / f"part-{part:05d}.parquet", compression="zstd")
        written += n
        part += 1
        print(f"  history {written:,}/{total:,}", end="\r", flush=True)
    print()
    size = sum(f.stat().st_size for f in path.glob("*.parquet"))
    return size


def generate_spine(entities: int, rows: int) -> pl.DataFrame:
    """A spine of `(entity_id, timestamp)`, each row asking as of its own moment.

    Timestamps land mid-history so the join has to find a real predecessor
    rather than returning the newest row for everything, which would make the
    as-of join degenerate into a group-by and flatter the result.
    """
    idx = pl.int_range(0, rows, dtype=pl.Int64)
    entity = idx % entities
    version = (idx * 7919) % 300  # coprime stride: spread across the history
    return pl.select(
        entity_id=pl.format("e{}", entity),
        timestamp=pl.format(
            "2026-{}-{}T{}:30:00.000000Z",
            (version // 28 + 1).cast(pl.String).str.zfill(2),
            (version % 28 + 1).cast(pl.String).str.zfill(2),
            (version % 24).cast(pl.String).str.zfill(2),
        ),
    )


def _blob_sql(columns: list[str]) -> str:
    """The same as-of join, but reading `state_value` too.

    Hand-written because `build_asof_sql` refuses to project blob columns — that
    guard is the shipped behaviour and the reason this arm exists is to show
    what it is worth. Kept structurally identical so the only difference being
    measured is the bytes read.
    """
    projected = ", ".join(f"h.{c}" for c in columns)
    return f"""
        SELECT s.entity_id, s.timestamp, {projected}, h.state_value
        FROM thyme_spine s
        ASOF LEFT JOIN offline_history h
          ON s.entity_id = h.entity_id AND s.timestamp >= h.ts
    """


def run(
    history_dir: Path,
    spine: pl.DataFrame,
    columns: list[str],
    threads: int,
    *,
    memory_limit: str,
    with_blob: bool = False,
):
    con = duckdb.connect(config={"threads": threads})
    # Bounded on purpose. This benchmark runs on a developer's machine beside
    # whatever else they are doing, and DuckDB's default is ~80% of RAM — which
    # is how an unbounded run took a 30 GB box down. The spill directory is the
    # workdir rather than /tmp, for the tmpfs reason above.
    con.execute(f"SET memory_limit = '{memory_limit}'")
    con.execute(f"SET temp_directory = '{history_dir.parent}'")
    con.execute(
        f"CREATE VIEW offline_history AS SELECT * FROM read_parquet('{history_dir}/*.parquet')"
    )
    # Same materialisation `resolve_spine` does, and for the same reason: an
    # Arrow-registered spine forces DuckDB off its native ASOF operator.
    con.register("thyme_spine_arrow", spine.to_arrow())
    con.execute("CREATE TEMP TABLE thyme_spine AS SELECT * FROM thyme_spine_arrow")
    sql = (
        _blob_sql(columns)
        if with_blob
        else build_asof_sql(
            table="offline_history",
            feature_columns=columns,
            entity_column="entity_id",
            timestamp_column="timestamp",
            spine_relation="thyme_spine",
            max_lookback=None,
        )
    )
    start = time.monotonic()
    # Aggregate rather than materialise: the question is scan-and-join
    # throughput, not how fast Arrow can be copied into Python.
    #
    # But the aggregate has to *touch* the projected columns. A bare `count(*)`
    # lets DuckDB prune every column away, which made both arms below read
    # nothing and report identical times — the projection comparison measured
    # the projection being optimised out rather than the bytes it saves.
    touch = "sum(avg_rating_30d)"
    if with_blob:
        touch += ", sum(octet_length(state_value))"
    n = con.sql(f"SELECT count(*), {touch} FROM ({sql})").fetchone()[0]
    elapsed = time.monotonic() - start
    con.close()
    return n, elapsed


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--entities", type=int, default=1_000_000)
    ap.add_argument("--history", type=int, default=20_000_000)
    ap.add_argument("--spine", type=int, default=20_000_000)
    ap.add_argument("--threads", type=int, default=0, help="0 = DuckDB default")
    ap.add_argument("--keep", action="store_true", help="keep the generated data")
    ap.add_argument(
        "--memory-limit",
        default="6GB",
        help=(
            "DuckDB memory cap. Deliberately well under the machine's RAM: "
            "this runs beside the developer's other work, and DuckDB's own "
            "default of ~80%% of RAM is enough to take the box down."
        ),
    )
    ap.add_argument(
        "--workdir",
        type=Path,
        default=Path.home() / ".cache" / "thyme-offline-bench",
        help=(
            "Where the generated history is written. NOT /tmp by default, and "
            "deliberately: /tmp is a 16 GB tmpfs on this machine, so a run "
            "there measures RAM and reports it as disk."
        ),
    )
    args = ap.parse_args()

    versions = max(1, args.history // args.entities)
    args.workdir.mkdir(parents=True, exist_ok=True)
    workdir = Path(tempfile.mkdtemp(prefix="run-", dir=args.workdir))
    fs = subprocess.run(
        ["findmnt", "-no", "FSTYPE", "--target", str(workdir)],
        capture_output=True,
        text=True,
    ).stdout.strip()
    print(f"workdir: {workdir} ({fs or 'unknown fs'})")
    if fs == "tmpfs":
        print("  WARNING: tmpfs is RAM. Throughput below is not a disk number.")
    history_dir = workdir / "history"

    print(
        f"history: {args.entities:,} entities x {versions} versions "
        f"= {args.entities * versions:,} rows"
    )
    t0 = time.monotonic()
    size = generate_history(history_dir, args.entities, versions)
    print(f"  generated in {time.monotonic() - t0:,.1f}s, {_human(size)} on disk")

    print(f"spine: {args.spine:,} rows")
    spine = generate_spine(args.entities, args.spine)

    threads = args.threads or duckdb.connect().execute(
        "SELECT current_setting('threads')"
    ).fetchone()[0]

    try:
        print("\n--- projected (entity_id, ts, features) ---")
        rows, elapsed = run(
            history_dir, spine, FEATURE_COLUMNS, threads,
            memory_limit=args.memory_limit,
        )
        rate = rows / elapsed
        print(f"  {rows:,} rows in {elapsed:,.1f}s = {rate:,.0f} rows/sec")
        target = 20_000_000 / rate
        print(f"  implied time for a 20M-row spine: {target / 60:,.1f} min")

        print("\n--- with state_value, i.e. what SELECT * costs ---")
        blob_rows, blob_elapsed = run(
            history_dir, spine, FEATURE_COLUMNS, threads,
            memory_limit=args.memory_limit, with_blob=True,
        )
        print(
            f"  {blob_rows:,} rows in {blob_elapsed:,.1f}s "
            f"= {blob_rows / blob_elapsed:,.0f} rows/sec"
        )
        print(f"  projection is {blob_elapsed / elapsed:,.1f}x faster")
    finally:
        if not args.keep:
            shutil.rmtree(workdir, ignore_errors=True)
        else:
            print(f"\nkept: {workdir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
