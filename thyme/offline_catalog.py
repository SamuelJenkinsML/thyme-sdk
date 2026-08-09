"""Resolving and attaching the Iceberg catalog for offline reads (TH-312).

The catalog is only a name -> current-metadata-pointer store with atomic
compare-and-swap on commit; the data is plain Parquet and Iceberg metadata in an
object store we own. So which catalog we use is a deployment choice, not an
architectural one, and it is selected by config exactly as the Go sink selects
it. Glue runs on the demo cluster; REST is the default and what CI exercises.

Settings mirror `offline-sink/cmd/thyme-offline-sink/main.go` deliberately: an
operator configuring the sink and a user pulling training data should not have
to learn two vocabularies. `THYME_ICEBERG_CATALOG` is the catalog *type*, and
both sides default it to ``rest``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

#: Catalog types this reader can attach. Mirrors the sink's `loadCatalog`.
CATALOG_TYPES = ("rest", "glue", "sql")


def _env(name: str, default: str = "") -> str:
    value = os.environ.get(name)
    return value if value else default


def table_name(entity_type: str) -> str:
    """Map an entity type to its table name: `UserOrderStats` -> `user_order_stats`.

    **Must stay byte-identical to `sink.TableName`** in
    `offline-sink/internal/sink/schema.go`. The sink creates the table under this
    name and the reader looks it up under this name; disagreeing means reading a
    table that does not exist, which surfaces as "no data" rather than as an
    error.

    Lower snake case because catalogs differ on identifier case folding — Glue
    lowercases, so a table created as `UserOrderStats` can come back as
    `userorderstats`.

    A separator goes in only between a lower/digit and an upper, so `HTTPStats`
    becomes `http_stats` rather than `h_t_t_p_stats`.
    """
    out: list[str] = []
    for i, ch in enumerate(entity_type):
        if "A" <= ch <= "Z":
            if i > 0:
                prev = entity_type[i - 1]
                if ("a" <= prev <= "z") or ("0" <= prev <= "9"):
                    out.append("_")
            out.append(ch.lower())
        else:
            out.append(ch)
    return "".join(out)


@dataclass(frozen=True)
class CatalogConfig:
    """Where the offline tables live, and how to reach them."""

    #: One of :data:`CATALOG_TYPES`.
    type: str = "rest"
    #: REST endpoint, or the AWS account id when ``type`` is ``glue``.
    uri: str = "http://localhost:8181"
    #: Iceberg namespace holding one table per entity type.
    database: str = "thyme"
    region: str = "us-east-1"
    #: Object-store overrides. Empty means "use the ambient provider chain",
    #: which is what a deployment on IRSA needs — see :func:`attach_sql`.
    s3_endpoint: str = ""
    s3_access_key: str = field(default="", repr=False)
    s3_secret_key: str = field(default="", repr=False)

    @classmethod
    def from_env(cls) -> CatalogConfig:
        return cls(
            type=_env("THYME_ICEBERG_CATALOG", "rest"),
            uri=_env("THYME_ICEBERG_URI", "http://localhost:8181"),
            database=_env("THYME_ICEBERG_DATABASE", "thyme"),
            region=_env("AWS_REGION", _env("AWS_DEFAULT_REGION", "us-east-1")),
            s3_endpoint=_env("OFFLINE_ENDPOINT"),
            s3_access_key=_env("OFFLINE_ACCESS_KEY"),
            s3_secret_key=_env("OFFLINE_SECRET_KEY"),
        )

    def table(self, entity_type: str) -> str:
        """The fully-qualified table for an entity type.

        `UserOrderStats` -> `thyme.user_order_stats`.
        """
        return f"{self.database}.{table_name(entity_type)}"


def _escape(value: str) -> str:
    """Escape a single-quoted SQL literal."""
    return value.replace("'", "''")


def attach_sql(config: CatalogConfig, alias: str = "ice") -> list[str]:
    """The statements that prepare a DuckDB connection to read the store.

    Returned rather than executed so the wiring is unit-testable without a
    catalog, a network, or AWS credentials.

    ## Credentials

    When object-store keys are configured they are set explicitly — that is the
    MinIO case the e2e runs. When they are **not**, the ambient provider chain is
    used instead of empty strings, because empty credentials override IRSA with
    an anonymous identity. The Go sink documents the same trap, as does
    `thyme_common::object_store::build_s3_builder` on the Rust side.
    """
    if config.type not in CATALOG_TYPES:
        raise ValueError(
            f"unknown Iceberg catalog type {config.type!r}; "
            f"expected one of {', '.join(CATALOG_TYPES)}. "
            f"Set THYME_ICEBERG_CATALOG to match the sink's."
        )

    stmts = [
        "INSTALL iceberg",
        "LOAD iceberg",
        "INSTALL httpfs",
        "LOAD httpfs",
    ]

    if config.s3_access_key and config.s3_secret_key:
        secret = [
            "CREATE OR REPLACE SECRET thyme_offline (",
            "  TYPE s3",
            f", KEY_ID '{_escape(config.s3_access_key)}'",
            f", SECRET '{_escape(config.s3_secret_key)}'",
            f", REGION '{_escape(config.region)}'",
        ]
        if config.s3_endpoint:
            # A custom endpoint is MinIO in practice: path-style addressing and
            # no TLS, matching how the e2e stands it up.
            secret.append(f", ENDPOINT '{_escape(config.s3_endpoint)}'")
            secret.append(", URL_STYLE 'path'")
            secret.append(", USE_SSL false")
        secret.append(")")
        stmts.append("".join(secret))
    else:
        stmts += ["INSTALL aws", "LOAD aws"]
        stmts.append(
            "CREATE OR REPLACE SECRET thyme_offline ("
            "TYPE s3, PROVIDER credential_chain"
            f", REGION '{_escape(config.region)}')"
        )

    if config.type == "glue":
        # Verified against AWS on 2026-08-09: DuckDB 1.5.5 attaches with the
        # account id and takes its endpoint from the AWS SDK, so only the region
        # matters — the same asymmetry the sink's `loadCatalog` documents.
        stmts.append(
            f"ATTACH '{_escape(config.uri)}' AS {alias} "
            f"(TYPE iceberg, ENDPOINT_TYPE 'glue')"
        )
    else:
        stmts.append(
            f"ATTACH '{_escape(config.database)}' AS {alias} "
            f"(TYPE iceberg, ENDPOINT '{_escape(config.uri)}')"
        )

    return stmts


def connect(config: CatalogConfig | None = None, alias: str = "ice"):
    """A DuckDB connection with the offline store attached.

    DuckDB rather than PyIceberg, and that is a correctness requirement:
    PyIceberg is ``N`` for "Read with equality deletes" in the Iceberg support
    matrix and Polars delegates its Iceberg scan to it, so after a backfill
    either returns superseded rows *quietly*. See `thyme/offline_iceberg.py`.
    """
    try:
        import duckdb
    except ImportError as exc:  # pragma: no cover - import guard
        raise ImportError(
            "duckdb is required to read the offline store. "
            "Install it with: pip install 'thyme-sdk[offline]'"
        ) from exc

    config = config or CatalogConfig.from_env()
    con = duckdb.connect()
    for stmt in attach_sql(config, alias=alias):
        con.execute(stmt)
    return con
