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
import tempfile
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
    #: The name the catalog is ATTACHed as. Part of the config rather than a
    #: loose argument because :meth:`table` and :func:`attach_sql` have to agree
    #: — a qualified name missing the alias resolves against DuckDB's own
    #: schemas and fails with "schema does not exist".
    alias: str = "ice"
    #: REST endpoint, or the AWS account id when ``type`` is ``glue``.
    uri: str = "http://localhost:8181"
    #: Iceberg namespace holding one table per entity type.
    database: str = "thyme"
    region: str = "us-east-1"
    #: The REST catalog's warehouse. This is what DuckDB attaches to for a REST
    #: catalog; namespaces live inside it. Falls back to `database` when unset.
    warehouse: str = ""
    #: REST auth. DuckDB defaults to `oauth2`, which fails against an
    #: unauthenticated catalog, so `none` is the default here and the local and
    #: CI catalogs work without configuration.
    authorization_type: str = "none"
    #: Object-store overrides. Empty means "use the ambient provider chain",
    #: which is what a deployment on IRSA needs — see :func:`attach_sql`.
    s3_endpoint: str = ""
    s3_access_key: str = field(default="", repr=False)
    s3_secret_key: str = field(default="", repr=False)

    #: Where DuckDB spills when a join exceeds memory.
    #:
    #: **Not optional in practice.** An in-memory DuckDB with no temp directory
    #: cannot spill at all: a join larger than RAM does not degrade, it takes
    #: the process — and on a machine where the pull is running beside other
    #: work, that is the machine. A training pull is exactly the shape that
    #: exceeds memory, so this defaults to the system temp directory rather than
    #: to "off".
    #:
    #: Point it at real disk. On many Linux setups `/tmp` is a tmpfs, i.e. RAM,
    #: and spilling there is not spilling — it is the same OOM by a longer road.
    temp_directory: str = ""
    #: Cap on DuckDB's memory before it spills. Empty leaves DuckDB's own
    #: default (about 80% of RAM). Set it lower to leave room for whatever else
    #: the machine is doing.
    memory_limit: str = ""
    #: DuckDB worker threads. Empty leaves the default (one per core).
    threads: str = ""

    @classmethod
    def from_env(cls) -> CatalogConfig:
        return cls(
            type=_env("THYME_ICEBERG_CATALOG", "rest"),
            uri=_env("THYME_ICEBERG_URI", "http://localhost:8181"),
            database=_env("THYME_ICEBERG_DATABASE", "thyme"),
            warehouse=_env("ICEBERG_WAREHOUSE"),
            authorization_type=_env("THYME_ICEBERG_AUTH", "none"),
            region=_env("AWS_REGION", _env("AWS_DEFAULT_REGION", "us-east-1")),
            s3_endpoint=_env("OFFLINE_ENDPOINT"),
            s3_access_key=_env("OFFLINE_ACCESS_KEY"),
            s3_secret_key=_env("OFFLINE_SECRET_KEY"),
            temp_directory=_env("THYME_OFFLINE_TEMP_DIR", tempfile.gettempdir()),
            memory_limit=_env("THYME_OFFLINE_MEMORY_LIMIT"),
            threads=_env("THYME_OFFLINE_THREADS"),
        )

    def table(self, entity_type: str) -> str:
        """The fully-qualified table for an entity type.

        `UserOrderStats` -> `ice.thyme.user_order_stats` — attach alias,
        namespace, table. All three parts are required: DuckDB resolves an
        unqualified `thyme.x` against its own schemas, not the attached catalog.
        """
        return f"{self.alias}.{self.database}.{table_name(entity_type)}"


def _escape(value: str) -> str:
    """Escape a single-quoted SQL literal."""
    return value.replace("'", "''")


def attach_sql(config: CatalogConfig, alias: str | None = None) -> list[str]:
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
    alias = alias or config.alias
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
        # Two things here are easy to get wrong, and both were:
        #
        # The ATTACH target is the **warehouse**, not the namespace. DuckDB asks
        # the REST catalog for a warehouse and finds namespaces inside it, so
        # passing the database attaches nothing that resolves.
        #
        # And `AUTHORIZATION_TYPE` defaults to `oauth2`, which fails outright
        # against an unauthenticated catalog -- the local one the e2e runs. It is
        # configurable rather than pinned to `none` so a deployment behind OAuth2
        # is still reachable.
        warehouse = config.warehouse or config.database
        stmts.append(
            f"ATTACH '{_escape(warehouse)}' AS {alias} "
            f"(TYPE iceberg, ENDPOINT '{_escape(config.uri)}'"
            f", AUTHORIZATION_TYPE '{_escape(config.authorization_type)}')"
        )

    return stmts


def connect(config: CatalogConfig | None = None, alias: str | None = None):
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
    for stmt in resource_sql(config):
        con.execute(stmt)
    for stmt in attach_sql(config, alias=alias):
        con.execute(stmt)
    return con


def resource_sql(config: CatalogConfig) -> list[str]:
    """Memory and spill settings, applied before anything is attached.

    Separate from :func:`attach_sql` because these are about the machine rather
    than the catalog, and returned rather than executed for the same reason: so
    they can be asserted without a DuckDB instance.

    The temp directory is the one that matters. Without it a join larger than
    memory cannot spill and the process dies; with it the same join gets slower.
    See :attr:`CatalogConfig.temp_directory` for why the default is not `/tmp`
    on every machine.
    """
    stmts = []
    if config.temp_directory:
        stmts.append(f"SET temp_directory = '{_escape(config.temp_directory)}'")
    if config.memory_limit:
        stmts.append(f"SET memory_limit = '{_escape(config.memory_limit)}'")
    if config.threads:
        stmts.append(f"SET threads = {int(config.threads)}")
    return stmts
