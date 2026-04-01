"""Schema introspection tools for Thyme source connectors.

Each introspect_* function connects to a data source, reads column metadata,
and returns an IntrospectedSchema with field names, Python-mapped types, and
a small sample of rows.
"""
from dataclasses import dataclass, field
from typing import Any


@dataclass
class IntrospectedField:
    name: str
    type: str  # Python type string: "str", "int", "float", "bool", "datetime"
    nullable: bool = True


@dataclass
class IntrospectedSchema:
    fields: list[IntrospectedField] = field(default_factory=list)
    sample_rows: list[dict] = field(default_factory=list)
    row_count_estimate: int | None = None


def _duckdb_type_to_python(duckdb_type: str) -> str:
    """Map a DuckDB column type string to the closest Python type string."""
    t = duckdb_type.upper().strip()
    if any(t.startswith(k) for k in ("INT", "BIGINT", "SMALLINT", "TINYINT", "HUGEINT", "UBIGINT", "UINTEGER")):
        return "int"
    if any(t.startswith(k) for k in ("FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL")):
        return "float"
    if t == "BOOLEAN":
        return "bool"
    if any(t.startswith(k) for k in ("TIMESTAMP", "DATE", "TIME")):
        return "datetime"
    if any(t.startswith(k) for k in ("VARCHAR", "TEXT", "STRING", "CHAR", "UUID", "JSON", "BLOB")):
        return "str"
    return "str"  # safe fallback


def _duckdb_fetch_records(conn: Any, query: str) -> list[dict]:
    """Execute *query* and return results as a list of dicts (no pandas/numpy)."""
    result = conn.execute(query)
    col_names = [d[0] for d in result.description]
    return [dict(zip(col_names, row)) for row in result.fetchall()]


def introspect_jsonl(path: str, sample_n: int = 5) -> IntrospectedSchema:
    """Introspect a JSONL file using DuckDB."""
    import duckdb

    conn = duckdb.connect()
    describe_rows = conn.execute(f"DESCRIBE SELECT * FROM read_json_auto('{path}')").fetchall()
    fields = [IntrospectedField(name=r[0], type=_duckdb_type_to_python(r[1])) for r in describe_rows]
    sample_rows = _duckdb_fetch_records(conn, f"SELECT * FROM read_json_auto('{path}') LIMIT {sample_n}")
    return IntrospectedSchema(fields=fields, sample_rows=sample_rows)


def introspect_iceberg(catalog: str, database: str, table: str, sample_n: int = 5) -> IntrospectedSchema:
    """Introspect an Iceberg table using DuckDB + iceberg extension."""
    import duckdb

    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    table_ref = f"iceberg_scan('{catalog}.{database}.{table}')"
    describe_rows = conn.execute(f"DESCRIBE SELECT * FROM {table_ref}").fetchall()
    fields = [IntrospectedField(name=r[0], type=_duckdb_type_to_python(r[1])) for r in describe_rows]
    sample_rows = _duckdb_fetch_records(conn, f"SELECT * FROM {table_ref} LIMIT {sample_n}")
    return IntrospectedSchema(fields=fields, sample_rows=sample_rows)


def introspect_postgres(
    host: str,
    port: int,
    database: str,
    table: str,
    user: str,
    password: str,
    schema: str = "public",
    sample_n: int = 5,
) -> IntrospectedSchema:
    """Introspect a Postgres table using psycopg3."""
    import psycopg

    PG_TO_PYTHON: dict[str, str] = {
        "integer": "int",
        "bigint": "int",
        "smallint": "int",
        "serial": "int",
        "bigserial": "int",
        "real": "float",
        "double precision": "float",
        "numeric": "float",
        "decimal": "float",
        "boolean": "bool",
        "timestamp without time zone": "datetime",
        "timestamp with time zone": "datetime",
        "date": "datetime",
        "time without time zone": "datetime",
        "character varying": "str",
        "text": "str",
        "char": "str",
        "uuid": "str",
        "json": "str",
        "jsonb": "str",
    }

    conn_str = f"host={host} port={port} dbname={database} user={user} password={password}"
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type "
                "FROM information_schema.columns "
                "WHERE table_name = %s AND table_schema = %s "
                "ORDER BY ordinal_position",
                (table, schema),
            )
            col_rows = cur.fetchall()
            fields = [
                IntrospectedField(name=r[0], type=PG_TO_PYTHON.get(r[1], "str"))
                for r in col_rows
            ]
            cur.execute(f'SELECT * FROM "{schema}"."{table}" LIMIT %s', (sample_n,))
            col_names = [d[0] for d in cur.description]
            sample_rows = [dict(zip(col_names, row)) for row in cur.fetchall()]

    return IntrospectedSchema(fields=fields, sample_rows=sample_rows)


def introspect_s3json(
    bucket: str,
    prefix: str = "",
    region: str = "us-east-1",
    sample_n: int = 5,
) -> IntrospectedSchema:
    """Introspect JSON files in S3 using DuckDB + httpfs extension."""
    import duckdb

    conn = duckdb.connect()
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute(f"SET s3_region='{region}';")

    path = f"s3://{bucket}/{prefix.lstrip('/')}"
    if not path.endswith((".json", ".jsonl", "*")):
        path = path.rstrip("/") + "/*.json"

    describe_rows = conn.execute(f"DESCRIBE SELECT * FROM read_json_auto('{path}')").fetchall()
    fields = [IntrospectedField(name=r[0], type=_duckdb_type_to_python(r[1])) for r in describe_rows]
    sample_rows = _duckdb_fetch_records(conn, f"SELECT * FROM read_json_auto('{path}') LIMIT {sample_n}")
    return IntrospectedSchema(fields=fields, sample_rows=sample_rows)


def introspect_kafka(
    brokers: str,
    topic: str,
    format: str = "json",
    sample_n: int = 5,
) -> IntrospectedSchema:
    """Introspect a Kafka topic by consuming sample messages.

    Requires the ``confluent-kafka`` package. Install with:
        pip install confluent-kafka
    """
    try:
        from confluent_kafka import Consumer
    except ImportError:
        raise ImportError(
            "Kafka introspection requires confluent-kafka. "
            "Install with: pip install confluent-kafka"
        )

    import json as json_mod
    import uuid

    consumer = Consumer({
        "bootstrap.servers": brokers,
        "group.id": f"thyme-introspect-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])

    rows: list[dict] = []
    try:
        for _ in range(sample_n * 10):  # poll up to 10x to get enough messages
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                break
            if msg.error():
                continue
            if format == "json":
                rows.append(json_mod.loads(msg.value().decode("utf-8")))
            if len(rows) >= sample_n:
                break
    finally:
        consumer.close()

    if not rows:
        return IntrospectedSchema()

    # Infer fields from first message
    sample = rows[0]
    fields = []
    for key, value in sample.items():
        if isinstance(value, bool):
            t = "bool"
        elif isinstance(value, int):
            t = "int"
        elif isinstance(value, float):
            t = "float"
        else:
            t = "str"
        fields.append(IntrospectedField(name=key, type=t))

    return IntrospectedSchema(fields=fields, sample_rows=rows)


def introspect_schema(connector: Any, sample_n: int = 5) -> IntrospectedSchema:
    """Introspect a source connector and return its schema.

    Accepts either a connector object with a ``to_dict()`` method or a raw
    connector config dict (as produced by ``connector.to_dict()``).
    """
    config = connector.to_dict() if hasattr(connector, "to_dict") else connector
    ct: str = config["connector_type"]
    cfg: dict = config.get("config", {})

    if ct == "iceberg":
        return introspect_iceberg(cfg["catalog"], cfg["database"], cfg["table"], sample_n)
    if ct == "postgres":
        return introspect_postgres(
            cfg["host"], cfg["port"], cfg["database"], cfg["table"],
            cfg["user"], cfg["password"], cfg.get("schema", "public"), sample_n,
        )
    if ct == "s3json":
        return introspect_s3json(
            cfg["bucket"], cfg.get("prefix", ""), cfg.get("region", "us-east-1"), sample_n,
        )
    if ct == "kafka":
        return introspect_kafka(
            cfg["brokers"], cfg["topic"], cfg.get("format", "json"), sample_n,
        )
    if ct == "jsonl":
        return introspect_jsonl(cfg["path"], sample_n)

    raise ValueError(f"Unsupported connector type for introspection: {ct!r}")


def sample_data(connector: Any, n: int = 100) -> list[dict]:
    """Pull up to *n* sample rows from a connector."""
    return introspect_schema(connector, sample_n=n).sample_rows
