from typing import Any, Callable, Optional


class IcebergSource:
    """Configuration for an Iceberg table source."""

    def __init__(self, catalog: str, database: str, table: str):
        self.catalog = catalog
        self.database = database
        self.table = table

    def to_dict(self) -> dict:
        return {
            "connector_type": "iceberg",
            "config": {
                "catalog": self.catalog,
                "database": self.database,
                "table": self.table,
            },
        }


class PostgresSource:
    """Configuration for a Postgres table source."""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        table: str,
        user: str,
        password: str,
        schema: str = "public",
    ):
        self.host = host
        self.port = port
        self.database = database
        self.table = table
        self.user = user
        self.password = password
        self.schema = schema

    def to_dict(self) -> dict:
        return {
            "connector_type": "postgres",
            "config": {
                "host": self.host,
                "port": self.port,
                "database": self.database,
                "table": self.table,
                "user": self.user,
                "password": self.password,
                "schema": self.schema,
            },
        }


class S3JsonSource:
    """Configuration for a JSON/JSONL files source stored in S3."""

    def __init__(self, bucket: str, prefix: str = "", region: str = "us-east-1"):
        self.bucket = bucket
        self.prefix = prefix
        self.region = region

    def to_dict(self) -> dict:
        return {
            "connector_type": "s3json",
            "config": {
                "bucket": self.bucket,
                "prefix": self.prefix,
                "region": self.region,
            },
        }


_SOURCE_REGISTRY: dict[str, dict] = {}


def clear_source_registry() -> None:
    _SOURCE_REGISTRY.clear()


def get_registered_sources() -> dict[str, dict]:
    from copy import deepcopy
    return deepcopy(_SOURCE_REGISTRY)


def source(
    connector: Any,
    cursor: str = "",
    every: str = "",
    disorder: str = "",
    cdc: str = "append",
) -> Callable:
    """Decorator to attach a source connector to a dataset class.

    Args:
        connector: The source connector (e.g. IcebergSource).
        cursor: The field to use as an incremental cursor.
        every: Poll interval (e.g. "5m", "1h").
        disorder: Maximum expected out-of-order delay (e.g. "1h", "1d").
            Events arriving later than (max_event_time - disorder) are
            discarded. This sets the watermark for all downstream pipelines.
    """

    _VALID_CDC_MODES = {"append", "debezium", "upsert"}
    if cdc not in _VALID_CDC_MODES:
        raise ValueError(
            f"Invalid cdc mode '{cdc}'. Must be one of {sorted(_VALID_CDC_MODES)}."
        )

    def wrapper(cls: type) -> type:
        source_meta = connector.to_dict()
        source_meta["dataset"] = cls.__name__
        source_meta["cursor"] = cursor
        source_meta["every"] = every
        source_meta["disorder"] = disorder
        source_meta["cdc"] = cdc
        _SOURCE_REGISTRY[cls.__name__] = source_meta
        cls._source_meta = source_meta
        return cls

    return wrapper
