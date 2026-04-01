from abc import ABC, abstractmethod
from typing import Any, Callable

from thyme.duration import parse_duration


class Connector(ABC):
    """Abstract base for all source connectors."""

    connector_type: str
    required_fields: tuple[str, ...] = ()

    @abstractmethod
    def to_dict(self) -> dict:
        """Return a dict with 'connector_type' and 'config' keys."""
        ...

    def validate(self) -> None:
        """Validate that required fields are non-empty."""
        for field_name in self.required_fields:
            value = getattr(self, field_name, None)
            if not value and value != 0:
                raise ValueError(
                    f"{self.__class__.__name__}: '{field_name}' must not be empty"
                )


class SQLSource(Connector):
    """Base class for SQL database connectors."""

    required_fields = ("host", "database", "table", "user", "password")

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        table: str,
        user: str,
        password: str,
        schema: str = "public",
        sslmode: str = "prefer",
    ):
        self.host = host
        self.port = port
        self.database = database
        self.table = table
        self.user = user
        self.password = password
        self.schema = schema
        self.sslmode = sslmode
        self.validate()

    def to_dict(self) -> dict:
        return {
            "connector_type": self.connector_type,
            "config": {
                "host": self.host,
                "port": self.port,
                "database": self.database,
                "table": self.table,
                "user": self.user,
                "password": self.password,
                "schema": self.schema,
                "sslmode": self.sslmode,
            },
        }


class IcebergSource(Connector):
    """Configuration for an Iceberg table source."""

    connector_type = "iceberg"
    required_fields = ("catalog", "database", "table")

    def __init__(self, catalog: str, database: str, table: str):
        self.catalog = catalog
        self.database = database
        self.table = table
        self.validate()

    def to_dict(self) -> dict:
        return {
            "connector_type": self.connector_type,
            "config": {
                "catalog": self.catalog,
                "database": self.database,
                "table": self.table,
            },
        }


class PostgresSource(SQLSource):
    """Configuration for a Postgres table source."""

    connector_type = "postgres"

    def __init__(
        self,
        host: str,
        port: int = 5432,
        database: str = "",
        table: str = "",
        user: str = "",
        password: str = "",
        schema: str = "public",
        sslmode: str = "prefer",
    ):
        super().__init__(
            host=host, port=port, database=database, table=table,
            user=user, password=password, schema=schema, sslmode=sslmode,
        )


class S3JsonSource(Connector):
    """Configuration for a JSON/JSONL files source stored in S3."""

    connector_type = "s3json"
    required_fields = ("bucket",)

    def __init__(self, bucket: str, prefix: str = "", region: str = "us-east-1"):
        self.bucket = bucket
        self.prefix = prefix
        self.region = region
        self.validate()

    def to_dict(self) -> dict:
        return {
            "connector_type": self.connector_type,
            "config": {
                "bucket": self.bucket,
                "prefix": self.prefix,
                "region": self.region,
            },
        }


class KafkaSource(Connector):
    """Configuration for a Kafka topic source."""

    connector_type = "kafka"
    required_fields = ("brokers", "topic")

    _VALID_SECURITY_PROTOCOLS = {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}
    _VALID_FORMATS = {"json", "avro", "protobuf"}

    def __init__(
        self,
        brokers: str,
        topic: str,
        security_protocol: str = "PLAINTEXT",
        sasl_mechanism: str = "",
        sasl_username: str = "",
        sasl_password: str = "",
        format: str = "json",
        group_id: str = "",
        schema_registry_url: str = "",
    ):
        if security_protocol not in self._VALID_SECURITY_PROTOCOLS:
            raise ValueError(
                f"Invalid security_protocol '{security_protocol}'. "
                f"Must be one of {sorted(self._VALID_SECURITY_PROTOCOLS)}."
            )
        if format not in self._VALID_FORMATS:
            raise ValueError(
                f"Invalid format '{format}'. "
                f"Must be one of {sorted(self._VALID_FORMATS)}."
            )
        self.brokers = brokers
        self.topic = topic
        self.security_protocol = security_protocol
        self.sasl_mechanism = sasl_mechanism
        self.sasl_username = sasl_username
        self.sasl_password = sasl_password
        self.format = format
        self.group_id = group_id
        self.schema_registry_url = schema_registry_url
        self.validate()

    def to_dict(self) -> dict:
        return {
            "connector_type": self.connector_type,
            "config": {
                "brokers": self.brokers,
                "topic": self.topic,
                "security_protocol": self.security_protocol,
                "sasl_mechanism": self.sasl_mechanism,
                "sasl_username": self.sasl_username,
                "sasl_password": self.sasl_password,
                "format": self.format,
                "group_id": self.group_id,
                "schema_registry_url": self.schema_registry_url,
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

    if not isinstance(connector, Connector):
        raise TypeError(
            f"connector must be a Connector instance, got {type(connector).__name__}"
        )

    _VALID_CDC_MODES = {"append", "debezium", "upsert"}
    if cdc not in _VALID_CDC_MODES:
        raise ValueError(
            f"Invalid cdc mode '{cdc}'. Must be one of {sorted(_VALID_CDC_MODES)}."
        )

    for param_name, param_value in [("every", every), ("disorder", disorder)]:
        if param_value:
            try:
                parse_duration(param_value)
            except ValueError as e:
                raise ValueError(f"Invalid {param_name} duration: {e}") from e

    def wrapper(cls: type) -> type:
        if cursor and hasattr(cls, "_dataset_meta"):
            field_names = {f["name"] for f in cls._dataset_meta["fields"]}
            if cursor not in field_names:
                raise ValueError(
                    f"cursor '{cursor}' is not a field of dataset '{cls.__name__}'. "
                    f"Available fields: {sorted(field_names)}"
                )

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
