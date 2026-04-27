from datetime import datetime
from typing import Any, Callable

from thyme.env_defaults import env_default, env_default_int
from thyme.secret import Secret


def _credential_dict(value: str | Secret | None) -> dict:
    """Encode a credential field as a tagged dict.

    Literal strings and Secret refs share one shape so the wire format and
    engine-side deserializer treat them uniformly. The engine resolves
    `kind="env"|"arn"|"name"` at connector init; `kind="literal"` is used as-is.
    """
    if isinstance(value, Secret):
        return {"kind": value.kind, "value": value.value}
    return {"kind": "literal", "value": value or ""}


def _require(connector_type: str, field: str, value: object, hint: str = "") -> str:
    """Per-dataset fields can't fall back to env vars. Fail fast on missing."""
    if value is None or value == "":
        suffix = f" {hint}" if hint else ""
        raise ValueError(
            f"{connector_type} requires '{field}' (per-dataset, no env fallback).{suffix}"
        )
    return value  # type: ignore[return-value]


class IcebergSource:
    """Configuration for an Iceberg table source.

    Per-dataset (required): table.
    Env-defaulted (THYME_ICEBERG_*): catalog, database.
    """

    def __init__(
        self,
        *,
        table: str,
        catalog: str | None = None,
        database: str | None = None,
    ):
        self.table = _require("IcebergSource", "table", table)
        self.catalog = catalog or env_default("iceberg", "catalog", default="")
        self.database = database or env_default("iceberg", "database", default="")

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
    """Configuration for a Postgres table source.

    Per-dataset (required): table.
    Env-defaulted (THYME_POSTGRES_*): host, port, database, user, schema, sslmode.
    Secret-capable: password.
    """

    def __init__(
        self,
        *,
        table: str,
        host: str | None = None,
        port: int | None = None,
        database: str | None = None,
        user: str | None = None,
        password: str | Secret | None = None,
        schema: str | None = None,
        sslmode: str | None = None,
    ):
        self.table = _require("PostgresSource", "table", table)
        self.host = host or env_default("postgres", "host", default="localhost")
        self.port = port if port is not None else env_default_int("postgres", "port", default=5432)
        self.database = database or env_default("postgres", "database", default="")
        self.user = user or env_default("postgres", "user", default="")
        self.password: str | Secret = password if password is not None else env_default("postgres", "password", default="")
        self.schema = schema or env_default("postgres", "schema", default="public")
        self.sslmode = sslmode or env_default("postgres", "sslmode", default="prefer")

    def to_dict(self) -> dict:
        return {
            "connector_type": "postgres",
            "config": {
                "host": self.host,
                "port": self.port,
                "database": self.database,
                "table": self.table,
                "user": self.user,
                "password": _credential_dict(self.password),
                "schema": self.schema,
                "sslmode": self.sslmode,
            },
        }


class S3JsonSource:
    """Configuration for a JSON/JSONL files source stored in S3.

    Per-dataset (required): bucket. (`prefix` defaults to empty.)
    Env-defaulted (THYME_S3_*): region.
    """

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str = "",
        region: str | None = None,
    ):
        self.bucket = _require("S3JsonSource", "bucket", bucket)
        self.prefix = prefix
        self.region = region or env_default("s3", "region", default="us-east-1")

    def to_dict(self) -> dict:
        return {
            "connector_type": "s3json",
            "config": {
                "bucket": self.bucket,
                "prefix": self.prefix,
                "region": self.region,
            },
        }


class KafkaSource:
    """Configuration for a Kafka topic source.

    Per-dataset (required): topic.
    Env-defaulted (THYME_KAFKA_*): brokers, security_protocol, sasl_mechanism,
        sasl_username, format, group_id, schema_registry_url.
    Secret-capable: sasl_password.
    """

    _VALID_SECURITY_PROTOCOLS = {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}
    _VALID_FORMATS = {"json", "avro", "protobuf"}

    def __init__(
        self,
        *,
        topic: str,
        brokers: str | None = None,
        security_protocol: str | None = None,
        sasl_mechanism: str | None = None,
        sasl_username: str | None = None,
        sasl_password: str | Secret | None = None,
        format: str | None = None,
        group_id: str | None = None,
        schema_registry_url: str | None = None,
    ):
        self.topic = _require("KafkaSource", "topic", topic)
        self.brokers = brokers or env_default("kafka", "brokers", default="")
        self.security_protocol = security_protocol or env_default("kafka", "security_protocol", default="PLAINTEXT")
        if self.security_protocol not in self._VALID_SECURITY_PROTOCOLS:
            raise ValueError(
                f"Invalid security_protocol '{self.security_protocol}'. "
                f"Must be one of {sorted(self._VALID_SECURITY_PROTOCOLS)}."
            )
        self.sasl_mechanism = sasl_mechanism or env_default("kafka", "sasl_mechanism", default="")
        self.sasl_username = sasl_username or env_default("kafka", "sasl_username", default="")
        self.sasl_password: str | Secret = sasl_password if sasl_password is not None else env_default("kafka", "sasl_password", default="")
        self.format = format or env_default("kafka", "format", default="json")
        if self.format not in self._VALID_FORMATS:
            raise ValueError(
                f"Invalid format '{self.format}'. "
                f"Must be one of {sorted(self._VALID_FORMATS)}."
            )
        self.group_id = group_id or env_default("kafka", "group_id", default="")
        self.schema_registry_url = schema_registry_url or env_default("kafka", "schema_registry_url", default="")

    def to_dict(self) -> dict:
        return {
            "connector_type": "kafka",
            "config": {
                "brokers": self.brokers,
                "topic": self.topic,
                "security_protocol": self.security_protocol,
                "sasl_mechanism": self.sasl_mechanism,
                "sasl_username": self.sasl_username,
                "sasl_password": _credential_dict(self.sasl_password),
                "format": self.format,
                "group_id": self.group_id,
                "schema_registry_url": self.schema_registry_url,
            },
        }


class KinesisSource:
    """Configuration for an AWS Kinesis stream source.

    Per-dataset (required): stream_arn. Per-stream semantics: init_position, format.
    Env-defaulted (THYME_KINESIS_*): region, endpoint_url.
    Secret-capable: role_arn.
    """

    _VALID_INIT_POSITIONS = {"latest", "trim_horizon"}
    _VALID_FORMATS = {"json"}

    def __init__(
        self,
        *,
        stream_arn: str,
        role_arn: str | Secret | None = None,
        region: str | None = None,
        init_position: str = "latest",
        format: str = "json",
        endpoint_url: str | None = None,
    ):
        self.stream_arn = _require("KinesisSource", "stream_arn", stream_arn)
        if init_position not in self._VALID_INIT_POSITIONS:
            try:
                datetime.fromisoformat(init_position)
            except ValueError:
                raise ValueError(
                    f"Invalid init_position '{init_position}'. "
                    f"Must be one of {sorted(self._VALID_INIT_POSITIONS)} "
                    f"or a valid ISO-8601 timestamp."
                )
        if format not in self._VALID_FORMATS:
            raise ValueError(
                f"Invalid format '{format}'. "
                f"Must be one of {sorted(self._VALID_FORMATS)}."
            )
        self.role_arn: str | Secret = role_arn if role_arn is not None else env_default("kinesis", "role_arn", default="")
        self.region = region or env_default("kinesis", "region", default="us-east-1")
        self.init_position = init_position
        self.format = format
        self.endpoint_url = endpoint_url if endpoint_url is not None else env_default("kinesis", "endpoint_url", default=None)

    def to_dict(self) -> dict:
        config: dict[str, Any] = {
            "stream_arn": self.stream_arn,
            "role_arn": _credential_dict(self.role_arn),
            "region": self.region,
            "init_position": self.init_position,
            "format": self.format,
        }
        if self.endpoint_url:
            config["endpoint_url"] = self.endpoint_url
        return {"connector_type": "kinesis", "config": config}


class SnowflakeSource:
    """Configuration for a Snowflake table source.

    Per-dataset (required): table.
    Env-defaulted (THYME_SNOWFLAKE_*): account, database, warehouse, role, user, schema.
    Secret-capable: password.
    """

    def __init__(
        self,
        *,
        table: str,
        account: str | None = None,
        database: str | None = None,
        warehouse: str | None = None,
        user: str | None = None,
        password: str | Secret | None = None,
        schema: str | None = None,
        role: str | None = None,
    ):
        self.table = _require("SnowflakeSource", "table", table)
        self.account = account or env_default("snowflake", "account", default="")
        self.database = database or env_default("snowflake", "database", default="")
        self.warehouse = warehouse or env_default("snowflake", "warehouse", default="")
        self.user = user or env_default("snowflake", "user", default="")
        self.password: str | Secret = password if password is not None else env_default("snowflake", "password", default="")
        self.schema = schema or env_default("snowflake", "schema", default="PUBLIC")
        self.role = role or env_default("snowflake", "role", default="")

    def to_dict(self) -> dict:
        return {
            "connector_type": "snowflake",
            "config": {
                "account": self.account,
                "database": self.database,
                "schema": self.schema,
                "warehouse": self.warehouse,
                "role": self.role,
                "table": self.table,
                "user": self.user,
                "password": _credential_dict(self.password),
            },
        }


class BigQuerySource:
    """Configuration for a BigQuery table source.

    Per-dataset (required): dataset_id, table.
    Env-defaulted (THYME_BIGQUERY_*): project_id.
    Secret-capable: credentials_json.
    """

    def __init__(
        self,
        *,
        dataset_id: str,
        table: str,
        project_id: str | None = None,
        credentials_json: str | Secret | None = None,
    ):
        self.dataset_id = _require("BigQuerySource", "dataset_id", dataset_id)
        self.table = _require("BigQuerySource", "table", table)
        self.project_id = project_id or env_default("bigquery", "project_id", default="")
        self.credentials_json: str | Secret = credentials_json if credentials_json is not None else env_default("bigquery", "credentials_json", default="")

    def to_dict(self) -> dict:
        return {
            "connector_type": "bigquery",
            "config": {
                "project_id": self.project_id,
                "dataset_id": self.dataset_id,
                "table": self.table,
                "credentials_json": _credential_dict(self.credentials_json),
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
    max_lateness: str = "",
    cdc: str = "append",
) -> Callable:
    """Decorator to attach a source connector to a dataset class.

    Args:
        connector: The source connector (e.g. IcebergSource).
        cursor: The field to use as an incremental cursor.
        every: Poll interval (e.g. "5m", "1h").
        max_lateness: Maximum expected out-of-order delay (e.g. "1h", "1d").
            Events arriving later than (max_event_time - max_lateness) are
            discarded. This sets the watermark for all downstream pipelines.
    """

    _VALID_CDC_MODES = {"append", "debezium", "upsert"}
    if cdc not in _VALID_CDC_MODES:
        raise ValueError(
            f"Invalid cdc mode '{cdc}'. Must be one of {sorted(_VALID_CDC_MODES)}."
        )

    _STREAMING_CONNECTORS = {"kafka", "kinesis"}
    connector_type = connector.to_dict().get("connector_type", "")
    if connector_type in _STREAMING_CONNECTORS:
        if cursor:
            raise ValueError(
                f"Streaming source '{connector_type}' does not support 'cursor'. "
                f"Remove the cursor parameter."
            )
        if every:
            raise ValueError(
                f"Streaming source '{connector_type}' does not support 'every'. "
                f"Remove the every parameter."
            )

    def wrapper(cls: type) -> type:
        source_meta = connector.to_dict()
        source_meta["dataset"] = cls.__name__
        source_meta["cursor"] = cursor
        source_meta["every"] = every
        source_meta["max_lateness"] = max_lateness
        source_meta["cdc"] = cdc
        _SOURCE_REGISTRY[cls.__name__] = source_meta
        cls._source_meta = source_meta
        return cls

    return wrapper
