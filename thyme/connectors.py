from datetime import datetime
from typing import Any, Callable


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
                "sslmode": self.sslmode,
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


class KafkaSource:
    """Configuration for a Kafka topic source."""

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

    def to_dict(self) -> dict:
        return {
            "connector_type": "kafka",
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


class KinesisSource:
    """Configuration for an AWS Kinesis stream source."""

    _VALID_INIT_POSITIONS = {"latest", "trim_horizon"}
    _VALID_FORMATS = {"json"}

    def __init__(
        self,
        stream_arn: str,
        role_arn: str = "",
        region: str = "us-east-1",
        init_position: str = "latest",
        format: str = "json",
        endpoint_url: str | None = None,
    ):
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
        self.stream_arn = stream_arn
        self.role_arn = role_arn
        self.region = region
        self.init_position = init_position
        self.format = format
        self.endpoint_url = endpoint_url

    def to_dict(self) -> dict:
        config = {
            "stream_arn": self.stream_arn,
            "role_arn": self.role_arn,
            "region": self.region,
            "init_position": self.init_position,
            "format": self.format,
        }
        if self.endpoint_url is not None:
            config["endpoint_url"] = self.endpoint_url
        return {"connector_type": "kinesis", "config": config}


class SnowflakeSource:
    """Configuration for a Snowflake table source."""

    def __init__(
        self,
        account: str,
        database: str,
        warehouse: str,
        table: str,
        user: str,
        password: str,
        schema: str = "PUBLIC",
        role: str = "",
    ):
        self.account = account
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.role = role
        self.table = table
        self.user = user
        self.password = password

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
                "password": self.password,
            },
        }


class BigQuerySource:
    """Configuration for a BigQuery table source."""

    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        table: str,
        credentials_json: str = "",
    ):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table = table
        self.credentials_json = credentials_json

    def to_dict(self) -> dict:
        return {
            "connector_type": "bigquery",
            "config": {
                "project_id": self.project_id,
                "dataset_id": self.dataset_id,
                "table": self.table,
                "credentials_json": self.credentials_json,
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
        source_meta["disorder"] = disorder
        source_meta["cdc"] = cdc
        _SOURCE_REGISTRY[cls.__name__] = source_meta
        cls._source_meta = source_meta
        return cls

    return wrapper
