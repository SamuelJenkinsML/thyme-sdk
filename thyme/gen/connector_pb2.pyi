from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class IcebergSource(_message.Message):
    __slots__ = ("catalog", "database", "table")
    CATALOG_FIELD_NUMBER: _ClassVar[int]
    DATABASE_FIELD_NUMBER: _ClassVar[int]
    TABLE_FIELD_NUMBER: _ClassVar[int]
    catalog: str
    database: str
    table: str
    def __init__(self, catalog: _Optional[str] = ..., database: _Optional[str] = ..., table: _Optional[str] = ...) -> None: ...

class PostgresSource(_message.Message):
    __slots__ = ("host", "port", "database", "table", "user", "password", "schema", "sslmode")
    HOST_FIELD_NUMBER: _ClassVar[int]
    PORT_FIELD_NUMBER: _ClassVar[int]
    DATABASE_FIELD_NUMBER: _ClassVar[int]
    TABLE_FIELD_NUMBER: _ClassVar[int]
    USER_FIELD_NUMBER: _ClassVar[int]
    PASSWORD_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    SSLMODE_FIELD_NUMBER: _ClassVar[int]
    host: str
    port: int
    database: str
    table: str
    user: str
    password: str
    schema: str
    sslmode: str
    def __init__(self, host: _Optional[str] = ..., port: _Optional[int] = ..., database: _Optional[str] = ..., table: _Optional[str] = ..., user: _Optional[str] = ..., password: _Optional[str] = ..., schema: _Optional[str] = ..., sslmode: _Optional[str] = ...) -> None: ...

class S3JsonSource(_message.Message):
    __slots__ = ("bucket", "prefix", "region")
    BUCKET_FIELD_NUMBER: _ClassVar[int]
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    REGION_FIELD_NUMBER: _ClassVar[int]
    bucket: str
    prefix: str
    region: str
    def __init__(self, bucket: _Optional[str] = ..., prefix: _Optional[str] = ..., region: _Optional[str] = ...) -> None: ...

class KafkaSource(_message.Message):
    __slots__ = ("brokers", "topic", "security_protocol", "sasl_mechanism", "sasl_username", "sasl_password", "format", "group_id", "schema_registry_url")
    BROKERS_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    SECURITY_PROTOCOL_FIELD_NUMBER: _ClassVar[int]
    SASL_MECHANISM_FIELD_NUMBER: _ClassVar[int]
    SASL_USERNAME_FIELD_NUMBER: _ClassVar[int]
    SASL_PASSWORD_FIELD_NUMBER: _ClassVar[int]
    FORMAT_FIELD_NUMBER: _ClassVar[int]
    GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_REGISTRY_URL_FIELD_NUMBER: _ClassVar[int]
    brokers: str
    topic: str
    security_protocol: str
    sasl_mechanism: str
    sasl_username: str
    sasl_password: str
    format: str
    group_id: str
    schema_registry_url: str
    def __init__(self, brokers: _Optional[str] = ..., topic: _Optional[str] = ..., security_protocol: _Optional[str] = ..., sasl_mechanism: _Optional[str] = ..., sasl_username: _Optional[str] = ..., sasl_password: _Optional[str] = ..., format: _Optional[str] = ..., group_id: _Optional[str] = ..., schema_registry_url: _Optional[str] = ...) -> None: ...

class KinesisSource(_message.Message):
    __slots__ = ("stream_arn", "role_arn", "region", "init_position", "format", "endpoint_url")
    STREAM_ARN_FIELD_NUMBER: _ClassVar[int]
    ROLE_ARN_FIELD_NUMBER: _ClassVar[int]
    REGION_FIELD_NUMBER: _ClassVar[int]
    INIT_POSITION_FIELD_NUMBER: _ClassVar[int]
    FORMAT_FIELD_NUMBER: _ClassVar[int]
    ENDPOINT_URL_FIELD_NUMBER: _ClassVar[int]
    stream_arn: str
    role_arn: str
    region: str
    init_position: str
    format: str
    endpoint_url: str
    def __init__(self, stream_arn: _Optional[str] = ..., role_arn: _Optional[str] = ..., region: _Optional[str] = ..., init_position: _Optional[str] = ..., format: _Optional[str] = ..., endpoint_url: _Optional[str] = ...) -> None: ...

class SnowflakeSource(_message.Message):
    __slots__ = ("account", "database", "schema", "warehouse", "role", "table", "user", "password")
    ACCOUNT_FIELD_NUMBER: _ClassVar[int]
    DATABASE_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    WAREHOUSE_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    TABLE_FIELD_NUMBER: _ClassVar[int]
    USER_FIELD_NUMBER: _ClassVar[int]
    PASSWORD_FIELD_NUMBER: _ClassVar[int]
    account: str
    database: str
    schema: str
    warehouse: str
    role: str
    table: str
    user: str
    password: str
    def __init__(self, account: _Optional[str] = ..., database: _Optional[str] = ..., schema: _Optional[str] = ..., warehouse: _Optional[str] = ..., role: _Optional[str] = ..., table: _Optional[str] = ..., user: _Optional[str] = ..., password: _Optional[str] = ...) -> None: ...

class BigQuerySource(_message.Message):
    __slots__ = ("project_id", "dataset_id", "table", "credentials_json")
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    DATASET_ID_FIELD_NUMBER: _ClassVar[int]
    TABLE_FIELD_NUMBER: _ClassVar[int]
    CREDENTIALS_JSON_FIELD_NUMBER: _ClassVar[int]
    project_id: str
    dataset_id: str
    table: str
    credentials_json: str
    def __init__(self, project_id: _Optional[str] = ..., dataset_id: _Optional[str] = ..., table: _Optional[str] = ..., credentials_json: _Optional[str] = ...) -> None: ...

class Source(_message.Message):
    __slots__ = ("dataset", "cursor", "every", "max_lateness", "cdc", "iceberg", "postgres", "s3json", "kafka", "kinesis", "snowflake", "bigquery")
    DATASET_FIELD_NUMBER: _ClassVar[int]
    CURSOR_FIELD_NUMBER: _ClassVar[int]
    EVERY_FIELD_NUMBER: _ClassVar[int]
    MAX_LATENESS_FIELD_NUMBER: _ClassVar[int]
    CDC_FIELD_NUMBER: _ClassVar[int]
    ICEBERG_FIELD_NUMBER: _ClassVar[int]
    POSTGRES_FIELD_NUMBER: _ClassVar[int]
    S3JSON_FIELD_NUMBER: _ClassVar[int]
    KAFKA_FIELD_NUMBER: _ClassVar[int]
    KINESIS_FIELD_NUMBER: _ClassVar[int]
    SNOWFLAKE_FIELD_NUMBER: _ClassVar[int]
    BIGQUERY_FIELD_NUMBER: _ClassVar[int]
    dataset: str
    cursor: str
    every: str
    max_lateness: str
    cdc: str
    iceberg: IcebergSource
    postgres: PostgresSource
    s3json: S3JsonSource
    kafka: KafkaSource
    kinesis: KinesisSource
    snowflake: SnowflakeSource
    bigquery: BigQuerySource
    def __init__(self, dataset: _Optional[str] = ..., cursor: _Optional[str] = ..., every: _Optional[str] = ..., max_lateness: _Optional[str] = ..., cdc: _Optional[str] = ..., iceberg: _Optional[_Union[IcebergSource, _Mapping]] = ..., postgres: _Optional[_Union[PostgresSource, _Mapping]] = ..., s3json: _Optional[_Union[S3JsonSource, _Mapping]] = ..., kafka: _Optional[_Union[KafkaSource, _Mapping]] = ..., kinesis: _Optional[_Union[KinesisSource, _Mapping]] = ..., snowflake: _Optional[_Union[SnowflakeSource, _Mapping]] = ..., bigquery: _Optional[_Union[BigQuerySource, _Mapping]] = ...) -> None: ...
