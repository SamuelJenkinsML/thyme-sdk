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
    __slots__ = ("host", "port", "database", "table", "user", "password", "schema")
    HOST_FIELD_NUMBER: _ClassVar[int]
    PORT_FIELD_NUMBER: _ClassVar[int]
    DATABASE_FIELD_NUMBER: _ClassVar[int]
    TABLE_FIELD_NUMBER: _ClassVar[int]
    USER_FIELD_NUMBER: _ClassVar[int]
    PASSWORD_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    host: str
    port: int
    database: str
    table: str
    user: str
    password: str
    schema: str
    def __init__(self, host: _Optional[str] = ..., port: _Optional[int] = ..., database: _Optional[str] = ..., table: _Optional[str] = ..., user: _Optional[str] = ..., password: _Optional[str] = ..., schema: _Optional[str] = ...) -> None: ...

class S3JsonSource(_message.Message):
    __slots__ = ("bucket", "prefix", "region")
    BUCKET_FIELD_NUMBER: _ClassVar[int]
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    REGION_FIELD_NUMBER: _ClassVar[int]
    bucket: str
    prefix: str
    region: str
    def __init__(self, bucket: _Optional[str] = ..., prefix: _Optional[str] = ..., region: _Optional[str] = ...) -> None: ...

class Source(_message.Message):
    __slots__ = ("dataset", "cursor", "every", "disorder", "cdc", "iceberg", "postgres", "s3json")
    DATASET_FIELD_NUMBER: _ClassVar[int]
    CURSOR_FIELD_NUMBER: _ClassVar[int]
    EVERY_FIELD_NUMBER: _ClassVar[int]
    DISORDER_FIELD_NUMBER: _ClassVar[int]
    CDC_FIELD_NUMBER: _ClassVar[int]
    ICEBERG_FIELD_NUMBER: _ClassVar[int]
    POSTGRES_FIELD_NUMBER: _ClassVar[int]
    S3JSON_FIELD_NUMBER: _ClassVar[int]
    dataset: str
    cursor: str
    every: str
    disorder: str
    cdc: str
    iceberg: IcebergSource
    postgres: PostgresSource
    s3json: S3JsonSource
    def __init__(self, dataset: _Optional[str] = ..., cursor: _Optional[str] = ..., every: _Optional[str] = ..., disorder: _Optional[str] = ..., cdc: _Optional[str] = ..., iceberg: _Optional[_Union[IcebergSource, _Mapping]] = ..., postgres: _Optional[_Union[PostgresSource, _Mapping]] = ..., s3json: _Optional[_Union[S3JsonSource, _Mapping]] = ...) -> None: ...
