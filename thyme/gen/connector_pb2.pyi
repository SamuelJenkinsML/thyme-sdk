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

class Source(_message.Message):
    __slots__ = ("dataset", "cursor", "every", "disorder", "cdc", "iceberg")
    DATASET_FIELD_NUMBER: _ClassVar[int]
    CURSOR_FIELD_NUMBER: _ClassVar[int]
    EVERY_FIELD_NUMBER: _ClassVar[int]
    DISORDER_FIELD_NUMBER: _ClassVar[int]
    CDC_FIELD_NUMBER: _ClassVar[int]
    ICEBERG_FIELD_NUMBER: _ClassVar[int]
    dataset: str
    cursor: str
    every: str
    disorder: str
    cdc: str
    iceberg: IcebergSource
    def __init__(self, dataset: _Optional[str] = ..., cursor: _Optional[str] = ..., every: _Optional[str] = ..., disorder: _Optional[str] = ..., cdc: _Optional[str] = ..., iceberg: _Optional[_Union[IcebergSource, _Mapping]] = ...) -> None: ...
