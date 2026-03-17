from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DataType(_message.Message):
    __slots__ = ("bool_type", "int_type", "float_type", "string_type", "timestamp_type", "optional_type", "list_type", "map_type")
    BOOL_TYPE_FIELD_NUMBER: _ClassVar[int]
    INT_TYPE_FIELD_NUMBER: _ClassVar[int]
    FLOAT_TYPE_FIELD_NUMBER: _ClassVar[int]
    STRING_TYPE_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_TYPE_FIELD_NUMBER: _ClassVar[int]
    OPTIONAL_TYPE_FIELD_NUMBER: _ClassVar[int]
    LIST_TYPE_FIELD_NUMBER: _ClassVar[int]
    MAP_TYPE_FIELD_NUMBER: _ClassVar[int]
    bool_type: BoolType
    int_type: IntType
    float_type: FloatType
    string_type: StringType
    timestamp_type: TimestampType
    optional_type: OptionalType
    list_type: ListType
    map_type: MapType
    def __init__(self, bool_type: _Optional[_Union[BoolType, _Mapping]] = ..., int_type: _Optional[_Union[IntType, _Mapping]] = ..., float_type: _Optional[_Union[FloatType, _Mapping]] = ..., string_type: _Optional[_Union[StringType, _Mapping]] = ..., timestamp_type: _Optional[_Union[TimestampType, _Mapping]] = ..., optional_type: _Optional[_Union[OptionalType, _Mapping]] = ..., list_type: _Optional[_Union[ListType, _Mapping]] = ..., map_type: _Optional[_Union[MapType, _Mapping]] = ...) -> None: ...

class BoolType(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class IntType(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class FloatType(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class StringType(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class TimestampType(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class OptionalType(_message.Message):
    __slots__ = ("inner",)
    INNER_FIELD_NUMBER: _ClassVar[int]
    inner: DataType
    def __init__(self, inner: _Optional[_Union[DataType, _Mapping]] = ...) -> None: ...

class ListType(_message.Message):
    __slots__ = ("element",)
    ELEMENT_FIELD_NUMBER: _ClassVar[int]
    element: DataType
    def __init__(self, element: _Optional[_Union[DataType, _Mapping]] = ...) -> None: ...

class MapType(_message.Message):
    __slots__ = ("key", "value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: DataType
    value: DataType
    def __init__(self, key: _Optional[_Union[DataType, _Mapping]] = ..., value: _Optional[_Union[DataType, _Mapping]] = ...) -> None: ...

class Field(_message.Message):
    __slots__ = ("name", "dtype", "is_key", "is_timestamp")
    NAME_FIELD_NUMBER: _ClassVar[int]
    DTYPE_FIELD_NUMBER: _ClassVar[int]
    IS_KEY_FIELD_NUMBER: _ClassVar[int]
    IS_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    name: str
    dtype: DataType
    is_key: bool
    is_timestamp: bool
    def __init__(self, name: _Optional[str] = ..., dtype: _Optional[_Union[DataType, _Mapping]] = ..., is_key: bool = ..., is_timestamp: bool = ...) -> None: ...

class DSSchema(_message.Message):
    __slots__ = ("fields",)
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    fields: _containers.RepeatedCompositeFieldContainer[Field]
    def __init__(self, fields: _Optional[_Iterable[_Union[Field, _Mapping]]] = ...) -> None: ...
