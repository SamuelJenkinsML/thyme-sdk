from thyme import schema_pb2 as _schema_pb2
from thyme import pycode_pb2 as _pycode_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Dataset(_message.Message):
    __slots__ = ("name", "version", "schema", "indexed", "pycode", "expectations")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    INDEXED_FIELD_NUMBER: _ClassVar[int]
    PYCODE_FIELD_NUMBER: _ClassVar[int]
    EXPECTATIONS_FIELD_NUMBER: _ClassVar[int]
    name: str
    version: int
    schema: _schema_pb2.DSSchema
    indexed: bool
    pycode: _pycode_pb2.PyCode
    expectations: _containers.RepeatedCompositeFieldContainer[Expectation]
    def __init__(self, name: _Optional[str] = ..., version: _Optional[int] = ..., schema: _Optional[_Union[_schema_pb2.DSSchema, _Mapping]] = ..., indexed: bool = ..., pycode: _Optional[_Union[_pycode_pb2.PyCode, _Mapping]] = ..., expectations: _Optional[_Iterable[_Union[Expectation, _Mapping]]] = ...) -> None: ...

class Expectation(_message.Message):
    __slots__ = ("type", "column", "mostly", "min_value", "max_value", "values", "type_name")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    MOSTLY_FIELD_NUMBER: _ClassVar[int]
    MIN_VALUE_FIELD_NUMBER: _ClassVar[int]
    MAX_VALUE_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    TYPE_NAME_FIELD_NUMBER: _ClassVar[int]
    type: str
    column: str
    mostly: float
    min_value: float
    max_value: float
    values: _containers.RepeatedScalarFieldContainer[str]
    type_name: str
    def __init__(self, type: _Optional[str] = ..., column: _Optional[str] = ..., mostly: _Optional[float] = ..., min_value: _Optional[float] = ..., max_value: _Optional[float] = ..., values: _Optional[_Iterable[str]] = ..., type_name: _Optional[str] = ...) -> None: ...

class Pipeline(_message.Message):
    __slots__ = ("name", "version", "input_datasets", "output_dataset", "operators", "pycode")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    INPUT_DATASETS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_DATASET_FIELD_NUMBER: _ClassVar[int]
    OPERATORS_FIELD_NUMBER: _ClassVar[int]
    PYCODE_FIELD_NUMBER: _ClassVar[int]
    name: str
    version: int
    input_datasets: _containers.RepeatedScalarFieldContainer[str]
    output_dataset: str
    operators: _containers.RepeatedCompositeFieldContainer[Operator]
    pycode: _pycode_pb2.PyCode
    def __init__(self, name: _Optional[str] = ..., version: _Optional[int] = ..., input_datasets: _Optional[_Iterable[str]] = ..., output_dataset: _Optional[str] = ..., operators: _Optional[_Iterable[_Union[Operator, _Mapping]]] = ..., pycode: _Optional[_Union[_pycode_pb2.PyCode, _Mapping]] = ...) -> None: ...

class Operator(_message.Message):
    __slots__ = ("id", "aggregate", "filter", "transform", "group_by")
    ID_FIELD_NUMBER: _ClassVar[int]
    AGGREGATE_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    TRANSFORM_FIELD_NUMBER: _ClassVar[int]
    GROUP_BY_FIELD_NUMBER: _ClassVar[int]
    id: str
    aggregate: Aggregate
    filter: Filter
    transform: Transform
    group_by: GroupBy
    def __init__(self, id: _Optional[str] = ..., aggregate: _Optional[_Union[Aggregate, _Mapping]] = ..., filter: _Optional[_Union[Filter, _Mapping]] = ..., transform: _Optional[_Union[Transform, _Mapping]] = ..., group_by: _Optional[_Union[GroupBy, _Mapping]] = ...) -> None: ...

class Aggregate(_message.Message):
    __slots__ = ("operand_id", "keys", "specs")
    OPERAND_ID_FIELD_NUMBER: _ClassVar[int]
    KEYS_FIELD_NUMBER: _ClassVar[int]
    SPECS_FIELD_NUMBER: _ClassVar[int]
    operand_id: str
    keys: _containers.RepeatedScalarFieldContainer[str]
    specs: _containers.RepeatedCompositeFieldContainer[AggSpec]
    def __init__(self, operand_id: _Optional[str] = ..., keys: _Optional[_Iterable[str]] = ..., specs: _Optional[_Iterable[_Union[AggSpec, _Mapping]]] = ...) -> None: ...

class AggSpec(_message.Message):
    __slots__ = ("agg_type", "field", "window", "output_field")
    AGG_TYPE_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    WINDOW_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FIELD_FIELD_NUMBER: _ClassVar[int]
    agg_type: str
    field: str
    window: str
    output_field: str
    def __init__(self, agg_type: _Optional[str] = ..., field: _Optional[str] = ..., window: _Optional[str] = ..., output_field: _Optional[str] = ...) -> None: ...

class Filter(_message.Message):
    __slots__ = ("operand_id", "pycode")
    OPERAND_ID_FIELD_NUMBER: _ClassVar[int]
    PYCODE_FIELD_NUMBER: _ClassVar[int]
    operand_id: str
    pycode: _pycode_pb2.PyCode
    def __init__(self, operand_id: _Optional[str] = ..., pycode: _Optional[_Union[_pycode_pb2.PyCode, _Mapping]] = ...) -> None: ...

class Transform(_message.Message):
    __slots__ = ("operand_id", "pycode")
    OPERAND_ID_FIELD_NUMBER: _ClassVar[int]
    PYCODE_FIELD_NUMBER: _ClassVar[int]
    operand_id: str
    pycode: _pycode_pb2.PyCode
    def __init__(self, operand_id: _Optional[str] = ..., pycode: _Optional[_Union[_pycode_pb2.PyCode, _Mapping]] = ...) -> None: ...

class GroupBy(_message.Message):
    __slots__ = ("operand_id", "keys")
    OPERAND_ID_FIELD_NUMBER: _ClassVar[int]
    KEYS_FIELD_NUMBER: _ClassVar[int]
    operand_id: str
    keys: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, operand_id: _Optional[str] = ..., keys: _Optional[_Iterable[str]] = ...) -> None: ...
