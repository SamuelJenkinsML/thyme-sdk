from thyme import schema_pb2 as _schema_pb2
from thyme import pycode_pb2 as _pycode_pb2
from thyme import expr_pb2 as _expr_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ExtractorKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    EXTRACTOR_KIND_UNSPECIFIED: _ClassVar[ExtractorKind]
    PY_FUNC: _ClassVar[ExtractorKind]
    LOOKUP: _ClassVar[ExtractorKind]
EXTRACTOR_KIND_UNSPECIFIED: ExtractorKind
PY_FUNC: ExtractorKind
LOOKUP: ExtractorKind

class Featureset(_message.Message):
    __slots__ = ("name", "features", "extractors", "pycode")
    NAME_FIELD_NUMBER: _ClassVar[int]
    FEATURES_FIELD_NUMBER: _ClassVar[int]
    EXTRACTORS_FIELD_NUMBER: _ClassVar[int]
    PYCODE_FIELD_NUMBER: _ClassVar[int]
    name: str
    features: _containers.RepeatedCompositeFieldContainer[Feature]
    extractors: _containers.RepeatedCompositeFieldContainer[Extractor]
    pycode: _pycode_pb2.PyCode
    def __init__(self, name: _Optional[str] = ..., features: _Optional[_Iterable[_Union[Feature, _Mapping]]] = ..., extractors: _Optional[_Iterable[_Union[Extractor, _Mapping]]] = ..., pycode: _Optional[_Union[_pycode_pb2.PyCode, _Mapping]] = ...) -> None: ...

class Feature(_message.Message):
    __slots__ = ("name", "dtype")
    NAME_FIELD_NUMBER: _ClassVar[int]
    DTYPE_FIELD_NUMBER: _ClassVar[int]
    name: str
    dtype: _schema_pb2.DataType
    def __init__(self, name: _Optional[str] = ..., dtype: _Optional[_Union[_schema_pb2.DataType, _Mapping]] = ...) -> None: ...

class Extractor(_message.Message):
    __slots__ = ("name", "inputs", "outputs", "deps", "pycode", "version", "kind", "lookup_info")
    NAME_FIELD_NUMBER: _ClassVar[int]
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    DEPS_FIELD_NUMBER: _ClassVar[int]
    PYCODE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    LOOKUP_INFO_FIELD_NUMBER: _ClassVar[int]
    name: str
    inputs: _containers.RepeatedScalarFieldContainer[str]
    outputs: _containers.RepeatedScalarFieldContainer[str]
    deps: _containers.RepeatedScalarFieldContainer[str]
    pycode: _pycode_pb2.PyCode
    version: int
    kind: ExtractorKind
    lookup_info: LookupInfo
    def __init__(self, name: _Optional[str] = ..., inputs: _Optional[_Iterable[str]] = ..., outputs: _Optional[_Iterable[str]] = ..., deps: _Optional[_Iterable[str]] = ..., pycode: _Optional[_Union[_pycode_pb2.PyCode, _Mapping]] = ..., version: _Optional[int] = ..., kind: _Optional[_Union[ExtractorKind, str]] = ..., lookup_info: _Optional[_Union[LookupInfo, _Mapping]] = ...) -> None: ...

class LookupInfo(_message.Message):
    __slots__ = ("dataset_name", "field_name", "default")
    DATASET_NAME_FIELD_NUMBER: _ClassVar[int]
    FIELD_NAME_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_FIELD_NUMBER: _ClassVar[int]
    dataset_name: str
    field_name: str
    default: _expr_pb2.Literal
    def __init__(self, dataset_name: _Optional[str] = ..., field_name: _Optional[str] = ..., default: _Optional[_Union[_expr_pb2.Literal, _Mapping]] = ...) -> None: ...
