from thyme import expr_pb2 as _expr_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Policy(_message.Message):
    __slots__ = ("output", "version", "rules", "fallback")
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    RULES_FIELD_NUMBER: _ClassVar[int]
    FALLBACK_FIELD_NUMBER: _ClassVar[int]
    output: str
    version: int
    rules: _containers.RepeatedCompositeFieldContainer[Rule]
    fallback: str
    def __init__(self, output: _Optional[str] = ..., version: _Optional[int] = ..., rules: _Optional[_Iterable[_Union[Rule, _Mapping]]] = ..., fallback: _Optional[str] = ...) -> None: ...

class Rule(_message.Message):
    __slots__ = ("label", "when")
    LABEL_FIELD_NUMBER: _ClassVar[int]
    WHEN_FIELD_NUMBER: _ClassVar[int]
    label: str
    when: _expr_pb2.Predicate
    def __init__(self, label: _Optional[str] = ..., when: _Optional[_Union[_expr_pb2.Predicate, _Mapping]] = ...) -> None: ...
