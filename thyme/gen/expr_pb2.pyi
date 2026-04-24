from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CompareOp(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    COMPARE_OP_UNSPECIFIED: _ClassVar[CompareOp]
    CMP_EQ: _ClassVar[CompareOp]
    CMP_NEQ: _ClassVar[CompareOp]
    CMP_LT: _ClassVar[CompareOp]
    CMP_LTE: _ClassVar[CompareOp]
    CMP_GT: _ClassVar[CompareOp]
    CMP_GTE: _ClassVar[CompareOp]

class ArithOp(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ARITH_OP_UNSPECIFIED: _ClassVar[ArithOp]
    ARITH_ADD: _ClassVar[ArithOp]
    ARITH_SUB: _ClassVar[ArithOp]
    ARITH_MUL: _ClassVar[ArithOp]
    ARITH_DIV: _ClassVar[ArithOp]

class LogicKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    LOGIC_KIND_UNSPECIFIED: _ClassVar[LogicKind]
    LOGIC_AND: _ClassVar[LogicKind]
    LOGIC_OR: _ClassVar[LogicKind]
    LOGIC_NOT: _ClassVar[LogicKind]
COMPARE_OP_UNSPECIFIED: CompareOp
CMP_EQ: CompareOp
CMP_NEQ: CompareOp
CMP_LT: CompareOp
CMP_LTE: CompareOp
CMP_GT: CompareOp
CMP_GTE: CompareOp
ARITH_OP_UNSPECIFIED: ArithOp
ARITH_ADD: ArithOp
ARITH_SUB: ArithOp
ARITH_MUL: ArithOp
ARITH_DIV: ArithOp
LOGIC_KIND_UNSPECIFIED: LogicKind
LOGIC_AND: LogicKind
LOGIC_OR: LogicKind
LOGIC_NOT: LogicKind

class Predicate(_message.Message):
    __slots__ = ("compare", "is_null", "logic")
    COMPARE_FIELD_NUMBER: _ClassVar[int]
    IS_NULL_FIELD_NUMBER: _ClassVar[int]
    LOGIC_FIELD_NUMBER: _ClassVar[int]
    compare: Compare
    is_null: IsNull
    logic: LogicOp
    def __init__(self, compare: _Optional[_Union[Compare, _Mapping]] = ..., is_null: _Optional[_Union[IsNull, _Mapping]] = ..., logic: _Optional[_Union[LogicOp, _Mapping]] = ...) -> None: ...

class Compare(_message.Message):
    __slots__ = ("lhs", "op", "rhs")
    LHS_FIELD_NUMBER: _ClassVar[int]
    OP_FIELD_NUMBER: _ClassVar[int]
    RHS_FIELD_NUMBER: _ClassVar[int]
    lhs: Derivation
    op: CompareOp
    rhs: Derivation
    def __init__(self, lhs: _Optional[_Union[Derivation, _Mapping]] = ..., op: _Optional[_Union[CompareOp, str]] = ..., rhs: _Optional[_Union[Derivation, _Mapping]] = ...) -> None: ...

class IsNull(_message.Message):
    __slots__ = ("column",)
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    column: str
    def __init__(self, column: _Optional[str] = ...) -> None: ...

class LogicOp(_message.Message):
    __slots__ = ("kind", "operands")
    KIND_FIELD_NUMBER: _ClassVar[int]
    OPERANDS_FIELD_NUMBER: _ClassVar[int]
    kind: LogicKind
    operands: _containers.RepeatedCompositeFieldContainer[Predicate]
    def __init__(self, kind: _Optional[_Union[LogicKind, str]] = ..., operands: _Optional[_Iterable[_Union[Predicate, _Mapping]]] = ...) -> None: ...

class Derivation(_message.Message):
    __slots__ = ("column_ref", "literal", "arith", "fill_null")
    COLUMN_REF_FIELD_NUMBER: _ClassVar[int]
    LITERAL_FIELD_NUMBER: _ClassVar[int]
    ARITH_FIELD_NUMBER: _ClassVar[int]
    FILL_NULL_FIELD_NUMBER: _ClassVar[int]
    column_ref: str
    literal: Literal
    arith: Arith
    fill_null: FillNull
    def __init__(self, column_ref: _Optional[str] = ..., literal: _Optional[_Union[Literal, _Mapping]] = ..., arith: _Optional[_Union[Arith, _Mapping]] = ..., fill_null: _Optional[_Union[FillNull, _Mapping]] = ...) -> None: ...

class Arith(_message.Message):
    __slots__ = ("op", "lhs", "rhs")
    OP_FIELD_NUMBER: _ClassVar[int]
    LHS_FIELD_NUMBER: _ClassVar[int]
    RHS_FIELD_NUMBER: _ClassVar[int]
    op: ArithOp
    lhs: Derivation
    rhs: Derivation
    def __init__(self, op: _Optional[_Union[ArithOp, str]] = ..., lhs: _Optional[_Union[Derivation, _Mapping]] = ..., rhs: _Optional[_Union[Derivation, _Mapping]] = ...) -> None: ...

class FillNull(_message.Message):
    __slots__ = ("value", "default")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_FIELD_NUMBER: _ClassVar[int]
    value: Derivation
    default: Literal
    def __init__(self, value: _Optional[_Union[Derivation, _Mapping]] = ..., default: _Optional[_Union[Literal, _Mapping]] = ...) -> None: ...

class Literal(_message.Message):
    __slots__ = ("int_value", "float_value", "string_value", "bool_value")
    INT_VALUE_FIELD_NUMBER: _ClassVar[int]
    FLOAT_VALUE_FIELD_NUMBER: _ClassVar[int]
    STRING_VALUE_FIELD_NUMBER: _ClassVar[int]
    BOOL_VALUE_FIELD_NUMBER: _ClassVar[int]
    int_value: int
    float_value: float
    string_value: str
    bool_value: bool
    def __init__(self, int_value: _Optional[int] = ..., float_value: _Optional[float] = ..., string_value: _Optional[str] = ..., bool_value: bool = ...) -> None: ...
