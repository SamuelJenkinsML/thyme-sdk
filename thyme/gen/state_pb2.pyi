from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class StateValue(_message.Message):
    __slots__ = ("features", "dimensions", "timestamp", "op", "before", "after")
    class FeaturesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: float
        def __init__(self, key: _Optional[str] = ..., value: _Optional[float] = ...) -> None: ...
    class DimensionsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    class BeforeEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: float
        def __init__(self, key: _Optional[str] = ..., value: _Optional[float] = ...) -> None: ...
    class AfterEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: float
        def __init__(self, key: _Optional[str] = ..., value: _Optional[float] = ...) -> None: ...
    FEATURES_FIELD_NUMBER: _ClassVar[int]
    DIMENSIONS_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    OP_FIELD_NUMBER: _ClassVar[int]
    BEFORE_FIELD_NUMBER: _ClassVar[int]
    AFTER_FIELD_NUMBER: _ClassVar[int]
    features: _containers.ScalarMap[str, float]
    dimensions: _containers.ScalarMap[str, str]
    timestamp: str
    op: str
    before: _containers.ScalarMap[str, float]
    after: _containers.ScalarMap[str, float]
    def __init__(self, features: _Optional[_Mapping[str, float]] = ..., dimensions: _Optional[_Mapping[str, str]] = ..., timestamp: _Optional[str] = ..., op: _Optional[str] = ..., before: _Optional[_Mapping[str, float]] = ..., after: _Optional[_Mapping[str, float]] = ...) -> None: ...

class AccumulatorValue(_message.Message):
    __slots__ = ("sum", "count")
    SUM_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    sum: float
    count: int
    def __init__(self, sum: _Optional[float] = ..., count: _Optional[int] = ...) -> None: ...

class DeleteQueueEntry(_message.Message):
    __slots__ = ("value", "is_minmax", "seq")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    IS_MINMAX_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    value: float
    is_minmax: bool
    seq: int
    def __init__(self, value: _Optional[float] = ..., is_minmax: bool = ..., seq: _Optional[int] = ...) -> None: ...

class StateWrite(_message.Message):
    __slots__ = ("entity_type", "entity_id", "ts_bytes", "value", "is_raw_key")
    ENTITY_TYPE_FIELD_NUMBER: _ClassVar[int]
    ENTITY_ID_FIELD_NUMBER: _ClassVar[int]
    TS_BYTES_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    IS_RAW_KEY_FIELD_NUMBER: _ClassVar[int]
    entity_type: str
    entity_id: str
    ts_bytes: bytes
    value: bytes
    is_raw_key: bool
    def __init__(self, entity_type: _Optional[str] = ..., entity_id: _Optional[str] = ..., ts_bytes: _Optional[bytes] = ..., value: _Optional[bytes] = ..., is_raw_key: bool = ...) -> None: ...

class ReplayEntry(_message.Message):
    __slots__ = ("job_name", "input_partition", "input_offset", "watermark_secs", "writes", "internal_writes", "internal_deletes")
    JOB_NAME_FIELD_NUMBER: _ClassVar[int]
    INPUT_PARTITION_FIELD_NUMBER: _ClassVar[int]
    INPUT_OFFSET_FIELD_NUMBER: _ClassVar[int]
    WATERMARK_SECS_FIELD_NUMBER: _ClassVar[int]
    WRITES_FIELD_NUMBER: _ClassVar[int]
    INTERNAL_WRITES_FIELD_NUMBER: _ClassVar[int]
    INTERNAL_DELETES_FIELD_NUMBER: _ClassVar[int]
    job_name: str
    input_partition: int
    input_offset: int
    watermark_secs: int
    writes: _containers.RepeatedCompositeFieldContainer[StateWrite]
    internal_writes: _containers.RepeatedCompositeFieldContainer[StateWrite]
    internal_deletes: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, job_name: _Optional[str] = ..., input_partition: _Optional[int] = ..., input_offset: _Optional[int] = ..., watermark_secs: _Optional[int] = ..., writes: _Optional[_Iterable[_Union[StateWrite, _Mapping]]] = ..., internal_writes: _Optional[_Iterable[_Union[StateWrite, _Mapping]]] = ..., internal_deletes: _Optional[_Iterable[bytes]] = ...) -> None: ...
