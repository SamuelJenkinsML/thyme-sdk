from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class StateValue(_message.Message):
    __slots__ = ("features", "dimensions", "timestamp", "op", "before", "after", "sketches", "json_features", "shares")
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
    class SketchesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bytes
        def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
    class JsonFeaturesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    class SharesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: AccumulatorValue
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[AccumulatorValue, _Mapping]] = ...) -> None: ...
    FEATURES_FIELD_NUMBER: _ClassVar[int]
    DIMENSIONS_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    OP_FIELD_NUMBER: _ClassVar[int]
    BEFORE_FIELD_NUMBER: _ClassVar[int]
    AFTER_FIELD_NUMBER: _ClassVar[int]
    SKETCHES_FIELD_NUMBER: _ClassVar[int]
    JSON_FEATURES_FIELD_NUMBER: _ClassVar[int]
    SHARES_FIELD_NUMBER: _ClassVar[int]
    features: _containers.ScalarMap[str, float]
    dimensions: _containers.ScalarMap[str, str]
    timestamp: str
    op: str
    before: _containers.ScalarMap[str, float]
    after: _containers.ScalarMap[str, float]
    sketches: _containers.ScalarMap[str, bytes]
    json_features: _containers.ScalarMap[str, str]
    shares: _containers.MessageMap[str, AccumulatorValue]
    def __init__(self, features: _Optional[_Mapping[str, float]] = ..., dimensions: _Optional[_Mapping[str, str]] = ..., timestamp: _Optional[str] = ..., op: _Optional[str] = ..., before: _Optional[_Mapping[str, float]] = ..., after: _Optional[_Mapping[str, float]] = ..., sketches: _Optional[_Mapping[str, bytes]] = ..., json_features: _Optional[_Mapping[str, str]] = ..., shares: _Optional[_Mapping[str, AccumulatorValue]] = ...) -> None: ...

class LatestEntry(_message.Message):
    __slots__ = ("event_ts", "seq", "value_json")
    EVENT_TS_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    VALUE_JSON_FIELD_NUMBER: _ClassVar[int]
    event_ts: int
    seq: int
    value_json: bytes
    def __init__(self, event_ts: _Optional[int] = ..., seq: _Optional[int] = ..., value_json: _Optional[bytes] = ...) -> None: ...

class LatestList(_message.Message):
    __slots__ = ("entries",)
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.RepeatedCompositeFieldContainer[LatestEntry]
    def __init__(self, entries: _Optional[_Iterable[_Union[LatestEntry, _Mapping]]] = ...) -> None: ...

class TDigestCentroid(_message.Message):
    __slots__ = ("mean", "weight")
    MEAN_FIELD_NUMBER: _ClassVar[int]
    WEIGHT_FIELD_NUMBER: _ClassVar[int]
    mean: float
    weight: float
    def __init__(self, mean: _Optional[float] = ..., weight: _Optional[float] = ...) -> None: ...

class TDigestTile(_message.Message):
    __slots__ = ("centroids", "total_weight")
    CENTROIDS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_WEIGHT_FIELD_NUMBER: _ClassVar[int]
    centroids: _containers.RepeatedCompositeFieldContainer[TDigestCentroid]
    total_weight: float
    def __init__(self, centroids: _Optional[_Iterable[_Union[TDigestCentroid, _Mapping]]] = ..., total_weight: _Optional[float] = ...) -> None: ...

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

class WindowContribution(_message.Message):
    __slots__ = ("agg_index", "value")
    AGG_INDEX_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    agg_index: int
    value: float
    def __init__(self, agg_index: _Optional[int] = ..., value: _Optional[float] = ...) -> None: ...

class WindowLogEntry(_message.Message):
    __slots__ = ("contributions",)
    CONTRIBUTIONS_FIELD_NUMBER: _ClassVar[int]
    contributions: _containers.RepeatedCompositeFieldContainer[WindowContribution]
    def __init__(self, contributions: _Optional[_Iterable[_Union[WindowContribution, _Mapping]]] = ...) -> None: ...

class SpecWindow(_message.Message):
    __slots__ = ("agg_index", "window_us", "sum", "count", "seen")
    AGG_INDEX_FIELD_NUMBER: _ClassVar[int]
    WINDOW_US_FIELD_NUMBER: _ClassVar[int]
    SUM_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    SEEN_FIELD_NUMBER: _ClassVar[int]
    agg_index: int
    window_us: int
    sum: float
    count: int
    seen: bool
    def __init__(self, agg_index: _Optional[int] = ..., window_us: _Optional[int] = ..., sum: _Optional[float] = ..., count: _Optional[int] = ..., seen: bool = ...) -> None: ...

class RowAccumulator(_message.Message):
    __slots__ = ("as_of_us", "log_floor_us", "specs", "stale_rows_until_us")
    AS_OF_US_FIELD_NUMBER: _ClassVar[int]
    LOG_FLOOR_US_FIELD_NUMBER: _ClassVar[int]
    SPECS_FIELD_NUMBER: _ClassVar[int]
    STALE_ROWS_UNTIL_US_FIELD_NUMBER: _ClassVar[int]
    as_of_us: int
    log_floor_us: int
    specs: _containers.RepeatedCompositeFieldContainer[SpecWindow]
    stale_rows_until_us: int
    def __init__(self, as_of_us: _Optional[int] = ..., log_floor_us: _Optional[int] = ..., specs: _Optional[_Iterable[_Union[SpecWindow, _Mapping]]] = ..., stale_rows_until_us: _Optional[int] = ...) -> None: ...

class StateWrite(_message.Message):
    __slots__ = ("entity_type", "entity_id", "ts_bytes", "value", "is_raw_key")
    ENTITY_TYPE_FIELD_NUMBER: _ClassVar[int]
    ENTITY_ID_FIELD_NUMBER: _ClassVar[int]
    TS_BYTES_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    IS_RAW_KEY_FIELD_NUMBER: _ClassVar[int]
    entity_type: str
    entity_id: bytes
    ts_bytes: bytes
    value: bytes
    is_raw_key: bool
    def __init__(self, entity_type: _Optional[str] = ..., entity_id: _Optional[bytes] = ..., ts_bytes: _Optional[bytes] = ..., value: _Optional[bytes] = ..., is_raw_key: bool = ...) -> None: ...

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
