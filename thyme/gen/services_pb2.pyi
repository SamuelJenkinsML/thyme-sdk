from thyme import dataset_pb2 as _dataset_pb2
from thyme import featureset_pb2 as _featureset_pb2
from thyme import connector_pb2 as _connector_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CommitRequest(_message.Message):
    __slots__ = ("message", "datasets", "pipelines", "featuresets", "sources")
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    DATASETS_FIELD_NUMBER: _ClassVar[int]
    PIPELINES_FIELD_NUMBER: _ClassVar[int]
    FEATURESETS_FIELD_NUMBER: _ClassVar[int]
    SOURCES_FIELD_NUMBER: _ClassVar[int]
    message: str
    datasets: _containers.RepeatedCompositeFieldContainer[_dataset_pb2.Dataset]
    pipelines: _containers.RepeatedCompositeFieldContainer[_dataset_pb2.Pipeline]
    featuresets: _containers.RepeatedCompositeFieldContainer[_featureset_pb2.Featureset]
    sources: _containers.RepeatedCompositeFieldContainer[_connector_pb2.Source]
    def __init__(self, message: _Optional[str] = ..., datasets: _Optional[_Iterable[_Union[_dataset_pb2.Dataset, _Mapping]]] = ..., pipelines: _Optional[_Iterable[_Union[_dataset_pb2.Pipeline, _Mapping]]] = ..., featuresets: _Optional[_Iterable[_Union[_featureset_pb2.Featureset, _Mapping]]] = ..., sources: _Optional[_Iterable[_Union[_connector_pb2.Source, _Mapping]]] = ...) -> None: ...

class CommitResponse(_message.Message):
    __slots__ = ("commit_id", "datasets_count", "pipelines_count", "featuresets_count", "jobs_created", "topics_created")
    COMMIT_ID_FIELD_NUMBER: _ClassVar[int]
    DATASETS_COUNT_FIELD_NUMBER: _ClassVar[int]
    PIPELINES_COUNT_FIELD_NUMBER: _ClassVar[int]
    FEATURESETS_COUNT_FIELD_NUMBER: _ClassVar[int]
    JOBS_CREATED_FIELD_NUMBER: _ClassVar[int]
    TOPICS_CREATED_FIELD_NUMBER: _ClassVar[int]
    commit_id: str
    datasets_count: int
    pipelines_count: int
    featuresets_count: int
    jobs_created: int
    topics_created: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, commit_id: _Optional[str] = ..., datasets_count: _Optional[int] = ..., pipelines_count: _Optional[int] = ..., featuresets_count: _Optional[int] = ..., jobs_created: _Optional[int] = ..., topics_created: _Optional[_Iterable[str]] = ...) -> None: ...

class QueryRequest(_message.Message):
    __slots__ = ("entity_type", "entity_id", "features", "timestamp")
    ENTITY_TYPE_FIELD_NUMBER: _ClassVar[int]
    ENTITY_ID_FIELD_NUMBER: _ClassVar[int]
    FEATURES_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    entity_type: str
    entity_id: str
    features: _containers.RepeatedScalarFieldContainer[str]
    timestamp: str
    def __init__(self, entity_type: _Optional[str] = ..., entity_id: _Optional[str] = ..., features: _Optional[_Iterable[str]] = ..., timestamp: _Optional[str] = ...) -> None: ...

class QueryResponse(_message.Message):
    __slots__ = ("entity_type", "entity_id", "features", "mode")
    class FeaturesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ENTITY_TYPE_FIELD_NUMBER: _ClassVar[int]
    ENTITY_ID_FIELD_NUMBER: _ClassVar[int]
    FEATURES_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    entity_type: str
    entity_id: str
    features: _containers.ScalarMap[str, str]
    mode: str
    def __init__(self, entity_type: _Optional[str] = ..., entity_id: _Optional[str] = ..., features: _Optional[_Mapping[str, str]] = ..., mode: _Optional[str] = ...) -> None: ...

class QueryRun(_message.Message):
    __slots__ = ("id", "featureset", "entity_ids", "requested_timestamp", "kind", "row_count", "hit_count", "latency_ms", "api_key_fingerprint", "error", "created_at")
    ID_FIELD_NUMBER: _ClassVar[int]
    FEATURESET_FIELD_NUMBER: _ClassVar[int]
    ENTITY_IDS_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    ROW_COUNT_FIELD_NUMBER: _ClassVar[int]
    HIT_COUNT_FIELD_NUMBER: _ClassVar[int]
    LATENCY_MS_FIELD_NUMBER: _ClassVar[int]
    API_KEY_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    featureset: str
    entity_ids: _containers.RepeatedScalarFieldContainer[str]
    requested_timestamp: str
    kind: str
    row_count: int
    hit_count: int
    latency_ms: int
    api_key_fingerprint: str
    error: str
    created_at: str
    def __init__(self, id: _Optional[str] = ..., featureset: _Optional[str] = ..., entity_ids: _Optional[_Iterable[str]] = ..., requested_timestamp: _Optional[str] = ..., kind: _Optional[str] = ..., row_count: _Optional[int] = ..., hit_count: _Optional[int] = ..., latency_ms: _Optional[int] = ..., api_key_fingerprint: _Optional[str] = ..., error: _Optional[str] = ..., created_at: _Optional[str] = ...) -> None: ...

class ListQueryRunsResponse(_message.Message):
    __slots__ = ("runs", "next_cursor")
    RUNS_FIELD_NUMBER: _ClassVar[int]
    NEXT_CURSOR_FIELD_NUMBER: _ClassVar[int]
    runs: _containers.RepeatedCompositeFieldContainer[QueryRun]
    next_cursor: str
    def __init__(self, runs: _Optional[_Iterable[_Union[QueryRun, _Mapping]]] = ..., next_cursor: _Optional[str] = ...) -> None: ...
