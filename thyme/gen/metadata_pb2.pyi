from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class EntityMetadata(_message.Message):
    __slots__ = ("description", "owner", "tags", "project", "deprecated", "deprecation_reason", "replacement")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    OWNER_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    DEPRECATED_FIELD_NUMBER: _ClassVar[int]
    DEPRECATION_REASON_FIELD_NUMBER: _ClassVar[int]
    REPLACEMENT_FIELD_NUMBER: _ClassVar[int]
    description: str
    owner: str
    tags: _containers.ScalarMap[str, str]
    project: str
    deprecated: bool
    deprecation_reason: str
    replacement: str
    def __init__(self, description: _Optional[str] = ..., owner: _Optional[str] = ..., tags: _Optional[_Mapping[str, str]] = ..., project: _Optional[str] = ..., deprecated: bool = ..., deprecation_reason: _Optional[str] = ..., replacement: _Optional[str] = ...) -> None: ...
