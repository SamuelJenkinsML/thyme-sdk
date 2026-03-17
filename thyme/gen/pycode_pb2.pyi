from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class PyCode(_message.Message):
    __slots__ = ("entry_point", "source_code", "generated_code", "imports")
    ENTRY_POINT_FIELD_NUMBER: _ClassVar[int]
    SOURCE_CODE_FIELD_NUMBER: _ClassVar[int]
    GENERATED_CODE_FIELD_NUMBER: _ClassVar[int]
    IMPORTS_FIELD_NUMBER: _ClassVar[int]
    entry_point: str
    source_code: str
    generated_code: str
    imports: str
    def __init__(self, entry_point: _Optional[str] = ..., source_code: _Optional[str] = ..., generated_code: _Optional[str] = ..., imports: _Optional[str] = ...) -> None: ...
