from thyme import connector_pb2 as _connector_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Provider(_message.Message):
    __slots__ = ("name", "format", "base_url", "api_key", "rate_limit_per_s", "samples", "input_price_per_mtok", "output_price_per_mtok")
    class Format(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        FORMAT_UNSPECIFIED: _ClassVar[Provider.Format]
        SYSTEMONE: _ClassVar[Provider.Format]
    FORMAT_UNSPECIFIED: Provider.Format
    SYSTEMONE: Provider.Format
    NAME_FIELD_NUMBER: _ClassVar[int]
    FORMAT_FIELD_NUMBER: _ClassVar[int]
    BASE_URL_FIELD_NUMBER: _ClassVar[int]
    API_KEY_FIELD_NUMBER: _ClassVar[int]
    RATE_LIMIT_PER_S_FIELD_NUMBER: _ClassVar[int]
    SAMPLES_FIELD_NUMBER: _ClassVar[int]
    INPUT_PRICE_PER_MTOK_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_PRICE_PER_MTOK_FIELD_NUMBER: _ClassVar[int]
    name: str
    format: Provider.Format
    base_url: str
    api_key: _connector_pb2.SecretRef
    rate_limit_per_s: float
    samples: int
    input_price_per_mtok: float
    output_price_per_mtok: float
    def __init__(self, name: _Optional[str] = ..., format: _Optional[_Union[Provider.Format, str]] = ..., base_url: _Optional[str] = ..., api_key: _Optional[_Union[_connector_pb2.SecretRef, _Mapping]] = ..., rate_limit_per_s: _Optional[float] = ..., samples: _Optional[int] = ..., input_price_per_mtok: _Optional[float] = ..., output_price_per_mtok: _Optional[float] = ...) -> None: ...
