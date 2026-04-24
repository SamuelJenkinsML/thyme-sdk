"""Serialise ``expr.proto`` messages to the JSON shape the Rust engine expects.

The engine decodes filter/assign operator JSON into ``thyme_proto::Predicate``
and ``thyme_proto::Derivation`` via ``serde_json::from_value``. Prost auto-
derives serde with externally-tagged oneofs and integer enums, so a bare
``google.protobuf.json_format.MessageToDict`` output will not deserialise.

This module walks the protobuf descriptor to emit a compatible shape: oneofs
become ``{<oneof>: {<PascalVariant>: <value>}}`` and enums become ints.
"""

from __future__ import annotations

from typing import Any

from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.message import Message


def proto_to_wire(msg: Message) -> dict[str, Any]:
    """Serialise ``msg`` to a Rust-prost-serde-compatible dict."""
    out: dict[str, Any] = {}
    for field, value in msg.ListFields():
        if field.containing_oneof is not None:
            oneof_name = field.containing_oneof.name  # usually "kind"
            variant = _snake_to_pascal(field.name)
            out[oneof_name] = {variant: _field_value(field, value)}
        else:
            out[field.name] = _field_value(field, value)
    return out


def _field_value(field: FieldDescriptor, value: Any) -> Any:
    repeated = field.is_repeated
    if field.type == FieldDescriptor.TYPE_MESSAGE:
        if repeated:
            return [proto_to_wire(v) for v in value]
        return proto_to_wire(value)
    if field.type == FieldDescriptor.TYPE_ENUM:
        return int(value)
    if repeated:
        return list(value)
    return value


def _snake_to_pascal(name: str) -> str:
    return "".join(part.capitalize() for part in name.split("_") if part)
