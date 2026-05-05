"""Base contract for source connectors.

Implementers declare `connector_type` and `is_streaming` as ClassVars and
emit a wire payload from `to_dict()` shaped:

    {"connector_type": str, "config": dict}

`is_streaming=True` opts the connector out of poll-based fields (`cursor`,
`every`) at @source-decoration time — the engine consumes streaming sources
continuously, so those knobs don't apply.
"""

from typing import ClassVar, Protocol, runtime_checkable


@runtime_checkable
class SourceConnector(Protocol):
    connector_type: ClassVar[str]
    is_streaming: ClassVar[bool]

    def to_dict(self) -> dict: ...
