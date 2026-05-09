"""Catalog metadata attached to `@featureset`, `@dataset`, and `@source`
decorated classes via `__thyme_metadata__`. See feature-catalog design §6.1
for the round-trip contract."""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from typing import Any

_KNOWN_FIELDS = (
    "description",
    "owner",
    "tags",
    "project",
    "deprecated",
    "deprecation_reason",
    "replacement",
)


@dataclass
class EntityMetadata:
    description: str | None = None
    owner: str | None = None
    tags: dict[str, str] = field(default_factory=dict)
    project: str | None = None
    deprecated: bool = False
    deprecation_reason: str | None = None
    replacement: str | None = None

    def __post_init__(self) -> None:
        # Accept `list[str]` for ergonomic parity with Tecton/Feast; normalize
        # to `dict[str, str]` so downstream code only sees one shape.
        if isinstance(self.tags, list):
            self.tags = {tag: "" for tag in self.tags}


def _build_metadata(decorator_name: str, kwargs: dict[str, Any]) -> EntityMetadata:
    """Pop known metadata kwargs out of `kwargs` and return an `EntityMetadata`.
    Unknown kwargs trigger `FutureWarning` (forward-compat: older SDKs should
    not break when a newer commit author uses a kwarg this SDK doesn't know)."""
    known = {k: kwargs.pop(k) for k in _KNOWN_FIELDS if k in kwargs}
    if kwargs:
        warnings.warn(
            f"@{decorator_name} ignored unknown kwargs: {sorted(kwargs)}",
            FutureWarning,
            stacklevel=3,
        )
    return EntityMetadata(**known)
