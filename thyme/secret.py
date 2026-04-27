"""Reference to a secret value resolved at engine runtime, not at commit time.

`Secret(env="X")` ships an env-var name to the engine, which calls
`std::env::var("X")` when the connector is built. The literal value never
flows over the wire and never lands in definition-service Postgres.

`Secret(arn=...)` and `Secret(name=...)` round-trip through the SDK and
proto wire format but are stubbed engine-side (return Unimplemented) until
AWS Secrets Manager and named-secret resolvers land.
"""

from typing import Literal


SecretKind = Literal["env", "arn", "name"]


class Secret:
    __slots__ = ("kind", "value")

    def __init__(
        self,
        *,
        env: str | None = None,
        arn: str | None = None,
        name: str | None = None,
    ):
        provided = [(k, v) for k, v in (("env", env), ("arn", arn), ("name", name)) if v is not None]
        if len(provided) != 1:
            raise ValueError(
                "Secret requires exactly one of env=, arn=, or name=; "
                f"got {len(provided)}."
            )
        kind, value = provided[0]
        if not value:
            raise ValueError(f"Secret({kind}=...) value must be non-empty.")
        self.kind: SecretKind = kind
        self.value: str = value

    def to_dict(self) -> dict:
        return {"kind": self.kind, "value": self.value}

    def __repr__(self) -> str:
        return f"Secret({self.kind}={self.value!r})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Secret) and self.kind == other.kind and self.value == other.value

    def __hash__(self) -> int:
        return hash((self.kind, self.value))
