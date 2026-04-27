"""Env-var fallbacks for connector connection settings.

When a connector field is left unset, the connector reads
`THYME_<TYPE>_<FIELD>` (uppercase) from the environment. This keeps demo
files portable across local dev (localhost) and cloud deployments without
the user having to thread a Config object through every connector call.

Per-dataset fields (table, topic, stream_arn, prefix) and per-stream
semantics (init_position, format) are NOT env-defaulted — they describe the
data, not the infrastructure, and silent fallback would mask typos.
"""

import os


_MISSING = object()


def env_default(connector_type: str, field: str, default: object = _MISSING) -> object:
    """Look up THYME_<TYPE>_<FIELD>. Returns `default` if unset.

    Caller decides whether unset-with-no-default is a hard error.
    """
    key = f"THYME_{connector_type.upper()}_{field.upper()}"
    value = os.environ.get(key)
    if value is not None and value != "":
        return value
    return default


def env_default_int(connector_type: str, field: str, default: int) -> int:
    raw = env_default(connector_type, field, default=None)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        raise ValueError(
            f"THYME_{connector_type.upper()}_{field.upper()}={raw!r} is not a valid integer."
        )
