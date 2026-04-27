"""Thyme platform configuration — connect to the control plane.

`Config` covers SDK ↔ Thyme platform settings only: the definition-service
URL, the query-server URL, the frontend URL, and the API key. Per-connector
infrastructure settings (Postgres host, Kafka brokers, Kinesis region, etc.)
live on the connector classes themselves and read `THYME_<TYPE>_<FIELD>`
environment variables when not passed explicitly. See `thyme.connectors`
and `thyme.env_defaults`.

Loads settings from (in priority order):

1. Explicit constructor arguments
2. Environment variables (THYME_API_URL, THYME_API_KEY, ...)
3. Config file (.thyme.yaml in cwd, then ~/.thyme.yaml)
4. Stored credentials from ``thyme login`` (~/.thyme/credentials)
5. Built-in defaults (localhost dev setup)

Usage::

    from thyme import Config

    config = Config.load()
    headers = config.auth_headers()
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


CREDENTIALS_DIR = Path.home() / ".thyme"
CREDENTIALS_FILE = CREDENTIALS_DIR / "credentials"


@dataclass
class Config:
    """Thyme platform configuration."""

    api_url: str = "http://localhost:8080/api/v1/commit"
    api_base: str = "http://localhost:8080"
    query_url: str = "http://localhost:8081"
    frontend_url: str = ""
    api_key: str = ""

    @classmethod
    def load(cls, path: str | Path | None = None) -> "Config":
        """Load config from file + environment variables + stored credentials.

        Search order for config file:
        1. Explicit *path* argument
        2. ``.thyme.yaml`` in current directory
        3. ``~/.thyme.yaml``

        Environment variables override file values.
        Stored credentials (from ``thyme login``) provide api_key and api_base
        as a fallback when not set elsewhere.
        """
        file_data: dict[str, Any] = {}
        if path is not None:
            file_data = _load_yaml(Path(path))
        else:
            for candidate in [Path(".thyme.yaml"), Path.home() / ".thyme.yaml"]:
                if candidate.exists():
                    file_data = _load_yaml(candidate)
                    break

        ds = file_data.get("definition_service", {})
        qs = file_data.get("query_server", {})
        api_base = file_data.get("api_base") or ds.get("url") or cls.api_base
        query_url = file_data.get("query_url") or qs.get("url") or cls.query_url
        frontend_url = file_data.get("frontend_url") or cls.frontend_url
        api_url = file_data.get("api_url", None)
        if api_url is None:
            api_url = api_base + "/api/v1/commit" if api_base != cls.api_base else cls.api_url

        _file_set_api_base = "api_base" in file_data or "url" in ds
        _file_set_query_url = "query_url" in file_data or "url" in qs
        _file_set_api_url = "api_url" in file_data
        _file_set_api_key = "api_key" in file_data

        config = cls(
            api_url=api_url,
            api_base=api_base,
            query_url=query_url,
            frontend_url=frontend_url,
            api_key=file_data.get("api_key", cls.api_key),
        )

        creds = load_credentials()
        if creds:
            if not config.api_key and not _file_set_api_key:
                config.api_key = creds.get("api_key", "")
            if not _file_set_api_base and creds.get("api_base"):
                config.api_base = creds["api_base"]
            if not _file_set_api_url and not _file_set_api_base and creds.get("api_base"):
                config.api_url = creds["api_base"] + "/api/v1/commit"
            if not _file_set_query_url and creds.get("query_url"):
                config.query_url = creds["query_url"]

        _apply_env_overrides(config)

        return config

    def auth_headers(self) -> dict[str, str]:
        """Return HTTP headers for authenticated requests."""
        if self.api_key:
            return {"Authorization": f"Bearer {self.api_key}"}
        return {}

    def query_run_url(self, run_id: str) -> str | None:
        """Build a URL to the frontend's query-run detail page.

        Returns ``None`` when ``frontend_url`` is not configured so callers
        can distinguish "no UI available" from a bad URL.
        """
        if not self.frontend_url or not run_id:
            return None
        base = self.frontend_url.rstrip("/")
        return f"{base}/query-runs/{run_id}"


# ---------------------------------------------------------------------------
# Credential storage
# ---------------------------------------------------------------------------


def save_credentials(api_key: str, api_base: str, query_url: str = "") -> Path:
    """Store credentials to ~/.thyme/credentials. Returns the file path."""
    CREDENTIALS_DIR.mkdir(parents=True, exist_ok=True)
    data = {"api_key": api_key, "api_base": api_base}
    if query_url:
        data["query_url"] = query_url
    CREDENTIALS_FILE.write_text(json.dumps(data, indent=2))
    CREDENTIALS_FILE.chmod(0o600)
    return CREDENTIALS_FILE


def load_credentials() -> dict[str, str] | None:
    """Load stored credentials. Returns None if no credentials file exists."""
    if not CREDENTIALS_FILE.exists():
        return None
    try:
        return json.loads(CREDENTIALS_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def clear_credentials() -> bool:
    """Remove stored credentials. Returns True if a file was deleted."""
    if CREDENTIALS_FILE.exists():
        CREDENTIALS_FILE.unlink()
        return True
    return False


# ---------------------------------------------------------------------------
# Environment variable overrides (platform only)
# ---------------------------------------------------------------------------


def _apply_env_overrides(config: Config) -> None:
    """Apply environment variable overrides for platform settings.

    Connector connection settings have their own env-var fallback inside
    `thyme.env_defaults` — they don't go through Config.
    """
    env = {
        "THYME_API_URL": "api_url",
        "THYME_API_BASE": "api_base",
        "THYME_QUERY_URL": "query_url",
        "THYME_FRONTEND_URL": "frontend_url",
        "THYME_API_KEY": "api_key",
    }
    for key, attr in env.items():
        val = os.environ.get(key)
        if val is not None:
            setattr(config, attr, val)


# ---------------------------------------------------------------------------
# YAML loading
# ---------------------------------------------------------------------------


def _load_yaml(path: Path) -> dict[str, Any]:
    """Load a YAML config file. Returns empty dict on failure."""
    try:
        import yaml
        with open(path) as f:
            return yaml.safe_load(f) or {}
    except ImportError:
        return _parse_simple_yaml(path)
    except Exception:
        return {}


def _parse_simple_yaml(path: Path) -> dict[str, Any]:
    """Minimal YAML-like parser for flat and one-level nested configs.

    Handles::

        key: value
        section:
          key: value
    """
    result: dict[str, Any] = {}
    current_section: str | None = None

    for line in path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if ":" not in stripped:
            continue

        indent = len(line) - len(line.lstrip())
        key, _, value = stripped.partition(":")
        key = key.strip()
        value = value.strip()

        if indent == 0:
            if value:
                result[key] = _coerce(value)
            else:
                result[key] = {}
                current_section = key
        elif indent > 0 and current_section and isinstance(result.get(current_section), dict):
            result[current_section][key] = _coerce(value)

    return result


def _coerce(value: str) -> str | int | float | bool:
    """Coerce a YAML string value to its likely Python type."""
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
        return value[1:-1]
    if value.lower() in ("true", "yes"):
        return True
    if value.lower() in ("false", "no"):
        return False
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value
