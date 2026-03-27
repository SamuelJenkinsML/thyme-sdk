"""Thyme configuration — connect to your infrastructure consistently.

Loads settings from (in priority order):
1. Explicit constructor arguments
2. Environment variables (THYME_API_URL, THYME_POSTGRES_HOST, etc.)
3. Config file (.thyme.yaml in cwd, then ~/.thyme.yaml)
4. Stored credentials from ``thyme login`` (~/.thyme/credentials)
5. Built-in defaults (localhost dev setup)

Usage::

    from thyme import Config

    config = Config.load()
    source = config.postgres_source(table="orders")
"""
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


CREDENTIALS_DIR = Path.home() / ".thyme"
CREDENTIALS_FILE = CREDENTIALS_DIR / "credentials"


@dataclass
class PostgresConfig:
    """Postgres connection settings."""
    host: str = "localhost"
    port: int = 5433
    database: str = "thyme"
    user: str = "thyme"
    password: str = "thyme"
    schema: str = "public"


@dataclass
class S3Config:
    """S3 connection settings."""
    bucket: str = ""
    prefix: str = ""
    region: str = "us-east-1"


@dataclass
class IcebergConfig:
    """Iceberg connection settings."""
    catalog: str = ""
    database: str = ""


@dataclass
class Config:
    """Thyme infrastructure configuration.

    Provides a single place to configure how the SDK connects to
    Thyme services and data sources. Supports loading from YAML files,
    environment variables, or explicit construction.
    """
    api_url: str = "http://localhost:8080/api/v1/commit"
    api_base: str = "http://localhost:8080"
    query_url: str = "http://localhost:8081"
    api_key: str = ""
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    s3: S3Config = field(default_factory=S3Config)
    iceberg: IcebergConfig = field(default_factory=IcebergConfig)

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

        # Start from file values (or defaults)
        config = cls(
            api_url=file_data.get("api_url", cls.api_url),
            api_base=file_data.get("api_base", cls.api_base),
            query_url=file_data.get("query_url", cls.query_url),
            api_key=file_data.get("api_key", cls.api_key),
        )

        # Postgres
        pg = file_data.get("postgres", {})
        config.postgres = PostgresConfig(
            host=pg.get("host", PostgresConfig.host),
            port=pg.get("port", PostgresConfig.port),
            database=pg.get("database", PostgresConfig.database),
            user=pg.get("user", PostgresConfig.user),
            password=pg.get("password", PostgresConfig.password),
            schema=pg.get("schema", PostgresConfig.schema),
        )

        # S3
        s3 = file_data.get("s3", {})
        config.s3 = S3Config(
            bucket=s3.get("bucket", S3Config.bucket),
            prefix=s3.get("prefix", S3Config.prefix),
            region=s3.get("region", S3Config.region),
        )

        # Iceberg
        ice = file_data.get("iceberg", {})
        config.iceberg = IcebergConfig(
            catalog=ice.get("catalog", IcebergConfig.catalog),
            database=ice.get("database", IcebergConfig.database),
        )

        # Stored credentials (lowest priority for api_key/api_base)
        creds = load_credentials()
        if creds:
            if not config.api_key:
                config.api_key = creds.get("api_key", "")
            if config.api_base == cls.api_base and creds.get("api_base"):
                config.api_base = creds["api_base"]
            if config.api_url == cls.api_url and creds.get("api_base"):
                config.api_url = creds["api_base"] + "/api/v1/commit"
            if config.query_url == cls.query_url and creds.get("query_url"):
                config.query_url = creds["query_url"]

        # Env var overrides (highest priority)
        _apply_env_overrides(config)

        return config

    def auth_headers(self) -> dict[str, str]:
        """Return HTTP headers for authenticated requests."""
        if self.api_key:
            return {"Authorization": f"Bearer {self.api_key}"}
        return {}

    def postgres_source(self, table: str, schema: str | None = None) -> "PostgresSource":
        """Create a PostgresSource from this config's connection settings."""
        from thyme.connectors import PostgresSource
        return PostgresSource(
            host=self.postgres.host,
            port=self.postgres.port,
            database=self.postgres.database,
            table=table,
            user=self.postgres.user,
            password=self.postgres.password,
            schema=schema or self.postgres.schema,
        )

    def s3_source(self, prefix: str | None = None) -> "S3JsonSource":
        """Create an S3JsonSource from this config's S3 settings."""
        from thyme.connectors import S3JsonSource
        return S3JsonSource(
            bucket=self.s3.bucket,
            prefix=prefix or self.s3.prefix,
            region=self.s3.region,
        )

    def iceberg_source(self, table: str, database: str | None = None) -> "IcebergSource":
        """Create an IcebergSource from this config's Iceberg settings."""
        from thyme.connectors import IcebergSource
        return IcebergSource(
            catalog=self.iceberg.catalog,
            database=database or self.iceberg.database,
            table=table,
        )


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
# Environment variable overrides
# ---------------------------------------------------------------------------


def _apply_env_overrides(config: Config) -> None:
    """Apply environment variable overrides (highest priority)."""
    env = {
        "THYME_API_URL": ("api_url", str),
        "THYME_API_BASE": ("api_base", str),
        "THYME_QUERY_URL": ("query_url", str),
        "THYME_API_KEY": ("api_key", str),
    }
    for key, (attr, fn) in env.items():
        val = os.environ.get(key)
        if val is not None:
            setattr(config, attr, fn(val))

    pg = {
        "THYME_POSTGRES_HOST": ("host", str),
        "THYME_POSTGRES_PORT": ("port", int),
        "THYME_POSTGRES_DATABASE": ("database", str),
        "THYME_POSTGRES_USER": ("user", str),
        "THYME_POSTGRES_PASSWORD": ("password", str),
        "THYME_POSTGRES_SCHEMA": ("schema", str),
    }
    for key, (attr, fn) in pg.items():
        val = os.environ.get(key)
        if val is not None:
            setattr(config.postgres, attr, fn(val))

    s3 = {
        "THYME_S3_BUCKET": ("bucket", str),
        "THYME_S3_PREFIX": ("prefix", str),
        "THYME_S3_REGION": ("region", str),
    }
    for key, (attr, fn) in s3.items():
        val = os.environ.get(key)
        if val is not None:
            setattr(config.s3, attr, fn(val))


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
