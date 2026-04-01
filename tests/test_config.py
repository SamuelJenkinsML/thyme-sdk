"""Tests for thyme.config — Config loading, env overrides, and credential storage."""
import json
import os
from pathlib import Path

import pytest

from thyme.config import (
    Config,
    PostgresConfig,
    S3Config,
    clear_credentials,
    load_credentials,
    save_credentials,
)


class TestConfigDefaults:
    """Given no config file or env vars, defaults should match local dev setup."""

    def test_default_api_url(self):
        config = Config()
        assert config.api_url == "http://localhost:8080/api/v1/commit"

    def test_default_postgres(self):
        config = Config()
        assert config.postgres.host == "localhost"
        assert config.postgres.port == 5433
        assert config.postgres.database == "thyme"

    def test_default_api_key_is_empty(self):
        config = Config()
        assert config.api_key == ""

    def test_auth_headers_empty_when_no_key(self):
        config = Config()
        assert config.auth_headers() == {}

    def test_auth_headers_with_key(self):
        config = Config(api_key="tk_test123")
        assert config.auth_headers() == {"Authorization": "Bearer tk_test123"}


class TestConfigFromYaml:
    """Given a .thyme.yaml file, Config.load() should read it."""

    def test_load_from_explicit_path(self, tmp_path):
        yaml_file = tmp_path / ".thyme.yaml"
        yaml_file.write_text(
            "api_base: http://prod.example.com\n"
            "api_key: tk_from_file\n"
            "postgres:\n"
            "  host: db.prod.example.com\n"
            "  port: 5432\n"
            "  database: orders_db\n"
        )
        config = Config.load(path=yaml_file)
        assert config.api_base == "http://prod.example.com"
        assert config.api_key == "tk_from_file"
        assert config.postgres.host == "db.prod.example.com"
        assert config.postgres.port == 5432
        assert config.postgres.database == "orders_db"
        # Unset values fall back to defaults
        assert config.postgres.user == "thyme"

    def test_load_from_cwd(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        yaml_file = tmp_path / ".thyme.yaml"
        yaml_file.write_text("api_key: tk_cwd\n")
        config = Config.load()
        assert config.api_key == "tk_cwd"

    def test_missing_file_returns_defaults(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr("thyme.config.CREDENTIALS_FILE", tmp_path / "nonexistent")
        config = Config.load()
        assert config.api_url == "http://localhost:8080/api/v1/commit"


class TestConfigEnvOverrides:
    """Given environment variables, they should override file and default values."""

    def test_api_key_from_env(self, monkeypatch):
        monkeypatch.setenv("THYME_API_KEY", "tk_env")
        config = Config.load()
        assert config.api_key == "tk_env"

    def test_postgres_host_from_env(self, monkeypatch):
        monkeypatch.setenv("THYME_POSTGRES_HOST", "rds.amazonaws.com")
        monkeypatch.setenv("THYME_POSTGRES_PORT", "5432")
        config = Config.load()
        assert config.postgres.host == "rds.amazonaws.com"
        assert config.postgres.port == 5432

    def test_env_overrides_yaml(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        yaml_file = tmp_path / ".thyme.yaml"
        yaml_file.write_text("api_key: tk_from_file\n")
        monkeypatch.setenv("THYME_API_KEY", "tk_from_env")
        config = Config.load()
        assert config.api_key == "tk_from_env"

    def test_s3_from_env(self, monkeypatch):
        monkeypatch.setenv("THYME_S3_BUCKET", "my-bucket")
        monkeypatch.setenv("THYME_S3_REGION", "eu-west-1")
        config = Config.load()
        assert config.s3.bucket == "my-bucket"
        assert config.s3.region == "eu-west-1"

    def test_kafka_from_env(self, monkeypatch):
        monkeypatch.setenv("THYME_KAFKA_BROKERS", "kafka:9092")
        monkeypatch.setenv("THYME_KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
        config = Config.load()
        assert config.kafka.brokers == "kafka:9092"
        assert config.kafka.security_protocol == "SASL_SSL"


class TestCredentialStorage:
    """Given thyme login/logout, credentials should persist to disk."""

    def test_save_and_load(self, tmp_path, monkeypatch):
        creds_file = tmp_path / "credentials"
        monkeypatch.setattr("thyme.config.CREDENTIALS_DIR", tmp_path)
        monkeypatch.setattr("thyme.config.CREDENTIALS_FILE", creds_file)

        save_credentials("tk_stored", "http://prod.example.com")
        creds = load_credentials()
        assert creds is not None
        assert creds["api_key"] == "tk_stored"
        assert creds["api_base"] == "http://prod.example.com"

    def test_file_permissions(self, tmp_path, monkeypatch):
        creds_file = tmp_path / "credentials"
        monkeypatch.setattr("thyme.config.CREDENTIALS_DIR", tmp_path)
        monkeypatch.setattr("thyme.config.CREDENTIALS_FILE", creds_file)

        save_credentials("tk_secret", "http://example.com")
        # File should be owner-readable only (0o600)
        mode = creds_file.stat().st_mode & 0o777
        assert mode == 0o600

    def test_clear_credentials(self, tmp_path, monkeypatch):
        creds_file = tmp_path / "credentials"
        monkeypatch.setattr("thyme.config.CREDENTIALS_DIR", tmp_path)
        monkeypatch.setattr("thyme.config.CREDENTIALS_FILE", creds_file)

        save_credentials("tk_stored", "http://example.com")
        assert clear_credentials() is True
        assert load_credentials() is None

    def test_clear_nonexistent(self, tmp_path, monkeypatch):
        creds_file = tmp_path / "credentials"
        monkeypatch.setattr("thyme.config.CREDENTIALS_FILE", creds_file)
        assert clear_credentials() is False

    def test_load_no_file(self, tmp_path, monkeypatch):
        creds_file = tmp_path / "nonexistent"
        monkeypatch.setattr("thyme.config.CREDENTIALS_FILE", creds_file)
        assert load_credentials() is None


class TestCredentialIntegrationWithConfig:
    """Given stored credentials, Config.load() should use them as fallback."""

    def test_stored_creds_provide_api_key(self, tmp_path, monkeypatch):
        creds_file = tmp_path / "credentials"
        monkeypatch.setattr("thyme.config.CREDENTIALS_DIR", tmp_path)
        monkeypatch.setattr("thyme.config.CREDENTIALS_FILE", creds_file)
        monkeypatch.chdir(tmp_path)

        save_credentials("tk_stored", "http://prod.example.com", "http://prod.example.com")
        config = Config.load()
        assert config.api_key == "tk_stored"
        assert config.api_base == "http://prod.example.com"

    def test_env_overrides_stored_creds(self, tmp_path, monkeypatch):
        creds_file = tmp_path / "credentials"
        monkeypatch.setattr("thyme.config.CREDENTIALS_DIR", tmp_path)
        monkeypatch.setattr("thyme.config.CREDENTIALS_FILE", creds_file)
        monkeypatch.chdir(tmp_path)

        save_credentials("tk_stored", "http://prod.example.com")
        monkeypatch.setenv("THYME_API_KEY", "tk_env_wins")
        config = Config.load()
        assert config.api_key == "tk_env_wins"

    def test_yaml_overrides_stored_creds(self, tmp_path, monkeypatch):
        creds_file = tmp_path / "credentials"
        monkeypatch.setattr("thyme.config.CREDENTIALS_DIR", tmp_path)
        monkeypatch.setattr("thyme.config.CREDENTIALS_FILE", creds_file)
        monkeypatch.chdir(tmp_path)

        save_credentials("tk_stored", "http://prod.example.com")
        (tmp_path / ".thyme.yaml").write_text("api_key: tk_yaml_wins\n")
        config = Config.load()
        assert config.api_key == "tk_yaml_wins"


class TestYamlDeepNesting:
    """Given deeply nested YAML, Config.load() should parse correctly."""

    def test_deeply_nested_yaml(self, tmp_path):
        yaml_file = tmp_path / ".thyme.yaml"
        yaml_file.write_text(
            "api_key: tk_deep\n"
            "postgres:\n"
            "  host: db.example.com\n"
            "  port: 5432\n"
        )
        config = Config.load(path=yaml_file)
        assert config.api_key == "tk_deep"
        assert config.postgres.host == "db.example.com"
        assert config.postgres.port == 5432


class TestConnectorFactories:
    """Given a Config, factory methods should create correctly configured connectors."""

    def test_postgres_source(self):
        config = Config()
        config.postgres.host = "db.example.com"
        config.postgres.port = 5432
        source = config.postgres_source(table="orders")
        d = source.to_dict()
        assert d["connector_type"] == "postgres"
        assert d["config"]["host"] == "db.example.com"
        assert d["config"]["port"] == 5432
        assert d["config"]["table"] == "orders"

    def test_s3_source(self):
        config = Config()
        config.s3.bucket = "my-bucket"
        config.s3.region = "eu-west-1"
        source = config.s3_source(prefix="data/")
        d = source.to_dict()
        assert d["connector_type"] == "s3json"
        assert d["config"]["bucket"] == "my-bucket"
        assert d["config"]["prefix"] == "data/"
        assert d["config"]["region"] == "eu-west-1"

    def test_iceberg_source(self):
        config = Config()
        config.iceberg.catalog = "prod"
        config.iceberg.database = "events"
        source = config.iceberg_source(table="orders")
        d = source.to_dict()
        assert d["connector_type"] == "iceberg"
        assert d["config"]["catalog"] == "prod"
        assert d["config"]["table"] == "orders"

    def test_kafka_source(self):
        config = Config()
        config.kafka.brokers = "kafka:9092"
        config.kafka.security_protocol = "SASL_SSL"
        config.kafka.sasl_mechanism = "PLAIN"
        config.kafka.sasl_username = "user"
        config.kafka.sasl_password = "pass"
        source = config.kafka_source(topic="raw-events")
        d = source.to_dict()
        assert d["connector_type"] == "kafka"
        assert d["config"]["brokers"] == "kafka:9092"
        assert d["config"]["topic"] == "raw-events"
        assert d["config"]["security_protocol"] == "SASL_SSL"
        assert d["config"]["sasl_mechanism"] == "PLAIN"
        assert d["config"]["sasl_username"] == "user"
        assert d["config"]["sasl_password"] == "pass"
