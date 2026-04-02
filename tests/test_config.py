"""Tests for thyme.config — Config loading, env overrides, and credential storage."""
from thyme.config import (
    Config,
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

    def test_kinesis_source(self):
        config = Config()
        config.kinesis.role_arn = "arn:aws:iam::123:role/reader"
        config.kinesis.region = "eu-west-1"
        source = config.kinesis_source(stream_arn="arn:aws:kinesis:eu-west-1:123:stream/events")
        d = source.to_dict()
        assert d["connector_type"] == "kinesis"
        assert d["config"]["stream_arn"] == "arn:aws:kinesis:eu-west-1:123:stream/events"
        assert d["config"]["role_arn"] == "arn:aws:iam::123:role/reader"
        assert d["config"]["region"] == "eu-west-1"

    def test_snowflake_source(self):
        config = Config()
        config.snowflake.account = "xy12345.us-east-1"
        config.snowflake.database = "analytics"
        config.snowflake.warehouse = "compute_wh"
        config.snowflake.user = "admin"
        config.snowflake.password = "secret"
        source = config.snowflake_source(table="orders")
        d = source.to_dict()
        assert d["connector_type"] == "snowflake"
        assert d["config"]["account"] == "xy12345.us-east-1"
        assert d["config"]["database"] == "analytics"
        assert d["config"]["warehouse"] == "compute_wh"
        assert d["config"]["table"] == "orders"
        assert d["config"]["user"] == "admin"
        assert d["config"]["password"] == "secret"

    def test_bigquery_source(self):
        config = Config()
        config.bigquery.project_id = "my-project"
        config.bigquery.dataset_id = "raw"
        source = config.bigquery_source(table="events")
        d = source.to_dict()
        assert d["connector_type"] == "bigquery"
        assert d["config"]["project_id"] == "my-project"
        assert d["config"]["dataset_id"] == "raw"
        assert d["config"]["table"] == "events"


class TestKinesisConfigDefaults:
    """Given no overrides, KinesisConfig should have sensible defaults."""

    def test_default_stream_arn_is_empty(self):
        config = Config()
        assert config.kinesis.stream_arn == ""

    def test_default_role_arn_is_empty(self):
        config = Config()
        assert config.kinesis.role_arn == ""

    def test_default_region_is_us_east_1(self):
        config = Config()
        assert config.kinesis.region == "us-east-1"


class TestKinesisEnvOverrides:
    """Given THYME_KINESIS_* env vars, they should override defaults."""

    def test_kinesis_from_env(self, monkeypatch):
        monkeypatch.setenv("THYME_KINESIS_STREAM_ARN", "arn:aws:kinesis:us-east-1:123:stream/s")
        monkeypatch.setenv("THYME_KINESIS_ROLE_ARN", "arn:aws:iam::123:role/r")
        monkeypatch.setenv("THYME_KINESIS_REGION", "ap-southeast-1")
        config = Config.load()
        assert config.kinesis.stream_arn == "arn:aws:kinesis:us-east-1:123:stream/s"
        assert config.kinesis.role_arn == "arn:aws:iam::123:role/r"
        assert config.kinesis.region == "ap-southeast-1"


class TestSnowflakeEnvOverrides:
    """Given THYME_SNOWFLAKE_* env vars, they should override defaults."""

    def test_snowflake_from_env(self, monkeypatch):
        monkeypatch.setenv("THYME_SNOWFLAKE_ACCOUNT", "xy12345.us-east-1")
        monkeypatch.setenv("THYME_SNOWFLAKE_DATABASE", "prod")
        monkeypatch.setenv("THYME_SNOWFLAKE_WAREHOUSE", "compute_wh")
        monkeypatch.setenv("THYME_SNOWFLAKE_USER", "admin")
        monkeypatch.setenv("THYME_SNOWFLAKE_PASSWORD", "secret")
        monkeypatch.setenv("THYME_SNOWFLAKE_SCHEMA", "RAW")
        monkeypatch.setenv("THYME_SNOWFLAKE_ROLE", "loader")
        config = Config.load()
        assert config.snowflake.account == "xy12345.us-east-1"
        assert config.snowflake.database == "prod"
        assert config.snowflake.warehouse == "compute_wh"
        assert config.snowflake.user == "admin"
        assert config.snowflake.password == "secret"
        assert config.snowflake.schema == "RAW"
        assert config.snowflake.role == "loader"


class TestBigQueryEnvOverrides:
    """Given THYME_BIGQUERY_* env vars, they should override defaults."""

    def test_bigquery_from_env(self, monkeypatch):
        monkeypatch.setenv("THYME_BIGQUERY_PROJECT", "my-project")
        monkeypatch.setenv("THYME_BIGQUERY_DATASET", "analytics")
        monkeypatch.setenv("THYME_BIGQUERY_CREDENTIALS", "/path/to/creds.json")
        config = Config.load()
        assert config.bigquery.project_id == "my-project"
        assert config.bigquery.dataset_id == "analytics"
        assert config.bigquery.credentials_json == "/path/to/creds.json"
