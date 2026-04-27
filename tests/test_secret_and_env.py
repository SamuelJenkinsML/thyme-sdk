"""Tests for Secret references and connector env-var fallbacks.

Two concerns covered here:

* `Secret(env=...)` round-trips through `to_dict()` and `compile_source()`
  without ever materializing the underlying value — that's the whole point
  (the engine resolves at runtime).
* When a connection-level kwarg is left unset, the connector falls back to
  `THYME_<TYPE>_<FIELD>`. Per-dataset kwargs (table, topic, stream_arn)
  must NOT have env fallback — silently turning a missing table into ""
  would mask a typo.
"""

import inspect

import pytest

from thyme import (
    BigQuerySource,
    KafkaSource,
    KinesisSource,
    PostgresSource,
    S3JsonSource,
    Secret,
    SnowflakeSource,
)
from thyme.compiler import compile_source


# ---------------------------------------------------------------------------
# Secret type
# ---------------------------------------------------------------------------


class TestSecret:
    def test_env_kind(self):
        s = Secret(env="MY_PW")
        assert s.kind == "env"
        assert s.value == "MY_PW"
        assert s.to_dict() == {"kind": "env", "value": "MY_PW"}

    def test_arn_kind(self):
        s = Secret(arn="arn:aws:secretsmanager:us-east-1:123:secret:foo")
        assert s.kind == "arn"

    def test_name_kind(self):
        s = Secret(name="prod-postgres")
        assert s.kind == "name"

    def test_requires_exactly_one(self):
        with pytest.raises(ValueError, match="exactly one"):
            Secret()
        with pytest.raises(ValueError, match="exactly one"):
            Secret(env="X", arn="Y")

    def test_value_must_be_non_empty(self):
        with pytest.raises(ValueError, match="non-empty"):
            Secret(env="")


# ---------------------------------------------------------------------------
# Connector env-var fallback
# ---------------------------------------------------------------------------


class TestPostgresEnvDefaults:
    def test_host_from_env(self, monkeypatch):
        monkeypatch.setenv("THYME_POSTGRES_HOST", "rds.example.com")
        src = PostgresSource(table="orders")
        assert src.to_dict()["config"]["host"] == "rds.example.com"

    def test_port_from_env_coerced_to_int(self, monkeypatch):
        monkeypatch.setenv("THYME_POSTGRES_PORT", "5433")
        src = PostgresSource(table="orders")
        assert src.to_dict()["config"]["port"] == 5433

    def test_invalid_port_raises(self, monkeypatch):
        monkeypatch.setenv("THYME_POSTGRES_PORT", "not-a-number")
        with pytest.raises(ValueError, match="not a valid integer"):
            PostgresSource(table="orders")

    def test_explicit_overrides_env(self, monkeypatch):
        monkeypatch.setenv("THYME_POSTGRES_HOST", "envhost")
        src = PostgresSource(table="orders", host="explicit-host")
        assert src.to_dict()["config"]["host"] == "explicit-host"

    def test_table_required(self):
        with pytest.raises(ValueError, match="requires 'table'"):
            PostgresSource(table="")

    def test_password_default_is_empty_literal(self):
        src = PostgresSource(table="orders")
        assert src.to_dict()["config"]["password"] == {"kind": "literal", "value": ""}


class TestKinesisEnvDefaults:
    def test_region_from_env(self, monkeypatch):
        monkeypatch.setenv("THYME_KINESIS_REGION", "eu-west-1")
        src = KinesisSource(stream_arn="arn:aws:kinesis:eu-west-1:123:stream/s")
        assert src.to_dict()["config"]["region"] == "eu-west-1"

    def test_endpoint_url_from_env(self, monkeypatch):
        monkeypatch.setenv("THYME_KINESIS_ENDPOINT_URL", "http://localstack:4566")
        src = KinesisSource(stream_arn="arn:aws:kinesis:us-east-1:123:stream/s")
        assert src.to_dict()["config"]["endpoint_url"] == "http://localstack:4566"

    def test_stream_arn_required(self):
        with pytest.raises(ValueError, match="requires 'stream_arn'"):
            KinesisSource(stream_arn="")

    def test_init_position_no_env_fallback(self, monkeypatch):
        # init_position is per-stream semantics, not infra. No env fallback.
        monkeypatch.setenv("THYME_KINESIS_INIT_POSITION", "trim_horizon")
        src = KinesisSource(stream_arn="arn:foo")
        # default still wins; the env var is ignored
        assert src.to_dict()["config"]["init_position"] == "latest"


class TestKafkaEnvDefaults:
    def test_brokers_from_env(self, monkeypatch):
        monkeypatch.setenv("THYME_KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092")
        src = KafkaSource(topic="events")
        assert src.to_dict()["config"]["brokers"] == "kafka-1:9092,kafka-2:9092"

    def test_topic_required(self):
        with pytest.raises(ValueError, match="requires 'topic'"):
            KafkaSource(topic="")


class TestSnowflakeEnvDefaults:
    def test_account_from_env(self, monkeypatch):
        monkeypatch.setenv("THYME_SNOWFLAKE_ACCOUNT", "xy12345.us-east-1")
        src = SnowflakeSource(table="orders")
        assert src.to_dict()["config"]["account"] == "xy12345.us-east-1"


class TestBigQueryEnvDefaults:
    def test_project_id_from_env(self, monkeypatch):
        monkeypatch.setenv("THYME_BIGQUERY_PROJECT_ID", "my-gcp-project")
        src = BigQuerySource(dataset_id="raw", table="events")
        assert src.to_dict()["config"]["project_id"] == "my-gcp-project"


class TestS3EnvDefaults:
    def test_region_from_env(self, monkeypatch):
        monkeypatch.setenv("THYME_S3_REGION", "ap-southeast-2")
        src = S3JsonSource(bucket="my-bucket")
        assert src.to_dict()["config"]["region"] == "ap-southeast-2"


# ---------------------------------------------------------------------------
# Secret round-trip through to_dict() and compile_source()
# ---------------------------------------------------------------------------


class TestSecretRoundTrip:
    def test_postgres_password_secret_to_dict(self):
        src = PostgresSource(table="orders", password=Secret(env="PG_PW"))
        cfg = src.to_dict()["config"]
        assert cfg["password"] == {"kind": "env", "value": "PG_PW"}

    def test_postgres_password_secret_compiled_to_proto(self):
        src = PostgresSource(table="orders", password=Secret(env="PG_PW"))
        meta = src.to_dict()
        meta["dataset"] = "Orders"
        proto = compile_source(meta)
        # ENV kind == 1 in the proto enum.
        assert proto.postgres.password.kind == 1
        assert proto.postgres.password.value == "PG_PW"

    def test_kinesis_role_arn_secret_compiled_to_proto(self):
        src = KinesisSource(
            stream_arn="arn:aws:kinesis:us-east-1:123:stream/s",
            role_arn=Secret(env="KINESIS_ROLE"),
        )
        meta = src.to_dict()
        meta["dataset"] = "Events"
        proto = compile_source(meta)
        assert proto.kinesis.role_arn.kind == 1  # ENV
        assert proto.kinesis.role_arn.value == "KINESIS_ROLE"

    def test_kafka_sasl_password_secret(self):
        src = KafkaSource(topic="events", sasl_password=Secret(env="KAFKA_PW"))
        cfg = src.to_dict()["config"]
        assert cfg["sasl_password"] == {"kind": "env", "value": "KAFKA_PW"}

    def test_snowflake_password_secret(self):
        src = SnowflakeSource(table="orders", password=Secret(env="SF_PW"))
        cfg = src.to_dict()["config"]
        assert cfg["password"] == {"kind": "env", "value": "SF_PW"}

    def test_bigquery_credentials_json_secret(self):
        src = BigQuerySource(
            dataset_id="raw", table="events", credentials_json=Secret(env="GCP_CREDS"),
        )
        cfg = src.to_dict()["config"]
        assert cfg["credentials_json"] == {"kind": "env", "value": "GCP_CREDS"}


# ---------------------------------------------------------------------------
# Captured-source safety: the SDK file is `inspect.getsource()`-d and stored
# in definition-service Postgres. A user typing `Secret(env="X")` must NEVER
# leak their plaintext credential into that captured source.
# ---------------------------------------------------------------------------


class TestCapturedSourceSafety:
    def test_secret_env_keeps_plaintext_out_of_inspect_getsource(self):
        # Given: a module-level features.py that uses Secret(env=...)
        plaintext_secret = "supersecret-should-never-leak"

        def build():
            # The plaintext lives in the surrounding closure but the user code
            # only references Secret(env="REAL_PW"). When a downstream tool
            # captures `inspect.getsource(build)` it should not see the
            # plaintext.
            return PostgresSource(table="orders", password=Secret(env="REAL_PW"))

        src = inspect.getsource(build)
        # Sanity: the closure variable is technically in scope, but the
        # function body doesn't reference it.
        assert "Secret(env=" in src
        assert plaintext_secret not in src

        # And the actual proto carries the SecretRef, not the plaintext.
        connector = build()
        meta = connector.to_dict()
        meta["dataset"] = "Orders"
        proto = compile_source(meta)
        assert plaintext_secret not in str(proto)
        assert proto.postgres.password.value == "REAL_PW"
