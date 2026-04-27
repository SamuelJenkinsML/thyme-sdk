import pytest

from thyme.connectors import (
    BigQuerySource,
    IcebergSource,
    KafkaSource,
    KinesisSource,
    PostgresSource,
    S3JsonSource,
    SnowflakeSource,
    source,
    clear_source_registry,
    get_registered_sources,
)


@pytest.fixture(autouse=True)
def clean_registry():
    clear_source_registry()
    yield
    clear_source_registry()


def test_iceberg_source_to_dict():
    src = IcebergSource(catalog="local", database="restaurant_reviews", table="reviews")
    d = src.to_dict()
    assert d["connector_type"] == "iceberg"
    assert d["config"]["catalog"] == "local"
    assert d["config"]["database"] == "restaurant_reviews"
    assert d["config"]["table"] == "reviews"


def test_source_decorator_registers():
    reviews_src = IcebergSource(catalog="local", database="db", table="reviews")

    @source(reviews_src, cursor="timestamp", every="1m")
    class Review:
        pass

    sources = get_registered_sources()
    assert "Review" in sources
    assert sources["Review"]["dataset"] == "Review"
    assert sources["Review"]["cursor"] == "timestamp"
    assert sources["Review"]["every"] == "1m"
    assert sources["Review"]["connector_type"] == "iceberg"


def test_source_decorator_preserves_class():
    src = IcebergSource(catalog="c", database="d", table="t")

    @source(src, cursor="ts")
    class MyDataset:
        x = 42

    assert MyDataset.x == 42
    assert hasattr(MyDataset, "_source_meta")


def test_source_max_lateness_is_stored():
    src = IcebergSource(catalog="c", database="d", table="t")

    @source(src, cursor="ts", every="5m", max_lateness="1h")
    class Review:
        pass

    sources = get_registered_sources()
    assert sources["Review"]["max_lateness"] == "1h"


def test_source_max_lateness_defaults_to_empty():
    src = IcebergSource(catalog="c", database="d", table="t")

    @source(src, cursor="ts")
    class Review:
        pass

    sources = get_registered_sources()
    assert sources["Review"]["max_lateness"] == ""


def test_source_cdc_defaults_to_append():
    src = IcebergSource(catalog="c", database="d", table="t")

    @source(src, cursor="ts")
    class Review:
        pass

    sources = get_registered_sources()
    assert sources["Review"]["cdc"] == "append"


def test_source_cdc_append_is_stored():
    src = IcebergSource(catalog="c", database="d", table="t")

    @source(src, cursor="ts", cdc="append")
    class Review:
        pass

    sources = get_registered_sources()
    assert sources["Review"]["cdc"] == "append"


def test_source_cdc_debezium_is_stored():
    src = IcebergSource(catalog="c", database="d", table="t")

    @source(src, cursor="ts", cdc="debezium")
    class Review:
        pass

    sources = get_registered_sources()
    assert sources["Review"]["cdc"] == "debezium"


def test_source_cdc_upsert_is_stored():
    src = IcebergSource(catalog="c", database="d", table="t")

    @source(src, cursor="ts", cdc="upsert")
    class Review:
        pass

    sources = get_registered_sources()
    assert sources["Review"]["cdc"] == "upsert"


def test_source_invalid_cdc_raises_value_error():
    src = IcebergSource(catalog="c", database="d", table="t")

    with pytest.raises(ValueError, match="cdc"):
        @source(src, cursor="ts", cdc="invalid_mode")
        class Review:
            pass


# ---------------------------------------------------------------------------
# PostgresSource tests
# ---------------------------------------------------------------------------

def test_postgres_source_to_dict_connector_type():
    # Given: a PostgresSource configuration
    src = PostgresSource(
        host="localhost", port=5432, database="mydb",
        table="orders", user="admin", password="secret",
    )

    # When: calling to_dict
    d = src.to_dict()

    # Then: connector_type is "postgres"
    assert d["connector_type"] == "postgres"


def test_postgres_source_to_dict_config_fields():
    # Given: a PostgresSource with all fields
    src = PostgresSource(
        host="db.example.com", port=5433, database="sales",
        table="events", user="reader", password="pw", schema="public",
    )

    # When: calling to_dict
    d = src.to_dict()

    # Then: config contains all connection fields
    cfg = d["config"]
    assert cfg["host"] == "db.example.com"
    assert cfg["port"] == 5433
    assert cfg["database"] == "sales"
    assert cfg["table"] == "events"
    assert cfg["user"] == "reader"
    assert cfg["password"] == {"kind": "literal", "value": "pw"}
    assert cfg["schema"] == "public"


def test_postgres_source_default_schema_is_public():
    # Given: a PostgresSource without explicit schema
    src = PostgresSource(host="h", port=5432, database="d", table="t", user="u", password="p")

    # When: calling to_dict
    d = src.to_dict()

    # Then: schema defaults to "public"
    assert d["config"]["schema"] == "public"


def test_postgres_source_registers_with_source_decorator():
    # Given: a PostgresSource attached to a dataset
    pg_src = PostgresSource(host="h", port=5432, database="d", table="t", user="u", password="p")

    @source(pg_src, cursor="updated_at", every="5m")
    class PgDataset:
        pass

    # When: reading registered sources
    sources = get_registered_sources()

    # Then: the source is registered with the correct connector type
    assert "PgDataset" in sources
    assert sources["PgDataset"]["connector_type"] == "postgres"
    assert sources["PgDataset"]["cursor"] == "updated_at"


# ---------------------------------------------------------------------------
# S3JsonSource tests
# ---------------------------------------------------------------------------

def test_s3json_source_to_dict_connector_type():
    # Given: an S3JsonSource
    src = S3JsonSource(bucket="my-bucket", prefix="events/", region="us-east-1")

    # When: calling to_dict
    d = src.to_dict()

    # Then: connector_type is "s3json"
    assert d["connector_type"] == "s3json"


def test_s3json_source_to_dict_config_fields():
    # Given: an S3JsonSource with all fields
    src = S3JsonSource(bucket="data-lake", prefix="orders/2024/", region="eu-west-1")

    # When: calling to_dict
    d = src.to_dict()

    # Then: config contains bucket, prefix, region
    cfg = d["config"]
    assert cfg["bucket"] == "data-lake"
    assert cfg["prefix"] == "orders/2024/"
    assert cfg["region"] == "eu-west-1"


def test_s3json_source_default_region_is_us_east_1():
    # Given: an S3JsonSource without explicit region
    src = S3JsonSource(bucket="my-bucket")

    # When: calling to_dict
    d = src.to_dict()

    # Then: region defaults to "us-east-1"
    assert d["config"]["region"] == "us-east-1"


def test_s3json_source_default_prefix_is_empty():
    # Given: an S3JsonSource without explicit prefix
    src = S3JsonSource(bucket="my-bucket")

    # When: calling to_dict
    d = src.to_dict()

    # Then: prefix defaults to ""
    assert d["config"]["prefix"] == ""


def test_s3json_source_registers_with_source_decorator():
    # Given: an S3JsonSource attached to a dataset
    s3_src = S3JsonSource(bucket="events-bucket", prefix="raw/")

    @source(s3_src, cursor="ts", every="10m")
    class S3Dataset:
        pass

    # When: reading registered sources
    sources = get_registered_sources()

    # Then: correctly registered
    assert "S3Dataset" in sources
    assert sources["S3Dataset"]["connector_type"] == "s3json"


# ---------------------------------------------------------------------------
# KafkaSource tests
# ---------------------------------------------------------------------------

def test_kafka_source_to_dict_connector_type():
    # Given: a KafkaSource with required fields
    src = KafkaSource(brokers="kafka:9092", topic="raw-events")

    # When: calling to_dict
    d = src.to_dict()

    # Then: connector_type is "kafka"
    assert d["connector_type"] == "kafka"


def test_kafka_source_to_dict_config_fields():
    # Given: a KafkaSource with all fields
    src = KafkaSource(
        brokers="kafka:9092,kafka:9093",
        topic="raw-events",
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_username="user",
        sasl_password="pass",
        format="avro",
        group_id="my-group",
        schema_registry_url="http://registry:8081",
    )

    # When: calling to_dict
    d = src.to_dict()

    # Then: config contains all fields with correct values
    cfg = d["config"]
    assert cfg["brokers"] == "kafka:9092,kafka:9093"
    assert cfg["topic"] == "raw-events"
    assert cfg["security_protocol"] == "SASL_SSL"
    assert cfg["sasl_mechanism"] == "PLAIN"
    assert cfg["sasl_username"] == "user"
    assert cfg["sasl_password"] == {"kind": "literal", "value": "pass"}
    assert cfg["format"] == "avro"
    assert cfg["group_id"] == "my-group"
    assert cfg["schema_registry_url"] == "http://registry:8081"


def test_kafka_source_default_security_protocol_is_plaintext():
    # Given: a KafkaSource without explicit security_protocol
    src = KafkaSource(brokers="kafka:9092", topic="events")

    # When: calling to_dict
    d = src.to_dict()

    # Then: security_protocol defaults to "PLAINTEXT"
    assert d["config"]["security_protocol"] == "PLAINTEXT"


def test_kafka_source_default_format_is_json():
    # Given: a KafkaSource without explicit format
    src = KafkaSource(brokers="kafka:9092", topic="events")

    # When: calling to_dict
    d = src.to_dict()

    # Then: format defaults to "json"
    assert d["config"]["format"] == "json"


def test_kafka_source_default_sasl_fields_are_empty():
    # Given: a KafkaSource without SASL fields
    src = KafkaSource(brokers="kafka:9092", topic="events")

    # When: calling to_dict
    d = src.to_dict()

    # Then: SASL fields default to empty string (sasl_password is a SecretRef-shaped dict)
    assert d["config"]["sasl_mechanism"] == ""
    assert d["config"]["sasl_username"] == ""
    assert d["config"]["sasl_password"] == {"kind": "literal", "value": ""}


def test_kafka_source_default_group_id_is_empty():
    # Given: a KafkaSource without group_id
    src = KafkaSource(brokers="kafka:9092", topic="events")

    # When: calling to_dict
    d = src.to_dict()

    # Then: group_id defaults to ""
    assert d["config"]["group_id"] == ""


def test_kafka_source_default_schema_registry_url_is_empty():
    # Given: a KafkaSource without schema_registry_url
    src = KafkaSource(brokers="kafka:9092", topic="events")

    # When: calling to_dict
    d = src.to_dict()

    # Then: schema_registry_url defaults to ""
    assert d["config"]["schema_registry_url"] == ""


def test_kafka_source_invalid_security_protocol_raises():
    # Given/When: creating KafkaSource with invalid security_protocol
    # Then: ValueError is raised
    with pytest.raises(ValueError, match="security_protocol"):
        KafkaSource(brokers="kafka:9092", topic="events", security_protocol="INVALID")


def test_kafka_source_invalid_format_raises():
    # Given/When: creating KafkaSource with invalid format
    # Then: ValueError is raised
    with pytest.raises(ValueError, match="format"):
        KafkaSource(brokers="kafka:9092", topic="events", format="xml")


def test_kafka_source_registers_with_source_decorator():
    # Given: a KafkaSource attached to a dataset (streaming: no cursor/every)
    kafka_src = KafkaSource(brokers="kafka:9092", topic="raw-events")

    @source(kafka_src, cdc="append")
    class KafkaDataset:
        pass

    # When: reading registered sources
    sources = get_registered_sources()

    # Then: correctly registered
    assert "KafkaDataset" in sources
    assert sources["KafkaDataset"]["connector_type"] == "kafka"
    assert sources["KafkaDataset"]["cursor"] == ""


# ---------------------------------------------------------------------------
# KinesisSource tests
# ---------------------------------------------------------------------------

def test_kinesis_source_to_dict_connector_type():
    # Given: a KinesisSource with required fields
    src = KinesisSource(stream_arn="arn:aws:kinesis:us-east-1:123456789:stream/events")

    # When: calling to_dict
    d = src.to_dict()

    # Then: connector_type is "kinesis"
    assert d["connector_type"] == "kinesis"


def test_kinesis_source_to_dict_config_fields():
    # Given: a KinesisSource with all fields
    src = KinesisSource(
        stream_arn="arn:aws:kinesis:us-east-1:123456789:stream/events",
        role_arn="arn:aws:iam::123456789:role/kinesis-reader",
        region="eu-west-1",
        init_position="trim_horizon",
        format="json",
    )

    # When: calling to_dict
    d = src.to_dict()

    # Then: config contains all fields with correct values
    cfg = d["config"]
    assert cfg["stream_arn"] == "arn:aws:kinesis:us-east-1:123456789:stream/events"
    assert cfg["role_arn"] == {"kind": "literal", "value": "arn:aws:iam::123456789:role/kinesis-reader"}
    assert cfg["region"] == "eu-west-1"
    assert cfg["init_position"] == "trim_horizon"
    assert cfg["format"] == "json"


def test_kinesis_source_default_region_is_us_east_1():
    # Given: a KinesisSource without explicit region
    src = KinesisSource(stream_arn="arn:aws:kinesis:us-east-1:123:stream/s")

    # When: calling to_dict
    d = src.to_dict()

    # Then: region defaults to "us-east-1"
    assert d["config"]["region"] == "us-east-1"


def test_kinesis_source_default_init_position_is_latest():
    # Given: a KinesisSource without explicit init_position
    src = KinesisSource(stream_arn="arn:aws:kinesis:us-east-1:123:stream/s")

    # When: calling to_dict
    d = src.to_dict()

    # Then: init_position defaults to "latest"
    assert d["config"]["init_position"] == "latest"


def test_kinesis_source_default_format_is_json():
    # Given: a KinesisSource without explicit format
    src = KinesisSource(stream_arn="arn:aws:kinesis:us-east-1:123:stream/s")

    # When: calling to_dict
    d = src.to_dict()

    # Then: format defaults to "json"
    assert d["config"]["format"] == "json"


def test_kinesis_source_default_role_arn_is_empty():
    # Given: a KinesisSource without explicit role_arn
    src = KinesisSource(stream_arn="arn:aws:kinesis:us-east-1:123:stream/s")

    # When: calling to_dict
    d = src.to_dict()

    # Then: role_arn defaults to an empty literal SecretRef
    assert d["config"]["role_arn"] == {"kind": "literal", "value": ""}


def test_kinesis_source_init_position_trim_horizon():
    # Given/When: a KinesisSource with trim_horizon
    src = KinesisSource(
        stream_arn="arn:aws:kinesis:us-east-1:123:stream/s",
        init_position="trim_horizon",
    )

    # Then: no error, value stored
    assert src.init_position == "trim_horizon"


def test_kinesis_source_init_position_iso8601_timestamp():
    # Given/When: a KinesisSource with an ISO-8601 timestamp
    src = KinesisSource(
        stream_arn="arn:aws:kinesis:us-east-1:123:stream/s",
        init_position="2024-01-15T10:30:00",
    )

    # Then: no error, value stored
    assert src.init_position == "2024-01-15T10:30:00"


def test_kinesis_source_invalid_init_position_raises():
    # Given/When: creating KinesisSource with invalid init_position
    # Then: ValueError is raised
    with pytest.raises(ValueError, match="init_position"):
        KinesisSource(
            stream_arn="arn:aws:kinesis:us-east-1:123:stream/s",
            init_position="INVALID",
        )


def test_kinesis_source_invalid_format_raises():
    # Given/When: creating KinesisSource with invalid format
    # Then: ValueError is raised
    with pytest.raises(ValueError, match="format"):
        KinesisSource(
            stream_arn="arn:aws:kinesis:us-east-1:123:stream/s",
            format="avro",
        )


def test_kinesis_source_registers_with_source_decorator():
    # Given: a KinesisSource attached to a dataset (streaming: no cursor/every)
    kin_src = KinesisSource(
        stream_arn="arn:aws:kinesis:us-east-1:123:stream/events",
    )

    @source(kin_src, max_lateness="5m", cdc="append")
    class KinesisDataset:
        pass

    # When: reading registered sources
    sources = get_registered_sources()

    # Then: correctly registered
    assert "KinesisDataset" in sources
    assert sources["KinesisDataset"]["connector_type"] == "kinesis"
    assert sources["KinesisDataset"]["cursor"] == ""
    assert sources["KinesisDataset"]["every"] == ""
    assert sources["KinesisDataset"]["max_lateness"] == "5m"


# ---------------------------------------------------------------------------
# Streaming source validation tests (cursor/every rejection)
# ---------------------------------------------------------------------------

def test_source_rejects_cursor_for_kafka():
    # Given: a KafkaSource with cursor set
    kafka_src = KafkaSource(brokers="kafka:9092", topic="events")

    # When/Then: @source raises ValueError
    with pytest.raises(ValueError, match="cursor"):
        @source(kafka_src, cursor="ts")
        class BadKafka:
            pass


def test_source_rejects_every_for_kafka():
    # Given: a KafkaSource with every set
    kafka_src = KafkaSource(brokers="kafka:9092", topic="events")

    # When/Then: @source raises ValueError
    with pytest.raises(ValueError, match="every"):
        @source(kafka_src, every="5m")
        class BadKafka:
            pass


def test_source_rejects_cursor_for_kinesis():
    # Given: a KinesisSource with cursor set
    kin_src = KinesisSource(stream_arn="arn:aws:kinesis:us-east-1:123:stream/s")

    # When/Then: @source raises ValueError
    with pytest.raises(ValueError, match="cursor"):
        @source(kin_src, cursor="ts")
        class BadKinesis:
            pass


def test_source_rejects_every_for_kinesis():
    # Given: a KinesisSource with every set
    kin_src = KinesisSource(stream_arn="arn:aws:kinesis:us-east-1:123:stream/s")

    # When/Then: @source raises ValueError
    with pytest.raises(ValueError, match="every"):
        @source(kin_src, every="1m")
        class BadKinesis:
            pass


# ---------------------------------------------------------------------------
# SnowflakeSource tests
# ---------------------------------------------------------------------------

def test_snowflake_source_to_dict_connector_type():
    # Given: a SnowflakeSource configuration
    src = SnowflakeSource(
        account="xy12345.us-east-1", database="prod", warehouse="compute_wh",
        table="orders", user="admin", password="secret",
    )

    # When: calling to_dict
    d = src.to_dict()

    # Then: connector_type is "snowflake"
    assert d["connector_type"] == "snowflake"


def test_snowflake_source_to_dict_config_fields():
    # Given: a SnowflakeSource with all fields
    src = SnowflakeSource(
        account="xy12345.us-east-1", database="analytics", schema="RAW",
        warehouse="etl_wh", role="loader", table="events",
        user="etl_user", password="pw",
    )

    # When: calling to_dict
    d = src.to_dict()

    # Then: config contains all connection fields
    cfg = d["config"]
    assert cfg["account"] == "xy12345.us-east-1"
    assert cfg["database"] == "analytics"
    assert cfg["schema"] == "RAW"
    assert cfg["warehouse"] == "etl_wh"
    assert cfg["role"] == "loader"
    assert cfg["table"] == "events"
    assert cfg["user"] == "etl_user"
    assert cfg["password"] == {"kind": "literal", "value": "pw"}


def test_snowflake_source_default_schema_is_public():
    # Given: a SnowflakeSource without explicit schema
    src = SnowflakeSource(
        account="a", database="d", warehouse="w", table="t", user="u", password="p",
    )

    # When: calling to_dict
    d = src.to_dict()

    # Then: schema defaults to "PUBLIC"
    assert d["config"]["schema"] == "PUBLIC"


def test_snowflake_source_default_role_is_empty():
    # Given: a SnowflakeSource without explicit role
    src = SnowflakeSource(
        account="a", database="d", warehouse="w", table="t", user="u", password="p",
    )

    # When: calling to_dict
    d = src.to_dict()

    # Then: role defaults to ""
    assert d["config"]["role"] == ""


def test_snowflake_source_registers_with_source_decorator():
    # Given: a SnowflakeSource attached to a dataset
    sf_src = SnowflakeSource(
        account="a", database="d", warehouse="w", table="t", user="u", password="p",
    )

    @source(sf_src, cursor="updated_at", every="5m")
    class SnowflakeDataset:
        pass

    # When: reading registered sources
    sources = get_registered_sources()

    # Then: the source is registered with the correct connector type
    assert "SnowflakeDataset" in sources
    assert sources["SnowflakeDataset"]["connector_type"] == "snowflake"
    assert sources["SnowflakeDataset"]["cursor"] == "updated_at"


# ---------------------------------------------------------------------------
# BigQuerySource tests
# ---------------------------------------------------------------------------

def test_bigquery_source_to_dict_connector_type():
    # Given: a BigQuerySource configuration
    src = BigQuerySource(
        project_id="my-project", dataset_id="analytics", table="events",
    )

    # When: calling to_dict
    d = src.to_dict()

    # Then: connector_type is "bigquery"
    assert d["connector_type"] == "bigquery"


def test_bigquery_source_to_dict_config_fields():
    # Given: a BigQuerySource with all fields
    src = BigQuerySource(
        project_id="my-project", dataset_id="raw", table="clicks",
        credentials_json='{"type": "service_account"}',
    )

    # When: calling to_dict
    d = src.to_dict()

    # Then: config contains all fields
    cfg = d["config"]
    assert cfg["project_id"] == "my-project"
    assert cfg["dataset_id"] == "raw"
    assert cfg["table"] == "clicks"
    assert cfg["credentials_json"] == {"kind": "literal", "value": '{"type": "service_account"}'}


def test_bigquery_source_default_credentials_json_is_empty():
    # Given: a BigQuerySource without explicit credentials_json
    src = BigQuerySource(
        project_id="p", dataset_id="d", table="t",
    )

    # When: calling to_dict
    d = src.to_dict()

    # Then: credentials_json defaults to an empty literal SecretRef
    assert d["config"]["credentials_json"] == {"kind": "literal", "value": ""}


def test_bigquery_source_registers_with_source_decorator():
    # Given: a BigQuerySource attached to a dataset
    bq_src = BigQuerySource(
        project_id="my-project", dataset_id="analytics", table="events",
    )

    @source(bq_src, cursor="event_time", every="10m")
    class BigQueryDataset:
        pass

    # When: reading registered sources
    sources = get_registered_sources()

    # Then: the source is registered with the correct connector type
    assert "BigQueryDataset" in sources
    assert sources["BigQueryDataset"]["connector_type"] == "bigquery"
    assert sources["BigQueryDataset"]["cursor"] == "event_time"
