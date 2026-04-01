from datetime import datetime

import pytest

from thyme.connectors import (
    Connector,
    SQLSource,
    IcebergSource,
    KafkaSource,
    PostgresSource,
    S3JsonSource,
    source,
    clear_source_registry,
    get_registered_sources,
)
from thyme.dataset import dataset, field


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


def test_source_disorder_is_stored():
    src = IcebergSource(catalog="c", database="d", table="t")

    @source(src, cursor="ts", every="5m", disorder="1h")
    class Review:
        pass

    sources = get_registered_sources()
    assert sources["Review"]["disorder"] == "1h"


def test_source_disorder_defaults_to_empty():
    src = IcebergSource(catalog="c", database="d", table="t")

    @source(src, cursor="ts")
    class Review:
        pass

    sources = get_registered_sources()
    assert sources["Review"]["disorder"] == ""


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
    assert cfg["password"] == "pw"
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
    assert cfg["sasl_password"] == "pass"
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

    # Then: SASL fields default to empty string
    assert d["config"]["sasl_mechanism"] == ""
    assert d["config"]["sasl_username"] == ""
    assert d["config"]["sasl_password"] == ""


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
    # Given: a KafkaSource attached to a dataset
    kafka_src = KafkaSource(brokers="kafka:9092", topic="raw-events")

    @source(kafka_src, cursor="ts", every="1m", cdc="append")
    class KafkaDataset:
        pass

    # When: reading registered sources
    sources = get_registered_sources()

    # Then: correctly registered
    assert "KafkaDataset" in sources
    assert sources["KafkaDataset"]["connector_type"] == "kafka"
    assert sources["KafkaDataset"]["cursor"] == "ts"


# ---------------------------------------------------------------------------
# Connector ABC tests
# ---------------------------------------------------------------------------


class TestConnectorABC:
    """Given the Connector ABC, subclasses should follow the contract."""

    def test_connector_cannot_be_instantiated(self):
        with pytest.raises(TypeError):
            Connector()

    def test_iceberg_is_connector(self):
        src = IcebergSource(catalog="c", database="d", table="t")
        assert isinstance(src, Connector)

    def test_postgres_is_connector(self):
        src = PostgresSource(host="h", port=5432, database="d", table="t", user="u", password="p")
        assert isinstance(src, Connector)

    def test_s3json_is_connector(self):
        src = S3JsonSource(bucket="b")
        assert isinstance(src, Connector)

    def test_postgres_is_sql_source(self):
        src = PostgresSource(host="h", port=5432, database="d", table="t", user="u", password="p")
        assert isinstance(src, SQLSource)

    def test_iceberg_is_not_sql_source(self):
        src = IcebergSource(catalog="c", database="d", table="t")
        assert not isinstance(src, SQLSource)

    def test_connector_type_iceberg(self):
        assert IcebergSource.connector_type == "iceberg"

    def test_connector_type_postgres(self):
        assert PostgresSource.connector_type == "postgres"

    def test_connector_type_s3json(self):
        assert S3JsonSource.connector_type == "s3json"


# ---------------------------------------------------------------------------
# Constructor validation tests
# ---------------------------------------------------------------------------


class TestConstructorValidation:
    """Given empty required fields, constructors should raise ValueError."""

    def test_iceberg_empty_catalog_raises(self):
        with pytest.raises(ValueError, match="catalog"):
            IcebergSource(catalog="", database="d", table="t")

    def test_iceberg_empty_table_raises(self):
        with pytest.raises(ValueError, match="table"):
            IcebergSource(catalog="c", database="d", table="")

    def test_postgres_empty_host_raises(self):
        with pytest.raises(ValueError, match="host"):
            PostgresSource(host="", port=5432, database="d", table="t", user="u", password="p")

    def test_postgres_empty_password_raises(self):
        with pytest.raises(ValueError, match="password"):
            PostgresSource(host="h", port=5432, database="d", table="t", user="u", password="")

    def test_s3_empty_bucket_raises(self):
        with pytest.raises(ValueError, match="bucket"):
            S3JsonSource(bucket="")

    def test_s3_empty_prefix_is_ok(self):
        # prefix is optional, defaults to ""
        src = S3JsonSource(bucket="b", prefix="")
        assert src.prefix == ""


# ---------------------------------------------------------------------------
# Duration validation tests
# ---------------------------------------------------------------------------


class TestSourceDurationValidation:
    """Given invalid duration strings, @source should raise ValueError."""

    def test_valid_every_accepted(self):
        src = IcebergSource(catalog="c", database="d", table="t")

        @source(src, cursor="ts", every="5m")
        @dataset(version=1)
        class D:
            id: str = field(key=True)
            ts: datetime = field(timestamp=True)

    def test_invalid_every_raises(self):
        src = IcebergSource(catalog="c", database="d", table="t")

        with pytest.raises(ValueError, match="every"):
            @source(src, cursor="ts", every="badvalue")
            @dataset(version=1)
            class D:
                id: str = field(key=True)
                ts: datetime = field(timestamp=True)

    def test_invalid_disorder_raises(self):
        src = IcebergSource(catalog="c", database="d", table="t")

        with pytest.raises(ValueError, match="disorder"):
            @source(src, cursor="ts", disorder="xyz")
            @dataset(version=1)
            class D:
                id: str = field(key=True)
                ts: datetime = field(timestamp=True)

    def test_empty_every_is_ok(self):
        src = IcebergSource(catalog="c", database="d", table="t")

        @source(src, cursor="ts", every="")
        @dataset(version=1)
        class D:
            id: str = field(key=True)
            ts: datetime = field(timestamp=True)


# ---------------------------------------------------------------------------
# Cursor validation tests
# ---------------------------------------------------------------------------


class TestSourceCursorValidation:
    """Given cursor field names, @source should validate against dataset fields."""

    def test_cursor_matches_dataset_field(self):
        src = IcebergSource(catalog="c", database="d", table="t")

        @source(src, cursor="ts")
        @dataset(version=1)
        class D:
            id: str = field(key=True)
            ts: datetime = field(timestamp=True)

    def test_cursor_not_in_dataset_raises(self):
        src = IcebergSource(catalog="c", database="d", table="t")

        with pytest.raises(ValueError, match="nonexistent"):
            @source(src, cursor="nonexistent")
            @dataset(version=1)
            class D:
                id: str = field(key=True)
                ts: datetime = field(timestamp=True)

    def test_empty_cursor_skips_validation(self):
        src = IcebergSource(catalog="c", database="d", table="t")

        @source(src, cursor="")
        @dataset(version=1)
        class D:
            id: str = field(key=True)
            ts: datetime = field(timestamp=True)

    def test_source_without_dataset_skips_cursor_validation(self):
        src = IcebergSource(catalog="c", database="d", table="t")

        @source(src, cursor="whatever")
        class D:
            pass


# ---------------------------------------------------------------------------
# Connector type check tests
# ---------------------------------------------------------------------------


class TestSourceConnectorTypeCheck:
    """Given a non-Connector argument, @source should raise TypeError."""

    def test_non_connector_raises_type_error(self):
        with pytest.raises(TypeError, match="Connector"):
            @source({"not": "a connector"}, cursor="ts")
            class D:
                pass
