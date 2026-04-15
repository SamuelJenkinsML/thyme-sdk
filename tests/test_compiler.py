import pytest

from thyme.compiler import (
    compile_dataset,
    compile_pipeline,
    compile_featureset,
    compile_source,
    compile_commit_request,
)
from thyme.gen import (
    services_pb2,
)


def test_compile_dataset():
    ds_meta = {
        "name": "Review",
        "version": 1,
        "index": True,
        "fields": [
            {"name": "restaurant_id", "type": "str", "key": True},
            {"name": "rating", "type": "float"},
            {"name": "timestamp", "type": "datetime", "timestamp": True},
        ],
        "dependencies": [],
    }
    proto = compile_dataset(ds_meta)
    assert proto.name == "Review"
    assert proto.version == 1
    assert proto.indexed is True
    assert len(proto.schema.fields) == 3
    assert proto.schema.fields[0].name == "restaurant_id"
    assert proto.schema.fields[0].is_key is True
    assert proto.schema.fields[2].is_timestamp is True


def test_compile_pipeline_with_aggregate():
    pipe_meta = {
        "name": "compute_stats",
        "version": 1,
        "input_datasets": ["Review"],
        "output_dataset": "RestaurantRatingStats",
        "source_code": "def compute_stats(cls, reviews): pass",
        "operators": [{
            "aggregate": {
                "keys": ["restaurant_id"],
                "specs": [
                    {"type": "avg", "field": "rating", "window": "30d", "output_field": "avg_rating_30d"},
                    {"type": "count", "field": "", "window": "30d", "output_field": "review_count_30d"},
                ],
            }
        }],
    }
    proto = compile_pipeline(pipe_meta)
    assert proto.name == "compute_stats"
    assert proto.input_datasets == ["Review"]
    assert proto.output_dataset == "RestaurantRatingStats"
    assert len(proto.operators) == 1
    agg = proto.operators[0].aggregate
    assert agg.keys == ["restaurant_id"]
    assert len(agg.specs) == 2
    assert agg.specs[0].agg_type == "avg"
    assert agg.specs[0].field == "rating"
    assert agg.specs[0].window == "30d"
    assert proto.pycode.entry_point == "compute_stats"


def test_compile_featureset():
    fs_meta = {
        "name": "RestaurantFeatures",
        "features": [
            {"name": "restaurant_id", "dtype": "str", "id": 1},
            {"name": "avg_rating_30d", "dtype": "float", "id": 2},
            {"name": "is_highly_rated", "dtype": "bool", "id": 3},
        ],
        "extractors": [{
            "name": "compute_highly_rated",
            "inputs": ["avg_rating_30d"],
            "outputs": ["is_highly_rated"],
            "deps": [],
            "source_code": "def compute_highly_rated(cls, ts, ratings): return ratings > 4.0",
            "version": 1,
        }],
    }
    proto = compile_featureset(fs_meta)
    assert proto.name == "RestaurantFeatures"
    assert len(proto.features) == 3
    assert proto.features[0].name == "restaurant_id"
    assert proto.features[0].id == 1
    assert len(proto.extractors) == 1
    assert proto.extractors[0].name == "compute_highly_rated"
    assert proto.extractors[0].pycode.entry_point == "compute_highly_rated"


def test_compile_source():
    src_meta = {
        "dataset": "Review",
        "connector_type": "iceberg",
        "config": {
            "catalog": "local",
            "database": "restaurant_reviews",
            "table": "reviews",
        },
        "cursor": "timestamp",
        "every": "1m",
    }
    proto = compile_source(src_meta)
    assert proto.dataset == "Review"
    assert proto.cursor == "timestamp"
    assert proto.every == "1m"
    assert proto.iceberg.catalog == "local"
    assert proto.iceberg.database == "restaurant_reviews"
    assert proto.iceberg.table == "reviews"


def test_compile_commit_request():
    req = compile_commit_request(
        message="initial commit",
        datasets=[{
            "name": "Review",
            "version": 1,
            "index": False,
            "fields": [
                {"name": "id", "type": "str", "key": True},
                {"name": "ts", "type": "datetime", "timestamp": True},
            ],
        }],
        pipelines=[],
        featuresets=[],
        sources=[],
    )
    assert isinstance(req, services_pb2.CommitRequest)
    assert req.message == "initial commit"
    assert len(req.datasets) == 1
    assert req.datasets[0].name == "Review"


def test_compile_source_cdc_field_included():
    src_meta = {
        "dataset": "Review",
        "connector_type": "iceberg",
        "config": {"catalog": "local", "database": "db", "table": "reviews"},
        "cursor": "timestamp",
        "every": "1m",
        "cdc": "upsert",
    }
    proto = compile_source(src_meta)
    assert proto.cdc == "upsert"


def test_compile_source_cdc_defaults_to_append():
    src_meta = {
        "dataset": "Review",
        "connector_type": "iceberg",
        "config": {"catalog": "local", "database": "db", "table": "reviews"},
        "cursor": "timestamp",
        "every": "1m",
    }
    proto = compile_source(src_meta)
    assert proto.cdc == "append"


def test_compile_source_kafka():
    # Given: a Kafka source metadata dict
    src_meta = {
        "dataset": "RawEvents",
        "connector_type": "kafka",
        "config": {
            "brokers": "kafka:9092,kafka:9093",
            "topic": "raw-events",
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_username": "user",
            "sasl_password": "pass",
            "format": "json",
            "group_id": "my-group",
            "schema_registry_url": "http://registry:8081",
        },
        "cursor": "event_time",
        "every": "1m",
        "disorder": "5m",
        "cdc": "append",
    }

    # When: compiling to proto
    proto = compile_source(src_meta)

    # Then: proto fields are populated correctly
    assert proto.dataset == "RawEvents"
    assert proto.cursor == "event_time"
    assert proto.every == "1m"
    assert proto.disorder == "5m"
    assert proto.cdc == "append"
    assert proto.kafka.brokers == "kafka:9092,kafka:9093"
    assert proto.kafka.topic == "raw-events"
    assert proto.kafka.security_protocol == "SASL_SSL"
    assert proto.kafka.sasl_mechanism == "PLAIN"
    assert proto.kafka.sasl_username == "user"
    assert proto.kafka.sasl_password == "pass"
    assert proto.kafka.format == "json"
    assert proto.kafka.group_id == "my-group"
    assert proto.kafka.schema_registry_url == "http://registry:8081"


def test_compile_source_kinesis():
    # Given: a Kinesis source metadata dict
    src_meta = {
        "dataset": "ClickEvents",
        "connector_type": "kinesis",
        "config": {
            "stream_arn": "arn:aws:kinesis:us-east-1:123456789:stream/clicks",
            "role_arn": "arn:aws:iam::123456789:role/kinesis-reader",
            "region": "us-east-1",
            "init_position": "trim_horizon",
            "format": "json",
        },
        "cursor": "",
        "every": "",
        "disorder": "5m",
        "cdc": "append",
    }

    # When: compiling to proto
    proto = compile_source(src_meta)

    # Then: proto fields are populated correctly
    assert proto.dataset == "ClickEvents"
    assert proto.cursor == ""
    assert proto.every == ""
    assert proto.disorder == "5m"
    assert proto.cdc == "append"
    assert proto.kinesis.stream_arn == "arn:aws:kinesis:us-east-1:123456789:stream/clicks"
    assert proto.kinesis.role_arn == "arn:aws:iam::123456789:role/kinesis-reader"
    assert proto.kinesis.region == "us-east-1"
    assert proto.kinesis.init_position == "trim_horizon"
    assert proto.kinesis.format == "json"


def test_compile_source_kinesis_defaults():
    # Given: a minimal Kinesis source metadata dict
    src_meta = {
        "dataset": "Events",
        "connector_type": "kinesis",
        "config": {
            "stream_arn": "arn:aws:kinesis:us-east-1:123:stream/s",
        },
    }

    # When: compiling to proto
    proto = compile_source(src_meta)

    # Then: defaults are applied
    assert proto.kinesis.region == "us-east-1"
    assert proto.kinesis.init_position == "latest"
    assert proto.kinesis.format == "json"
    assert proto.kinesis.role_arn == ""


def test_compile_source_snowflake():
    # Given: a Snowflake source metadata dict
    src_meta = {
        "dataset": "Orders",
        "connector_type": "snowflake",
        "config": {
            "account": "xy12345.us-east-1",
            "database": "analytics",
            "schema": "RAW",
            "warehouse": "etl_wh",
            "role": "loader",
            "table": "orders",
            "user": "etl_user",
            "password": "secret",
        },
        "cursor": "updated_at",
        "every": "5m",
        "disorder": "1h",
        "cdc": "append",
    }

    # When: compiling to proto
    proto = compile_source(src_meta)

    # Then: proto fields are populated correctly
    assert proto.dataset == "Orders"
    assert proto.cursor == "updated_at"
    assert proto.every == "5m"
    assert proto.disorder == "1h"
    assert proto.cdc == "append"
    assert proto.snowflake.account == "xy12345.us-east-1"
    assert proto.snowflake.database == "analytics"
    assert proto.snowflake.schema == "RAW"
    assert proto.snowflake.warehouse == "etl_wh"
    assert proto.snowflake.role == "loader"
    assert proto.snowflake.table == "orders"
    assert proto.snowflake.user == "etl_user"
    assert proto.snowflake.password == "secret"


def test_compile_source_bigquery():
    # Given: a BigQuery source metadata dict
    src_meta = {
        "dataset": "ClickEvents",
        "connector_type": "bigquery",
        "config": {
            "project_id": "my-project",
            "dataset_id": "raw",
            "table": "clicks",
            "credentials_json": '{"type": "service_account"}',
        },
        "cursor": "event_time",
        "every": "10m",
        "disorder": "",
        "cdc": "append",
    }

    # When: compiling to proto
    proto = compile_source(src_meta)

    # Then: proto fields are populated correctly
    assert proto.dataset == "ClickEvents"
    assert proto.cursor == "event_time"
    assert proto.every == "10m"
    assert proto.cdc == "append"
    assert proto.bigquery.project_id == "my-project"
    assert proto.bigquery.dataset_id == "raw"
    assert proto.bigquery.table == "clicks"
    assert proto.bigquery.credentials_json == '{"type": "service_account"}'


def test_compile_dataset_with_expectations():
    ds_meta = {
        "name": "Review",
        "version": 1,
        "index": True,
        "fields": [
            {"name": "restaurant_id", "type": "str", "key": True},
            {"name": "rating", "type": "float"},
            {"name": "timestamp", "type": "datetime", "timestamp": True},
        ],
        "dependencies": [],
        "expectations": [
            {
                "type": "column_values_between",
                "column": "rating",
                "min_value": 1.0,
                "max_value": 5.0,
                "mostly": 0.95,
            },
            {
                "type": "column_values_not_null",
                "column": "restaurant_id",
                "mostly": 1.0,
            },
        ],
    }
    proto = compile_dataset(ds_meta)
    assert len(proto.expectations) == 2
    exp0 = proto.expectations[0]
    assert exp0.type == "column_values_between"
    assert exp0.column == "rating"
    assert exp0.min_value == pytest.approx(1.0)
    assert exp0.max_value == pytest.approx(5.0)
    assert exp0.mostly == pytest.approx(0.95)
    exp1 = proto.expectations[1]
    assert exp1.type == "column_values_not_null"
    assert exp1.column == "restaurant_id"
    assert exp1.mostly == pytest.approx(1.0)


def test_compile_dataset_no_expectations():
    ds_meta = {
        "name": "Review",
        "version": 1,
        "index": False,
        "fields": [
            {"name": "id", "type": "str", "key": True},
            {"name": "ts", "type": "datetime", "timestamp": True},
        ],
        "dependencies": [],
        "expectations": [],
    }
    proto = compile_dataset(ds_meta)
    assert len(proto.expectations) == 0


def test_compile_roundtrip_serialization():
    req = compile_commit_request(
        message="test",
        datasets=[{
            "name": "Test",
            "version": 1,
            "fields": [
                {"name": "k", "type": "int", "key": True},
                {"name": "t", "type": "datetime", "timestamp": True},
            ],
        }],
        pipelines=[],
        featuresets=[],
        sources=[],
    )
    serialized = req.SerializeToString()
    deserialized = services_pb2.CommitRequest()
    deserialized.ParseFromString(serialized)
    assert deserialized.message == "test"
    assert deserialized.datasets[0].name == "Test"


def test_compile_extractor_preserves_nested_pycode():
    """Given an extractor with pycode nested under ext["pycode"]["source_code"]
    (the actual SDK format), compile_commit_request should preserve it."""
    featuresets = [{
        "name": "TestFS",
        "features": [{"name": "x", "dtype": "float", "id": 1}],
        "extractors": [{
            "name": "compute_x",
            "inputs": ["raw_x"],
            "outputs": ["x"],
            "deps": [],
            "version": 1,
            "pycode": {
                "entry_point": "compute_x",
                "source_code": "def compute_x(cls, ts, raw_x): return raw_x * 2",
                "generated_code": "",
                "imports": "",
            },
        }],
    }]
    msg = compile_commit_request(
        "", datasets=[], pipelines=[], featuresets=featuresets, sources=[],
    )
    ext = msg.featuresets[0].extractors[0]
    assert ext.pycode is not None
    assert "compute_x" in ext.pycode.source_code
