"""Compiler: converts SDK registry objects to protobuf CommitRequest."""
from typing import List

from thyme.gen import (
    connector_pb2,
    dataset_pb2,
    featureset_pb2,
    pycode_pb2,
    schema_pb2,
    services_pb2,
)

TYPE_MAP = {
    "int": schema_pb2.DataType(int_type=schema_pb2.IntType()),
    "float": schema_pb2.DataType(float_type=schema_pb2.FloatType()),
    "str": schema_pb2.DataType(string_type=schema_pb2.StringType()),
    "bool": schema_pb2.DataType(bool_type=schema_pb2.BoolType()),
    "datetime": schema_pb2.DataType(timestamp_type=schema_pb2.TimestampType()),
}


def _type_str_to_proto(type_str: str) -> schema_pb2.DataType:
    return TYPE_MAP.get(type_str, schema_pb2.DataType(string_type=schema_pb2.StringType()))


def _make_pycode(source_code: str, entry_point: str = "") -> pycode_pb2.PyCode:
    return pycode_pb2.PyCode(
        entry_point=entry_point,
        source_code=source_code,
        generated_code=source_code,
        imports="",
    )


def compile_expectation(spec: dict) -> dataset_pb2.Expectation:
    kwargs = {
        "type": spec["type"],
        "column": spec["column"],
        "mostly": spec.get("mostly", 1.0),
    }
    if spec.get("min_value") is not None:
        kwargs["min_value"] = spec["min_value"]
    if spec.get("max_value") is not None:
        kwargs["max_value"] = spec["max_value"]
    if spec.get("values"):
        kwargs["values"] = spec["values"]
    if spec.get("type_name") is not None:
        kwargs["type_name"] = spec["type_name"]
    return dataset_pb2.Expectation(**kwargs)


def compile_dataset(ds_meta: dict) -> dataset_pb2.Dataset:
    fields = []
    for f in ds_meta["fields"]:
        dtype = _type_str_to_proto(f["type"])
        if f.get("optional"):
            dtype = schema_pb2.DataType(
                optional_type=schema_pb2.OptionalType(inner=dtype)
            )
        fields.append(schema_pb2.Field(
            name=f["name"],
            dtype=dtype,
            is_key=f.get("key", False),
            is_timestamp=f.get("timestamp", False),
        ))

    expectations = [
        compile_expectation(e) for e in ds_meta.get("expectations", [])
    ]

    return dataset_pb2.Dataset(
        name=ds_meta["name"],
        version=ds_meta["version"],
        schema=schema_pb2.DSSchema(fields=fields),
        indexed=ds_meta.get("index", False),
        expectations=expectations,
    )


def compile_pipeline(pipeline_meta: dict) -> dataset_pb2.Pipeline:
    operators = []
    for op in pipeline_meta.get("operators", []):
        if "temporal_join" in op:
            tj = op["temporal_join"]
            operators.append(dataset_pb2.Operator(
                id="temporal_join",
                temporal_join=dataset_pb2.TemporalJoin(
                    right_dataset=tj.get("right_dataset", ""),
                    left_key_field=tj.get("left_key_field", ""),
                    right_key_field=tj.get("right_key_field", ""),
                    select_fields=tj.get("select_fields", []),
                ),
            ))
        elif "aggregate" in op:
            agg = op["aggregate"]
            specs = []
            for s in agg.get("specs", []):
                specs.append(dataset_pb2.AggSpec(
                    agg_type=s["type"],
                    field=s["field"],
                    window=s["window"],
                    output_field=s["output_field"],
                ))
            operators.append(dataset_pb2.Operator(
                id="aggregate",
                aggregate=dataset_pb2.Aggregate(
                    keys=agg.get("keys", []),
                    specs=specs,
                ),
            ))

    pycode = None
    if "source_code" in pipeline_meta:
        pycode = _make_pycode(pipeline_meta["source_code"], pipeline_meta["name"])

    return dataset_pb2.Pipeline(
        name=pipeline_meta["name"],
        version=pipeline_meta.get("version", 1),
        input_datasets=pipeline_meta.get("input_datasets", []),
        output_dataset=pipeline_meta.get("output_dataset", ""),
        operators=operators,
        pycode=pycode,
    )


def compile_featureset(fs_meta: dict) -> featureset_pb2.Featureset:
    features = []
    for f in fs_meta.get("features", []):
        features.append(featureset_pb2.Feature(
            name=f["name"],
            dtype=_type_str_to_proto(f["dtype"]),
            id=f["id"],
        ))

    extractors = []
    for ext in fs_meta.get("extractors", []):
        pycode = None
        if "source_code" in ext:
            pycode = _make_pycode(ext["source_code"], ext["name"])

        extractors.append(featureset_pb2.Extractor(
            name=ext["name"],
            inputs=ext.get("inputs", []),
            outputs=ext.get("outputs", []),
            deps=ext.get("deps", []),
            pycode=pycode,
            version=ext.get("version", 1),
        ))

    return featureset_pb2.Featureset(
        name=fs_meta["name"],
        features=features,
        extractors=extractors,
    )


def compile_source(src_meta: dict) -> connector_pb2.Source:
    source = connector_pb2.Source(
        dataset=src_meta["dataset"],
        cursor=src_meta.get("cursor", ""),
        every=src_meta.get("every", ""),
        disorder=src_meta.get("disorder", ""),
        cdc=src_meta.get("cdc", "append"),
    )
    config = src_meta.get("config", {})
    connector_type = src_meta.get("connector_type", "")
    if connector_type == "iceberg":
        source.iceberg.CopyFrom(connector_pb2.IcebergSource(
            catalog=config.get("catalog", ""),
            database=config.get("database", ""),
            table=config.get("table", ""),
        ))
    elif connector_type == "postgres":
        source.postgres.CopyFrom(connector_pb2.PostgresSource(
            host=config.get("host", ""),
            port=config.get("port", 5432),
            database=config.get("database", ""),
            table=config.get("table", ""),
            user=config.get("user", ""),
            password=config.get("password", ""),
            schema=config.get("schema", "public"),
            sslmode=config.get("sslmode", "prefer"),
        ))
    elif connector_type == "s3json":
        source.s3json.CopyFrom(connector_pb2.S3JsonSource(
            bucket=config.get("bucket", ""),
            prefix=config.get("prefix", ""),
            region=config.get("region", "us-east-1"),
        ))
    elif connector_type == "kafka":
        source.kafka.CopyFrom(connector_pb2.KafkaSource(
            brokers=config.get("brokers", ""),
            topic=config.get("topic", ""),
            security_protocol=config.get("security_protocol", "PLAINTEXT"),
            sasl_mechanism=config.get("sasl_mechanism", ""),
            sasl_username=config.get("sasl_username", ""),
            sasl_password=config.get("sasl_password", ""),
            format=config.get("format", "json"),
            group_id=config.get("group_id", ""),
            schema_registry_url=config.get("schema_registry_url", ""),
        ))
    elif connector_type == "kinesis":
        source.kinesis.CopyFrom(connector_pb2.KinesisSource(
            stream_arn=config.get("stream_arn", ""),
            role_arn=config.get("role_arn", ""),
            region=config.get("region", "us-east-1"),
            init_position=config.get("init_position", "latest"),
            format=config.get("format", "json"),
        ))
    return source


def compile_commit_request(
    message: str,
    datasets: List[dict],
    pipelines: List[dict],
    featuresets: List[dict],
    sources: List[dict],
) -> services_pb2.CommitRequest:
    return services_pb2.CommitRequest(
        message=message,
        datasets=[compile_dataset(d) for d in datasets],
        pipelines=[compile_pipeline(p) for p in pipelines],
        featuresets=[compile_featureset(f) for f in featuresets],
        sources=[compile_source(s) for s in sources],
    )


