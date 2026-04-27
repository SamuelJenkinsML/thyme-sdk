"""Compiler: converts SDK registry objects to protobuf CommitRequest."""
from typing import List

from thyme.gen import (
    connector_pb2,
    dataset_pb2,
    expr_pb2,
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


_SECRET_KIND_MAP = {
    "literal": connector_pb2.SecretRef.LITERAL,
    "env": connector_pb2.SecretRef.ENV,
    "arn": connector_pb2.SecretRef.ARN,
    "name": connector_pb2.SecretRef.NAME,
}


def _make_secret_ref(value: object) -> connector_pb2.SecretRef:
    """Convert a credential dict (`{"kind": ..., "value": ...}`) to a SecretRef.

    Connector `to_dict()` always emits the tagged dict shape for credential
    fields; plain strings are accepted as a courtesy for legacy callers and
    treated as literals.
    """
    if isinstance(value, dict):
        kind = value.get("kind", "literal")
        v = value.get("value", "")
    else:
        kind = "literal"
        v = value or ""
    proto_kind = _SECRET_KIND_MAP.get(kind, connector_pb2.SecretRef.LITERAL)
    return connector_pb2.SecretRef(kind=proto_kind, value=v)


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
        if "filter" in op:
            spec = op["filter"]
            if "pycode" in spec:
                pc = spec["pycode"]
                operators.append(dataset_pb2.Operator(
                    id=f"filter:{pc.get('entry_point', '')}",
                    filter=dataset_pb2.Filter(
                        pycode=pycode_pb2.PyCode(
                            source_code=pc["source_code"],
                            entry_point=pc["entry_point"],
                            generated_code=pc["source_code"],
                            imports="",
                        ),
                    ),
                ))
            else:
                predicate = spec["predicate"]
                if not isinstance(predicate, expr_pb2.Predicate):
                    raise TypeError(
                        f"filter predicate must be a Predicate proto, got {type(predicate).__name__}"
                    )
                operators.append(dataset_pb2.Operator(
                    id="filter",
                    filter=dataset_pb2.Filter(predicate=predicate),
                ))
        elif "assign" in op:
            spec = op["assign"]
            value = spec["value"]
            if not isinstance(value, expr_pb2.Derivation):
                raise TypeError(
                    f"assign value must be a Derivation proto, got {type(value).__name__}"
                )
            assign_msg = dataset_pb2.Assign(
                column=spec["column"],
                value=value,
            )
            if "dtype" in spec:
                assign_msg.dtype.CopyFrom(spec["dtype"])
            operators.append(dataset_pb2.Operator(
                id=f"assign:{spec['column']}",
                assign=assign_msg,
            ))
        elif "transform" in op:
            spec = op["transform"]
            pc = spec.get("pycode") or {}
            source_code = pc.get("source_code", "")
            entry_point = pc.get("entry_point", "")
            if not source_code or not entry_point:
                raise ValueError(
                    "transform op requires pycode.source_code and pycode.entry_point"
                )
            operators.append(dataset_pb2.Operator(
                id=f"transform:{entry_point}",
                transform=dataset_pb2.Transform(
                    pycode=pycode_pb2.PyCode(
                        source_code=source_code,
                        entry_point=entry_point,
                        generated_code=source_code,
                        imports="",
                    ),
                ),
            ))
        elif "temporal_join" in op:
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
                agg_spec = dataset_pb2.AggSpec(
                    agg_type=s["type"],
                    field=s["field"],
                    window=s["window"],
                    output_field=s["output_field"],
                )
                predicate = s.get("predicate")
                if predicate is not None:
                    if not isinstance(predicate, expr_pb2.Predicate):
                        raise TypeError(
                            f"aggregate predicate must be a Predicate proto, "
                            f"got {type(predicate).__name__}"
                        )
                    agg_spec.predicate.CopyFrom(predicate)
                specs.append(agg_spec)
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


_EXTRACTOR_KIND_MAP = {
    "PY_FUNC": featureset_pb2.PY_FUNC,
    "LOOKUP": featureset_pb2.LOOKUP,
}


def _literal_pb_for_default(value):
    """Lower a Python scalar default value into a Literal proto (the same
    union shape used by expression filters/assigns). Returns None if the
    value is None — proto-side, an absent default means "no default; return
    null on miss"."""
    from thyme.gen import expr_pb2

    if value is None:
        return None
    if isinstance(value, bool):
        return expr_pb2.Literal(bool_value=value)
    if isinstance(value, int):
        return expr_pb2.Literal(int_value=value)
    if isinstance(value, float):
        return expr_pb2.Literal(float_value=value)
    if isinstance(value, str):
        return expr_pb2.Literal(string_value=value)
    raise TypeError(
        f"feature(default=...) only supports str/int/float/bool, got "
        f"{type(value).__name__}"
    )


def compile_featureset(fs_meta: dict) -> featureset_pb2.Featureset:
    features = []
    for f in fs_meta.get("features", []):
        dtype = _type_str_to_proto(f["dtype"])
        if f.get("optional"):
            dtype = schema_pb2.DataType(
                optional_type=schema_pb2.OptionalType(inner=dtype)
            )
        features.append(featureset_pb2.Feature(
            name=f["name"],
            dtype=dtype,
        ))

    extractors = []
    for ext in fs_meta.get("extractors", []):
        pycode = None
        if "source_code" in ext:
            pycode = _make_pycode(ext["source_code"], ext["name"])
        elif ext.get("pycode") and "source_code" in ext["pycode"]:
            pc = ext["pycode"]
            pycode = _make_pycode(pc["source_code"], pc.get("entry_point", ext["name"]))

        kind_str = ext.get("kind", "PY_FUNC")
        kind = _EXTRACTOR_KIND_MAP.get(kind_str, featureset_pb2.PY_FUNC)

        lookup_info_pb = None
        if ext.get("lookup_info"):
            li = ext["lookup_info"]
            lookup_info_pb = featureset_pb2.LookupInfo(
                dataset_name=li["dataset_name"],
                field_name=li["field_name"],
                default=_literal_pb_for_default(li.get("default")),
            )

        extractors.append(featureset_pb2.Extractor(
            name=ext["name"],
            inputs=ext.get("inputs", []),
            outputs=ext.get("outputs", []),
            deps=ext.get("deps", []),
            pycode=pycode,
            version=ext.get("version", 1),
            kind=kind,
            lookup_info=lookup_info_pb,
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
        max_lateness=src_meta.get("max_lateness", ""),
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
            password=_make_secret_ref(config.get("password", "")),
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
            sasl_password=_make_secret_ref(config.get("sasl_password", "")),
            format=config.get("format", "json"),
            group_id=config.get("group_id", ""),
            schema_registry_url=config.get("schema_registry_url", ""),
        ))
    elif connector_type == "kinesis":
        source.kinesis.CopyFrom(connector_pb2.KinesisSource(
            stream_arn=config.get("stream_arn", ""),
            role_arn=_make_secret_ref(config.get("role_arn", "")),
            region=config.get("region", "us-east-1"),
            init_position=config.get("init_position", "latest"),
            format=config.get("format", "json"),
            endpoint_url=config.get("endpoint_url", ""),
        ))
    elif connector_type == "snowflake":
        source.snowflake.CopyFrom(connector_pb2.SnowflakeSource(
            account=config.get("account", ""),
            database=config.get("database", ""),
            schema=config.get("schema", "PUBLIC"),
            warehouse=config.get("warehouse", ""),
            role=config.get("role", ""),
            table=config.get("table", ""),
            user=config.get("user", ""),
            password=_make_secret_ref(config.get("password", "")),
        ))
    elif connector_type == "bigquery":
        source.bigquery.CopyFrom(connector_pb2.BigQuerySource(
            project_id=config.get("project_id", ""),
            dataset_id=config.get("dataset_id", ""),
            table=config.get("table", ""),
            credentials_json=_make_secret_ref(config.get("credentials_json", "")),
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


