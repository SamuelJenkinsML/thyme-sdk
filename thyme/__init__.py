from thyme.config import Config, PostgresConfig, S3Config, IcebergConfig, KafkaConfig, KinesisConfig, SnowflakeConfig, BigQueryConfig
from thyme.dataset import dataset, field, get_commit_payload, get_registered_datasets, get_registered_pipelines, serialize_definitions
from thyme.pipeline import pipeline, inputs, Avg, Count, Sum, Min, Max, ApproxPercentile
from thyme.featureset import featureset, feature, extractor, extractor_inputs, extractor_outputs
from thyme.connectors import source, BigQuerySource, IcebergSource, KafkaSource, KinesisSource, PostgresSource, S3JsonSource, SnowflakeSource
from thyme.expectations import expectations


def __getattr__(name: str):
    """Lazy imports for Polars-dependent modules to avoid slowing thyme commit."""
    if name == "ThymeResult":
        from thyme.result import ThymeResult
        return ThymeResult
    if name == "ThymeClient":
        from thyme.client import ThymeClient
        return ThymeClient
    raise AttributeError(f"module 'thyme' has no attribute {name!r}")


__all__ = [
    "Config", "PostgresConfig", "S3Config", "IcebergConfig", "KafkaConfig", "KinesisConfig", "SnowflakeConfig", "BigQueryConfig",
    "dataset", "field", "get_commit_payload", "get_registered_datasets", "get_registered_pipelines", "serialize_definitions",
    "pipeline", "inputs", "Avg", "Count", "Sum", "Min", "Max", "ApproxPercentile",
    "featureset", "feature", "extractor", "extractor_inputs", "extractor_outputs",
    "source", "BigQuerySource", "IcebergSource", "KafkaSource", "KinesisSource", "PostgresSource", "S3JsonSource", "SnowflakeSource",
    "expectations",
    "ThymeResult",
    "ThymeClient",
]
