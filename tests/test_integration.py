"""Integration test: exercises the full SDK -> compile -> proto round-trip.

This test imports the restaurant review example features, compiles them to
protobuf, serializes, deserializes, and verifies the structure.
"""
import sys
from pathlib import Path

import pytest

from thyme.compiler import (
    compile_commit_request,
    compile_dataset,
    compile_featureset,
    compile_pipeline,
    compile_source,
)
from thyme.dataset import clear_registry, get_registered_datasets
from thyme.featureset import clear_featureset_registry, get_registered_featuresets
from thyme.connectors import clear_source_registry, get_registered_sources
from thyme.gen import services_pb2


@pytest.fixture
def restaurant_reviews_features():
    """Import restaurant review features module and return registry state."""
    clear_registry()
    clear_featureset_registry()
    clear_source_registry()

    examples_dir = str(Path(__file__).parent.parent.parent / "examples")
    if examples_dir not in sys.path:
        sys.path.insert(0, examples_dir)

    import importlib
    mod = importlib.import_module("restaurant_reviews.features")
    importlib.reload(mod)

    datasets = get_registered_datasets()
    featuresets = get_registered_featuresets()
    sources = get_registered_sources()

    yield {
        "datasets": datasets,
        "featuresets": featuresets,
        "sources": sources,
    }

    clear_registry()
    clear_featureset_registry()
    clear_source_registry()


def test_restaurant_reviews_registers_datasets(restaurant_reviews_features):
    datasets = restaurant_reviews_features["datasets"]
    assert "Review" in datasets
    assert "RestaurantRatingStats" in datasets


def test_restaurant_reviews_review_dataset_has_correct_schema(restaurant_reviews_features):
    review = restaurant_reviews_features["datasets"]["Review"]
    field_names = [f["name"] for f in review["fields"]]
    assert "restaurant_id" in field_names
    assert "user_id" in field_names
    assert "rating" in field_names
    assert "review_text" in field_names
    assert "timestamp" in field_names

    key_fields = [f for f in review["fields"] if f.get("key")]
    assert len(key_fields) == 1
    assert key_fields[0]["name"] == "restaurant_id"


def test_restaurant_reviews_registers_featuresets(restaurant_reviews_features):
    featuresets = restaurant_reviews_features["featuresets"]
    assert "RestaurantFeatures" in featuresets

    fs = featuresets["RestaurantFeatures"]
    feature_names = [f["name"] for f in fs["features"]]
    assert "restaurant_id" in feature_names
    assert "avg_rating_30d" in feature_names
    assert "review_count_30d" in feature_names
    assert "is_highly_rated" in feature_names


def test_restaurant_reviews_registers_extractors(restaurant_reviews_features):
    featuresets = restaurant_reviews_features["featuresets"]
    fs = featuresets["RestaurantFeatures"]
    extractor_names = [e["name"] for e in fs["extractors"]]
    assert "get_stats" in extractor_names
    assert "compute_highly_rated" in extractor_names


def test_restaurant_reviews_registers_source(restaurant_reviews_features):
    sources = restaurant_reviews_features["sources"]
    assert "Review" in sources
    assert sources["Review"]["connector_type"] == "iceberg"
    assert sources["Review"]["cursor"] == "timestamp"


def test_restaurant_reviews_compiles_to_proto(restaurant_reviews_features):
    datasets = list(restaurant_reviews_features["datasets"].values())
    featuresets = list(restaurant_reviews_features["featuresets"].values())
    sources = list(restaurant_reviews_features["sources"].values())

    req = compile_commit_request(
        message="restaurant review features",
        datasets=datasets,
        pipelines=[],
        featuresets=featuresets,
        sources=sources,
    )

    assert len(req.datasets) == 2
    assert len(req.featuresets) == 1
    assert len(req.sources) == 1

    serialized = req.SerializeToString()
    assert len(serialized) > 0

    deserialized = services_pb2.CommitRequest()
    deserialized.ParseFromString(serialized)
    assert deserialized.message == "restaurant review features"
    assert len(deserialized.datasets) == 2

    ds_names = {d.name for d in deserialized.datasets}
    assert ds_names == {"Review", "RestaurantRatingStats"}


def test_restaurant_reviews_proto_has_source_config(restaurant_reviews_features):
    sources = list(restaurant_reviews_features["sources"].values())
    req = compile_commit_request(
        message="test",
        datasets=[],
        pipelines=[],
        featuresets=[],
        sources=sources,
    )
    assert req.sources[0].iceberg.catalog == "local_catalog"
    assert req.sources[0].iceberg.database == "restaurant_reviews"
    assert req.sources[0].iceberg.table == "reviews"
