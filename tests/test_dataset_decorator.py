from datetime import datetime

import pytest

from thyme.dataset import dataset, field, get_registered_datasets


def test_dataset_registers_with_correct_schema(make_dataset):
    # Given: a dataset class with key, timestamp, and regular fields
    # When: the class is defined via the factory
    make_dataset("SampleDataset", index=True, version=2)

    # Then: it appears in the registry with correct schema
    datasets = get_registered_datasets()
    assert "SampleDataset" in datasets
    meta = datasets["SampleDataset"]
    assert meta["version"] == 2
    assert meta["index"] is True
    assert len([f for f in meta["fields"] if f.get("key")]) == 1
    assert len([f for f in meta["fields"] if f.get("timestamp")]) == 1


def test_dataset_class_remains_instantiable(make_dataset):
    # Given: a dataset class with required fields
    InstantiableDataset = make_dataset("InstantiableDataset", index=False, version=1)

    # When: we instantiate it with values
    instance = InstantiableDataset(
        id=1, value=10.0, ts=datetime.now()
    )

    # Then: the instance has the expected attributes
    assert instance.id == 1
    assert instance.value == 10.0


def test_dataset_schema_has_expected_structure(make_dataset):
    # Given: a dataset class
    # When: we create it and read its schema from the registry
    make_dataset("SchemaCheckDataset", index=True, version=3)
    meta = get_registered_datasets()["SchemaCheckDataset"]

    # Then: schema has name, version, index, fields, dependencies
    assert meta["name"] == "SchemaCheckDataset"
    assert meta["version"] == 3
    assert meta["index"] is True
    assert meta["dependencies"] == []

    field_names = [f["name"] for f in meta["fields"]]
    assert "id" in field_names
    assert "value" in field_names
    assert "ts" in field_names


# ---------------------------------------------------------------------------
# Catalog metadata kwargs (TH-CAT-A1)
# ---------------------------------------------------------------------------


def _make_dataset_with(**dataset_kwargs):
    """Build a minimal dataset class through the @dataset decorator with the
    given kwargs. Returns the decorated class."""
    namespace = {
        "__annotations__": {"id": int, "value": float, "ts": datetime},
        "id": field(key=True),
        "value": field(),
        "ts": field(timestamp=True),
    }
    cls = type("MetaDataset", (), namespace)
    return dataset(**dataset_kwargs)(cls)


def test_dataset_default_kwargs_attach_empty_metadata():
    from thyme.metadata import EntityMetadata

    cls = _make_dataset_with(index=True, version=1)
    assert cls.__thyme_metadata__ == EntityMetadata()


def test_dataset_with_metadata_kwargs_populates():
    cls = _make_dataset_with(
        index=True,
        version=2,
        owner="data-eng@thyme.io",
        tags=["raw", "events"],
        description="Raw event log",
        project="ingestion",
    )
    md = cls.__thyme_metadata__
    assert md.owner == "data-eng@thyme.io"
    assert md.tags == {"raw": "", "events": ""}
    assert md.description == "Raw event log"
    assert md.project == "ingestion"
    # Existing schema kwargs still applied
    meta = get_registered_datasets()["MetaDataset"]
    assert meta["version"] == 2
    assert meta["index"] is True


def test_dataset_unknown_kwarg_warns_but_registers():
    with pytest.warns(FutureWarning, match="future_kwarg"):
        cls = _make_dataset_with(index=True, version=1, future_kwarg="y")
    assert "MetaDataset" in get_registered_datasets()
    assert cls.__thyme_metadata__ == cls.__thyme_metadata__  # attribute exists


# ---------------------------------------------------------------------------
# Retention — backfill depth for the dataset's topic (TH-183)
# ---------------------------------------------------------------------------


def test_dataset_retention_lands_in_schema():
    # Given / When: a dataset declares how far back its history is kept
    _make_dataset_with(index=True, version=1, retention="180d")

    # Then: the value rides in the schema the commit payload is built from
    meta = get_registered_datasets()["MetaDataset"]
    assert meta["retention"] == "180d"


def test_dataset_retention_is_an_explicit_param_not_an_unknown_kwarg():
    # Given / When: retention is passed
    # Then: no FutureWarning — it must be a real parameter, not swept into
    # **kwargs and dropped by _build_metadata (the failure mode this guards).
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("error", FutureWarning)
        _make_dataset_with(index=True, version=1, retention="30d")


def test_dataset_without_retention_omits_the_key():
    # Given / When: no retention is declared
    _make_dataset_with(index=True, version=1)

    # Then: the key is absent rather than None, so the server can distinguish
    # "not specified" (use the deployment default) from an explicit value.
    meta = get_registered_datasets()["MetaDataset"]
    assert "retention" not in meta


@pytest.mark.parametrize("bad", ["30 fortnights", "forever", "d30", "", "0d"])
def test_dataset_rejects_unparseable_retention(bad):
    # Given / When: a retention that no duration parser can read
    # Then: it fails at decoration time, not silently at topic-creation time.
    # The server turns this into retention.ms; a value that parses to 0 would
    # delete every sealed segment on the next retention sweep.
    with pytest.raises(ValueError, match="retention"):
        _make_dataset_with(index=True, version=1, retention=bad)


@pytest.mark.parametrize("good", ["30d", "12h", "90m", "3600s", "365d"])
def test_dataset_accepts_supported_duration_units(good):
    # Given / When: any unit the engine's duration parser understands
    _make_dataset_with(index=True, version=1, retention=good)

    # Then: it round-trips verbatim — the server owns the conversion to ms, so
    # the SDK must not normalise and lose the author's intent.
    assert get_registered_datasets()["MetaDataset"]["retention"] == good
