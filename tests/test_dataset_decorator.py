from datetime import datetime

from thyme.dataset import get_registered_datasets


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
