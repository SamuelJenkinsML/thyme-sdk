import json

from thyme.dataset import get_registered_datasets, serialize_definitions


def test_get_registered_datasets_returns_all_datasets(make_dataset):
    # Given: multiple dataset classes registered
    make_dataset("RegistryDatasetA", index=True, version=1)
    make_dataset("RegistryDatasetB", index=False, version=1)

    # When: we call get_registered_datasets
    datasets = get_registered_datasets()

    # Then: both datasets are present
    assert "RegistryDatasetA" in datasets
    assert "RegistryDatasetB" in datasets
    assert datasets["RegistryDatasetA"]["index"] is True
    assert datasets["RegistryDatasetB"]["index"] is False


def test_get_registered_datasets_returns_copy(sample_dataset):
    # Given: a registered dataset
    # (sample_dataset fixture registers SampleDataset)

    # When: we get the registry and mutate the result
    datasets = get_registered_datasets()
    datasets["SampleDataset"]["version"] = 999

    # Then: the original registry is unchanged
    original = get_registered_datasets()
    assert original["SampleDataset"]["version"] == 1


def test_serialize_definitions_produces_valid_json(sample_dataset):
    # Given: at least one registered dataset
    # (sample_dataset fixture registers SampleDataset)

    # When: we call serialize_definitions
    result = serialize_definitions()

    # Then: the result is valid JSON
    parsed = json.loads(result)
    assert "SampleDataset" in parsed
    assert parsed["SampleDataset"]["version"] == 1
