from datetime import datetime
from typing import Optional

import pytest

from thyme.dataset import dataset, field


def test_raises_when_multiple_key_fields():
    # Given: a dataset class with two key fields
    # When: the class is defined
    # Then: ValueError is raised
    with pytest.raises(ValueError, match="exactly one key field"):

        @dataset(index=True, version=1)
        class MultipleKeyDataset:
            user_id: int = field(key=True)
            order_id: int = field(key=True)
            ts: datetime = field(timestamp=True)


def test_raises_when_no_key_field():
    # Given: a dataset class with no key field
    # When: the class is defined
    # Then: ValueError is raised
    with pytest.raises(ValueError, match="exactly one key field"):

        @dataset(index=True, version=1)
        class NoKeyDataset:
            user_id: int = field()
            amount: float = field()
            ts: datetime = field(timestamp=True)


def test_raises_when_multiple_timestamp_fields():
    # Given: a dataset class with two timestamp fields
    # When: the class is defined
    # Then: ValueError is raised
    with pytest.raises(ValueError, match="exactly one timestamp field"):

        @dataset(index=True, version=1)
        class MultipleTimestampDataset:
            user_id: int = field(key=True)
            created: datetime = field(timestamp=True)
            updated: datetime = field(timestamp=True)


def test_raises_when_no_timestamp_field():
    # Given: a dataset class with no timestamp field
    # When: the class is defined
    # Then: ValueError is raised
    with pytest.raises(ValueError, match="exactly one timestamp field"):

        @dataset(index=True, version=1)
        class NoTimestampDataset:
            user_id: int = field(key=True)
            amount: float = field()


def test_raises_when_key_field_is_optional():
    # Given: a dataset class where the key field is Optional
    # When: the class is defined
    # Then: ValueError is raised
    with pytest.raises(ValueError, match="cannot be Optional"):

        @dataset(index=True, version=1)
        class OptionalKeyDataset:
            user_id: Optional[int] = field(key=True)
            amount: float = field()
            ts: datetime = field(timestamp=True)
