"""Integration tests for ThymeClient against live services.

These tests require running Thyme services (definition-service + query-server).
Skip in CI unless THYME_INTEGRATION=1 is set.

Run manually:
    THYME_INTEGRATION=1 uv run pytest tests/test_client_integration.py -v
"""

import os

import pytest

from thyme.client import ThymeClient
from thyme.config import Config
from thyme.result import ThymeResult

# Skip entire module unless integration flag is set
pytestmark = pytest.mark.skipif(
    os.environ.get("THYME_INTEGRATION") != "1",
    reason="Integration tests require THYME_INTEGRATION=1 and running services",
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def config():
    """Load config from environment / config files."""
    return Config.load()


@pytest.fixture
def client(config):
    """Create a ThymeClient connected to live services."""
    return ThymeClient(config=config)


# ---------------------------------------------------------------------------
# inspect() integration
# ---------------------------------------------------------------------------


class TestInspectIntegration:
    """Integration tests for inspect() against the live definition-service."""

    def test_inspect_returns_status_dict(self, client):
        # when
        result = client.inspect()

        # then: should have the expected top-level keys
        assert isinstance(result, dict)
        assert "datasets" in result
        assert "featuresets" in result
        assert isinstance(result["datasets"], list)
        assert isinstance(result["featuresets"], list)

    def test_inspect_returns_committed_datasets(self, client):
        # when
        result = client.inspect()

        # then: at least some datasets should be committed in a running system
        # (this test is environment-dependent — it verifies connectivity, not specific data)
        assert isinstance(result.get("datasets"), list)

    def test_inspect_returns_committed_pipelines(self, client):
        # when
        result = client.inspect()

        # then
        assert isinstance(result.get("pipelines"), list)


# ---------------------------------------------------------------------------
# lookup() integration
# ---------------------------------------------------------------------------


class TestLookupIntegration:
    """Integration tests for lookup() against the live query-server.

    These tests verify that lookup() can connect and parse responses.
    They do NOT assert specific feature values since those depend on
    what data has been ingested.
    """

    def test_lookup_missing_entity_returns_empty(self, client):
        """Looking up a non-existent entity should return an empty result, not crash."""
        # Create a minimal dataset class for lookup
        # (we don't need a real @dataset decorator — just _dataset_meta)
        ds = type("TestDataset", (), {})
        ds._dataset_meta = {
            "name": "nonexistent_dataset_12345",
            "version": 1,
            "index": False,
            "fields": [],
            "dependencies": [],
            "expectations": [],
        }

        # when
        result = client.lookup(ds, "nonexistent_entity_id")

        # then: should return a ThymeResult (possibly empty), not raise
        assert isinstance(result, ThymeResult)

    def test_lookup_returns_thyme_result(self, client):
        """Verify lookup returns ThymeResult type for any entity_type."""
        ds = type("AnyDataset", (), {})
        ds._dataset_meta = {
            "name": "any_entity_type",
            "version": 1,
            "index": False,
            "fields": [],
            "dependencies": [],
            "expectations": [],
        }

        result = client.lookup(ds, "any_id")
        assert isinstance(result, ThymeResult)
        assert hasattr(result, "to_polars")
        assert hasattr(result, "to_dict")
