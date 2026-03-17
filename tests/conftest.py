from datetime import datetime

import pytest

from thyme.dataset import clear_registry, dataset, field
from thyme.featureset import clear_featureset_registry
from thyme.connectors import clear_source_registry


@pytest.fixture(autouse=True)
def isolated_registry():
    """Clear all registries before each test for isolation."""
    clear_registry()
    clear_featureset_registry()
    clear_source_registry()
    yield
    clear_registry()
    clear_featureset_registry()
    clear_source_registry()


@pytest.fixture
def make_dataset():
    """Factory fixture to create and register a valid dataset class with a given name."""

    def _make(
        name: str,
        index: bool = True,
        version: int = 1,
    ):
        namespace = {
            "__annotations__": {"id": int, "value": float, "ts": datetime},
            "id": field(key=True),
            "value": field(),
            "ts": field(timestamp=True),
        }
        cls = type(name, (), namespace)
        return dataset(index=index, version=version)(cls)

    return _make


@pytest.fixture
def sample_dataset(make_dataset):
    """A pre-registered valid dataset for tests that need one in the registry."""
    return make_dataset("SampleDataset", index=True, version=1)


# ---------------------------------------------------------------------------
# Parity metrics — populated by test_parity_online_offline, rendered at end
# ---------------------------------------------------------------------------


def pytest_configure(config):
    config.parity_results = []


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    results = getattr(config, "parity_results", [])
    if not results:
        return

    from rich.console import Console
    from rich.table import Table

    matched = sum(1 for r in results if r["match"])
    total = len(results)

    table = Table(
        title=f"Online/Offline Parity  [{matched}/{total} entities matched]",
        show_lines=True,
    )
    table.add_column("Entity", style="cyan")
    table.add_column("Feature", style="white")
    table.add_column("Online", justify="right")
    table.add_column("Offline", justify="right")
    table.add_column("Match", justify="center")

    for r in results:
        all_keys = sorted(set(r["online"].keys()) | set(r["offline"].keys()))
        first = True
        for key in all_keys:
            online_val = r["online"].get(key, "—")
            offline_val = r["offline"].get(key, "—")
            field_match = online_val == offline_val
            match_icon = "[green]✓[/green]" if field_match else "[red]✗[/red]"
            table.add_row(
                r["entity_id"] if first else "",
                key,
                str(online_val),
                str(offline_val),
                match_icon,
            )
            first = False

    Console().print()
    Console().print(table)
