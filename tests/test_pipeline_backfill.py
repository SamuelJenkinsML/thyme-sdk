"""`@pipeline(backfill=...)` — how much history a new pipeline computes (TH-382).

The window is part of the definition, so it arrives with the commit rather than
needing a command afterwards. These tests pin the three things a data scientist
can say, and the wire form each becomes, because the control plane tells "said
nothing" and "said no" apart by that form alone.
"""
from datetime import datetime, timezone

import pytest

from thyme import Backfill, Count, dataset, field, inputs, pipeline
from thyme.compiler import compile_commit_request, compile_pipeline
from thyme.dataset import clear_registry, get_commit_payload

JUNE_1 = datetime(2026, 6, 1, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _clean_registry():
    clear_registry()
    yield
    clear_registry()


def _committed_pipeline(**pipeline_kwargs) -> dict:
    """Define `Order` and `OrderStats.count_orders`, and return the pipeline as
    it goes into the commit payload."""

    @dataset(version=1)
    class Order:
        customer_id: str = field(key=True)
        amount: float = field()
        ts: datetime = field(timestamp=True)

    @dataset(version=1, index=True)
    class OrderStats:
        customer_id: str = field(key=True)
        order_count_1h: int = field()
        ts: datetime = field(timestamp=True)

        @pipeline(version=1, **pipeline_kwargs)
        @inputs(Order)
        def count_orders(cls, orders):
            return orders.groupby("customer_id").aggregate(order_count_1h=Count(window="1h"))

    payload = get_commit_payload()
    (p,) = [p for p in payload["pipelines"] if p["name"] == "count_orders"]
    return p


# ---------------------------------------------------------------------------
# Backfill itself
# ---------------------------------------------------------------------------


def test_backfill_takes_a_timezone_aware_datetime():
    # Given / When a window starting 1 June
    backfill = Backfill(since=JUNE_1)

    # Then it is carried as RFC-3339 in UTC, which the control plane parses
    assert backfill.to_wire() == {"enabled": True, "since": "2026-06-01T00:00:00Z"}


def test_backfill_refuses_a_naive_datetime():
    # A naive datetime is read differently by the machine committing and the
    # engine replaying, which silently moves the window.
    with pytest.raises(ValueError, match="timezone-aware"):
        Backfill(since=datetime(2026, 6, 1))


def test_backfill_refuses_a_relative_duration():
    # "90d" would make the same file mean a different window on every commit,
    # so the same definition would give different feature values depending on
    # when it was deployed.
    with pytest.raises(TypeError, match="datetime"):
        Backfill(since="90d")


# ---------------------------------------------------------------------------
# What a pipeline sends
# ---------------------------------------------------------------------------


def test_a_pipeline_that_says_nothing_backfills_all_retained_history():
    # Given @pipeline(version=1)
    p = _committed_pipeline()

    # Then the payload says: enabled, no start — every row the topic retains
    assert p["backfill"] == {"enabled": True, "since": None}


def test_a_pipeline_can_declare_where_its_history_starts():
    # Given @pipeline(version=1, backfill=Backfill(since=JUNE_1))
    p = _committed_pipeline(backfill=Backfill(since=JUNE_1))

    # Then the window travels with the definition
    assert p["backfill"] == {"enabled": True, "since": "2026-06-01T00:00:00Z"}


def test_a_pipeline_can_opt_out_of_backfilling():
    # Given @pipeline(version=1, backfill=None)
    p = _committed_pipeline(backfill=None)

    # Then it is sent as disabled — not omitted. Omitted means the default,
    # which would backfill a pipeline that explicitly said not to.
    assert p["backfill"] == {"enabled": False, "since": None}


# ---------------------------------------------------------------------------
# The protobuf path, which the CLI tries first
# ---------------------------------------------------------------------------


def test_the_proto_pipeline_carries_the_window():
    p = compile_pipeline(_committed_pipeline(backfill=Backfill(since=JUNE_1)))

    assert p.HasField("backfill")
    assert p.backfill.enabled is True
    assert p.backfill.since == "2026-06-01T00:00:00Z"


def test_the_proto_pipeline_carries_an_opt_out():
    p = compile_pipeline(_committed_pipeline(backfill=None))

    assert p.HasField("backfill")
    assert p.backfill.enabled is False


def test_a_pipeline_dict_without_backfill_sends_no_backfill_message():
    # A payload built by an older SDK has no `backfill` key. Sending no message
    # is how the control plane knows to apply the default.
    p = compile_pipeline(
        {
            "name": "count_orders",
            "version": 1,
            "input_datasets": ["Order"],
            "output_dataset": "OrderStats",
            "operators": [],
        }
    )

    assert not p.HasField("backfill")


def test_a_commit_is_allowed_to_backfill_unless_told_otherwise():
    allowed = compile_commit_request(
        message="", datasets=[], pipelines=[], featuresets=[], sources=[]
    )
    refused = compile_commit_request(
        message="", datasets=[], pipelines=[], featuresets=[], sources=[], backfill=False
    )

    assert allowed.HasField("backfill") and allowed.backfill is True
    assert refused.HasField("backfill") and refused.backfill is False
