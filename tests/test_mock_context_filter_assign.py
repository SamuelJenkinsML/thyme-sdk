"""MockContext parity for pre-aggregate filter/assign operators."""

from datetime import datetime, timedelta, timezone

import pytest

from thyme import Count, Sum, dataset, field, inputs, pipeline
from thyme.expr import col
from thyme.testing import MockContext


@pytest.fixture(autouse=True)
def _clean_registry():
    """Isolate each test: clean global registries before and after."""
    from thyme.dataset import _DATASET_REGISTRY, _PIPELINE_REGISTRY
    _DATASET_REGISTRY.clear()
    _PIPELINE_REGISTRY.clear()
    yield
    _DATASET_REGISTRY.clear()
    _PIPELINE_REGISTRY.clear()


def _ts(minutes_ago: int) -> str:
    now = datetime(2026, 4, 22, 12, 0, 0, tzinfo=timezone.utc)
    return (now - timedelta(minutes=minutes_ago)).isoformat().replace("+00:00", "Z")


class TestFilter:
    def test_filter_drops_matching_events(self):
        # Given a dataset with a pipeline that filters amount > 0
        @dataset(version=1)
        class Order:
            user_id: str = field(key=True)
            amount: float = field()
            timestamp: datetime = field(timestamp=True)

        @dataset(version=1, index=True)
        class OrderStats:
            user_id: str = field(key=True)
            count_1h: int = field()
            timestamp: datetime = field(timestamp=True)

            @pipeline(version=1)
            @inputs(Order)
            def compute(cls, orders):
                return (
                    orders.filter(col("amount") > 0)
                    .groupby("user_id")
                    .aggregate(count_1h=Count(window="1h"))
                )

        ctx = MockContext()
        ctx.add_events(
            Order,
            [
                {"user_id": "u1", "amount": 10.0, "timestamp": _ts(5)},
                {"user_id": "u1", "amount": 0.0, "timestamp": _ts(10)},    # filtered out
                {"user_id": "u1", "amount": 20.0, "timestamp": _ts(15)},
                {"user_id": "u1", "amount": -5.0, "timestamp": _ts(20)},   # filtered out
            ],
        )

        # Then only 2 of 4 events contribute to the count
        agg = ctx.get_aggregates(OrderStats, "u1")
        assert agg["count_1h"] == 2.0

    def test_filter_compound_predicate(self):
        # Given a predicate combining two columns with AND
        @dataset(version=1)
        class Event:
            user_id: str = field(key=True)
            amount: float = field()
            kind: str = field()
            timestamp: datetime = field(timestamp=True)

        @dataset(version=1, index=True)
        class EventStats:
            user_id: str = field(key=True)
            count_1h: int = field()
            timestamp: datetime = field(timestamp=True)

            @pipeline(version=1)
            @inputs(Event)
            def compute(cls, events):
                return (
                    events.filter((col("amount") > 0) & (col("kind") != "test"))
                    .groupby("user_id")
                    .aggregate(count_1h=Count(window="1h"))
                )

        ctx = MockContext()
        ctx.add_events(
            Event,
            [
                {"user_id": "u1", "amount": 10.0, "kind": "real", "timestamp": _ts(5)},
                {"user_id": "u1", "amount": 20.0, "kind": "test", "timestamp": _ts(10)},  # kind=test drops
                {"user_id": "u1", "amount": 0.0, "kind": "real", "timestamp": _ts(15)},   # amount<=0 drops
                {"user_id": "u1", "amount": 5.0, "kind": "real", "timestamp": _ts(20)},
            ],
        )
        agg = ctx.get_aggregates(EventStats, "u1")
        assert agg["count_1h"] == 2.0


class TestAssign:
    def test_assign_makes_derived_column_available_to_aggregate(self):
        @dataset(version=1)
        class FxOrder:
            user_id: str = field(key=True)
            amount: float = field()
            rate: float = field()
            timestamp: datetime = field(timestamp=True)

        @dataset(version=1, index=True)
        class FxOrderStats:
            user_id: str = field(key=True)
            total_usd_1h: float = field()
            timestamp: datetime = field(timestamp=True)

            @pipeline(version=1)
            @inputs(FxOrder)
            def compute(cls, orders):
                return (
                    orders.assign(amount_usd=col("amount") * col("rate"))
                    .groupby("user_id")
                    .aggregate(total_usd_1h=Sum(of="amount_usd", window="1h"))
                )

        ctx = MockContext()
        ctx.add_events(
            FxOrder,
            [
                {"user_id": "u1", "amount": 10.0, "rate": 1.5, "timestamp": _ts(5)},
                {"user_id": "u1", "amount": 20.0, "rate": 2.0, "timestamp": _ts(15)},
            ],
        )
        # Expected: 10*1.5 + 20*2.0 = 55.0
        agg = ctx.get_aggregates(FxOrderStats, "u1")
        assert agg["total_usd_1h"] == pytest.approx(55.0)


class TestFilterThenAssign:
    def test_filter_before_assign_chain(self):
        @dataset(version=1)
        class Txn:
            user_id: str = field(key=True)
            amount: float = field()
            rate: float = field()
            timestamp: datetime = field(timestamp=True)

        @dataset(version=1, index=True)
        class TxnStats:
            user_id: str = field(key=True)
            total_1h: float = field()
            timestamp: datetime = field(timestamp=True)

            @pipeline(version=1)
            @inputs(Txn)
            def compute(cls, txns):
                return (
                    txns.filter(col("amount") > 0)
                    .assign(usd=col("amount") * col("rate"))
                    .groupby("user_id")
                    .aggregate(total_1h=Sum(of="usd", window="1h"))
                )

        ctx = MockContext()
        ctx.add_events(
            Txn,
            [
                {"user_id": "u1", "amount": 10.0, "rate": 1.5, "timestamp": _ts(5)},
                {"user_id": "u1", "amount": -5.0, "rate": 2.0, "timestamp": _ts(10)},  # filtered
                {"user_id": "u1", "amount": 20.0, "rate": 1.0, "timestamp": _ts(15)},
            ],
        )
        agg = ctx.get_aggregates(TxnStats, "u1")
        # 10 * 1.5 + 20 * 1.0 = 35.0
        assert agg["total_1h"] == pytest.approx(35.0)
