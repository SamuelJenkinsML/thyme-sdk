"""Tests for thyme.testing module — unit-testable pipelines without infrastructure.

TDD: these tests are written first, then the implementation makes them pass.
"""

from datetime import datetime, timedelta, timezone

import pytest

from thyme import (
    Avg,
    Count,
    Max,
    Min,
    Sum,
    dataset,
    expectations,
    extractor,
    extractor_inputs,
    extractor_outputs,
    feature,
    featureset,
    field,
    inputs,
    pipeline,
)
from thyme.expectations import expect_column_values_to_be_between, expect_column_values_to_not_be_null
from thyme.testing import MockContext
from thyme.testing._window import parse_window_duration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_source():
    """Create a fresh source dataset (must be called inside each test after registry clear)."""
    @dataset(version=1)
    class Source:
        entity_id: str = field(key=True)
        value: float = field()
        timestamp: datetime = field(timestamp=True)
    return Source


# ---------------------------------------------------------------------------
# Window duration parsing
# ---------------------------------------------------------------------------


class TestWindowParsing:
    def test_parse_days(self):
        assert parse_window_duration("30d") == 30 * 86400

    def test_parse_hours(self):
        assert parse_window_duration("1h") == 3600

    def test_parse_minutes(self):
        assert parse_window_duration("90m") == 5400

    def test_parse_seconds(self):
        assert parse_window_duration("30s") == 30

    def test_parse_empty_returns_zero(self):
        assert parse_window_duration("") == 0

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError):
            parse_window_duration("abc")

    def test_missing_unit_raises(self):
        with pytest.raises(ValueError):
            parse_window_duration("30")


# ---------------------------------------------------------------------------
# Aggregation — individual operators
# ---------------------------------------------------------------------------


class TestCountAggregation:
    def test_count_within_window(self):
        # Given
        Source = _make_source()

        @dataset(version=1, index=True)
        class CountResult:
            entity_id: str = field(key=True)
            cnt: float = field()
            timestamp: datetime = field(timestamp=True)

            @pipeline(version=1)
            @inputs(Source)
            def count_pipe(cls, src):
                return src.groupby("entity_id").aggregate(cnt=Count(window="7d"))

        now = datetime.now(timezone.utc)
        events = [
            {"entity_id": "e1", "value": 1.0, "timestamp": now.isoformat()},
            {"entity_id": "e1", "value": 2.0, "timestamp": (now - timedelta(days=1)).isoformat()},
            {"entity_id": "e1", "value": 3.0, "timestamp": (now - timedelta(days=3)).isoformat()},
        ]

        # When
        ctx = MockContext()
        ctx.add_events(Source, events)
        result = ctx.get_aggregates(CountResult, "e1")

        # Then
        assert result["cnt"] == 3.0

    def test_count_excludes_expired_events(self):
        Source = _make_source()

        @dataset(version=1, index=True)
        class CountExpired:
            entity_id: str = field(key=True)
            cnt: float = field()
            timestamp: datetime = field(timestamp=True)

            @pipeline(version=1)
            @inputs(Source)
            def count_pipe(cls, src):
                return src.groupby("entity_id").aggregate(cnt=Count(window="7d"))

        now = datetime.now(timezone.utc)
        events = [
            {"entity_id": "e1", "value": 1.0, "timestamp": now.isoformat()},
            {"entity_id": "e1", "value": 2.0, "timestamp": (now - timedelta(days=10)).isoformat()},
        ]

        ctx = MockContext()
        ctx.add_events(Source, events)
        result = ctx.get_aggregates(CountExpired, "e1")

        # Only 1 event within window
        assert result["cnt"] == 1.0


class TestAvgAggregation:
    def test_avg_within_window(self):
        Source = _make_source()

        @dataset(version=1, index=True)
        class AvgResult:
            entity_id: str = field(key=True)
            avg_val: float = field()
            timestamp: datetime = field(timestamp=True)

            @pipeline(version=1)
            @inputs(Source)
            def avg_pipe(cls, src):
                return src.groupby("entity_id").aggregate(avg_val=Avg(of="value", window="30d"))

        now = datetime.now(timezone.utc)
        events = [
            {"entity_id": "e1", "value": 4.0, "timestamp": now.isoformat()},
            {"entity_id": "e1", "value": 6.0, "timestamp": (now - timedelta(days=1)).isoformat()},
        ]

        ctx = MockContext()
        ctx.add_events(Source, events)
        result = ctx.get_aggregates(AvgResult, "e1")

        assert result["avg_val"] == pytest.approx(5.0)


class TestSumAggregation:
    def test_sum_within_window(self):
        Source = _make_source()

        @dataset(version=1, index=True)
        class SumResult:
            entity_id: str = field(key=True)
            total: float = field()
            timestamp: datetime = field(timestamp=True)

            @pipeline(version=1)
            @inputs(Source)
            def sum_pipe(cls, src):
                return src.groupby("entity_id").aggregate(total=Sum(of="value", window="30d"))

        now = datetime.now(timezone.utc)
        events = [
            {"entity_id": "e1", "value": 10.0, "timestamp": now.isoformat()},
            {"entity_id": "e1", "value": 20.0, "timestamp": (now - timedelta(days=1)).isoformat()},
        ]

        ctx = MockContext()
        ctx.add_events(Source, events)
        result = ctx.get_aggregates(SumResult, "e1")

        assert result["total"] == pytest.approx(30.0)


class TestMinMaxAggregation:
    def test_min_within_window(self):
        Source = _make_source()

        @dataset(version=1, index=True)
        class MinResult:
            entity_id: str = field(key=True)
            min_val: float = field()
            timestamp: datetime = field(timestamp=True)

            @pipeline(version=1)
            @inputs(Source)
            def min_pipe(cls, src):
                return src.groupby("entity_id").aggregate(min_val=Min(of="value", window="30d"))

        now = datetime.now(timezone.utc)
        events = [
            {"entity_id": "e1", "value": 3.0, "timestamp": now.isoformat()},
            {"entity_id": "e1", "value": 7.0, "timestamp": (now - timedelta(days=1)).isoformat()},
            {"entity_id": "e1", "value": 1.0, "timestamp": (now - timedelta(days=2)).isoformat()},
        ]

        ctx = MockContext()
        ctx.add_events(Source, events)
        result = ctx.get_aggregates(MinResult, "e1")

        assert result["min_val"] == pytest.approx(1.0)

    def test_max_within_window(self):
        Source = _make_source()

        @dataset(version=1, index=True)
        class MaxResult:
            entity_id: str = field(key=True)
            max_val: float = field()
            timestamp: datetime = field(timestamp=True)

            @pipeline(version=1)
            @inputs(Source)
            def max_pipe(cls, src):
                return src.groupby("entity_id").aggregate(max_val=Max(of="value", window="30d"))

        now = datetime.now(timezone.utc)
        events = [
            {"entity_id": "e1", "value": 3.0, "timestamp": now.isoformat()},
            {"entity_id": "e1", "value": 7.0, "timestamp": (now - timedelta(days=1)).isoformat()},
            {"entity_id": "e1", "value": 1.0, "timestamp": (now - timedelta(days=2)).isoformat()},
        ]

        ctx = MockContext()
        ctx.add_events(Source, events)
        result = ctx.get_aggregates(MaxResult, "e1")

        assert result["max_val"] == pytest.approx(7.0)


class TestEmptyGroups:
    def test_missing_entity_returns_zeros(self):
        Source = _make_source()

        @dataset(version=1, index=True)
        class EmptyResult:
            entity_id: str = field(key=True)
            cnt: float = field()
            avg_val: float = field()
            timestamp: datetime = field(timestamp=True)

            @pipeline(version=1)
            @inputs(Source)
            def pipe(cls, src):
                return src.groupby("entity_id").aggregate(
                    cnt=Count(window="7d"),
                    avg_val=Avg(of="value", window="7d"),
                )

        ctx = MockContext()
        result = ctx.get_aggregates(EmptyResult, "nonexistent")

        assert result["cnt"] == 0.0
        assert result["avg_val"] == 0.0


# ---------------------------------------------------------------------------
# MockContext — pipeline processing
# ---------------------------------------------------------------------------


class TestMockContextPipeline:
    def test_groupby_partitions_independently(self):
        Source = _make_source()

        @dataset(version=1, index=True)
        class GroupResult:
            entity_id: str = field(key=True)
            cnt: float = field()
            timestamp: datetime = field(timestamp=True)

            @pipeline(version=1)
            @inputs(Source)
            def pipe(cls, src):
                return src.groupby("entity_id").aggregate(cnt=Count(window="30d"))

        now = datetime.now(timezone.utc)
        events = [
            {"entity_id": "a", "value": 1.0, "timestamp": now.isoformat()},
            {"entity_id": "a", "value": 2.0, "timestamp": now.isoformat()},
            {"entity_id": "b", "value": 3.0, "timestamp": now.isoformat()},
        ]

        ctx = MockContext()
        ctx.add_events(Source, events)

        assert ctx.get_aggregates(GroupResult, "a")["cnt"] == 2.0
        assert ctx.get_aggregates(GroupResult, "b")["cnt"] == 1.0

    def test_multiple_agg_specs(self):
        Source = _make_source()

        @dataset(version=1, index=True)
        class MultiResult:
            entity_id: str = field(key=True)
            avg_val: float = field()
            cnt: float = field()
            total: float = field()
            timestamp: datetime = field(timestamp=True)

            @pipeline(version=1)
            @inputs(Source)
            def pipe(cls, src):
                return src.groupby("entity_id").aggregate(
                    avg_val=Avg(of="value", window="30d"),
                    cnt=Count(window="30d"),
                    total=Sum(of="value", window="30d"),
                )

        now = datetime.now(timezone.utc)
        events = [
            {"entity_id": "e1", "value": 10.0, "timestamp": now.isoformat()},
            {"entity_id": "e1", "value": 20.0, "timestamp": now.isoformat()},
        ]

        ctx = MockContext()
        ctx.add_events(Source, events)
        result = ctx.get_aggregates(MultiResult, "e1")

        assert result["avg_val"] == pytest.approx(15.0)
        assert result["cnt"] == 2.0
        assert result["total"] == pytest.approx(30.0)


# ---------------------------------------------------------------------------
# Expectations
# ---------------------------------------------------------------------------


class TestMockContextExpectations:
    def test_violations_returned_from_add_events(self):
        @dataset(version=1)
        class Checked:
            entity_id: str = field(key=True)
            rating: float = field()
            timestamp: datetime = field(timestamp=True)

            @expectations
            def checks(cls):
                return [
                    expect_column_values_to_be_between(
                        column="rating", min_value=1.0, max_value=5.0
                    ),
                ]

        now = datetime.now(timezone.utc).isoformat()
        events = [
            {"entity_id": "e1", "rating": 3.0, "timestamp": now},
            {"entity_id": "e2", "rating": 6.0, "timestamp": now},  # violation
        ]

        ctx = MockContext()
        violations = ctx.add_events(Checked, events)

        assert len(violations) == 1
        assert violations[0].column == "rating"
        assert violations[0].expectation_type == "column_values_between"

    def test_no_violations_when_data_valid(self):
        @dataset(version=1)
        class AllGood:
            entity_id: str = field(key=True)
            rating: float = field()
            timestamp: datetime = field(timestamp=True)

            @expectations
            def checks(cls):
                return [
                    expect_column_values_to_be_between(
                        column="rating", min_value=1.0, max_value=5.0
                    ),
                ]

        now = datetime.now(timezone.utc).isoformat()
        events = [
            {"entity_id": "e1", "rating": 3.0, "timestamp": now},
            {"entity_id": "e2", "rating": 4.5, "timestamp": now},
        ]

        ctx = MockContext()
        violations = ctx.add_events(AllGood, events)

        assert violations == []

    def test_not_null_violation(self):
        @dataset(version=1)
        class NullCheck:
            entity_id: str = field(key=True)
            name: str = field()
            timestamp: datetime = field(timestamp=True)

            @expectations
            def checks(cls):
                return [expect_column_values_to_not_be_null(column="name")]

        now = datetime.now(timezone.utc).isoformat()
        events = [
            {"entity_id": "e1", "name": None, "timestamp": now},
        ]

        ctx = MockContext()
        violations = ctx.add_events(NullCheck, events)

        assert len(violations) == 1
        assert violations[0].expectation_type == "column_values_not_null"


# ---------------------------------------------------------------------------
# Extractor testing via query()
# ---------------------------------------------------------------------------


class TestMockContextExtractors:
    def test_query_seeds_from_pipeline_and_runs_transform_extractor(self):
        # Given: a source, an aggregated dataset, and a featureset with extractors.

        @dataset(version=1)
        class Orders:
            user_id: str = field(key=True)
            amount: float = field()
            timestamp: datetime = field(timestamp=True)

        @dataset(version=1, index=True)
        class UserStats:
            user_id: str = field(key=True)
            total_spend: float = field()
            order_count: float = field()
            timestamp: datetime = field(timestamp=True)

            @pipeline(version=1)
            @inputs(Orders)
            def compute(cls, orders):
                return orders.groupby("user_id").aggregate(
                    total_spend=Sum(of="amount", window="30d"),
                    order_count=Count(window="30d"),
                )

        @featureset
        class UserFeatures:
            user_id: str = feature(id=1)
            total_spend: float = feature(id=2)
            order_count: float = feature(id=3)
            is_whale: bool = feature(id=4)

            @extractor(deps=[UserStats])
            @extractor_inputs("user_id")
            @extractor_outputs("total_spend", "order_count")
            def get_stats(cls, ts, user_ids):
                stats = UserStats.lookup(ts, user_id=user_ids)
                return stats[["total_spend", "order_count"]]

            @extractor
            @extractor_inputs("total_spend", "order_count")
            @extractor_outputs("is_whale")
            def compute_whale(cls, ts, total_spend, order_count):
                return total_spend > 100.0 and order_count > 2

        now = datetime.now(timezone.utc)
        events = [
            {"user_id": "u1", "amount": 50.0, "timestamp": now.isoformat()},
            {"user_id": "u1", "amount": 75.0, "timestamp": (now - timedelta(days=1)).isoformat()},
            {"user_id": "u1", "amount": 30.0, "timestamp": (now - timedelta(days=2)).isoformat()},
        ]

        # When
        ctx = MockContext()
        ctx.add_events(Orders, events)
        features = ctx.query(UserFeatures, "u1")

        # Then
        assert features["total_spend"] == pytest.approx(155.0)
        assert features["order_count"] == pytest.approx(3.0)
        assert features["is_whale"] is True

    def test_query_non_whale(self):
        @dataset(version=1)
        class Orders2:
            user_id: str = field(key=True)
            amount: float = field()
            timestamp: datetime = field(timestamp=True)

        @dataset(version=1, index=True)
        class UserStats2:
            user_id: str = field(key=True)
            total_spend: float = field()
            order_count: float = field()
            timestamp: datetime = field(timestamp=True)

            @pipeline(version=1)
            @inputs(Orders2)
            def compute(cls, orders):
                return orders.groupby("user_id").aggregate(
                    total_spend=Sum(of="amount", window="30d"),
                    order_count=Count(window="30d"),
                )

        @featureset
        class UserFeatures2:
            user_id: str = feature(id=1)
            total_spend: float = feature(id=2)
            order_count: float = feature(id=3)
            is_whale: bool = feature(id=4)

            @extractor(deps=[UserStats2])
            @extractor_inputs("user_id")
            @extractor_outputs("total_spend", "order_count")
            def get_stats(cls, ts, user_ids):
                stats = UserStats2.lookup(ts, user_id=user_ids)
                return stats[["total_spend", "order_count"]]

            @extractor
            @extractor_inputs("total_spend", "order_count")
            @extractor_outputs("is_whale")
            def compute_whale(cls, ts, total_spend, order_count):
                return total_spend > 100.0 and order_count > 2

        now = datetime.now(timezone.utc)
        events = [
            {"user_id": "u2", "amount": 10.0, "timestamp": now.isoformat()},
        ]

        ctx = MockContext()
        ctx.add_events(Orders2, events)
        features = ctx.query(UserFeatures2, "u2")

        assert features["total_spend"] == pytest.approx(10.0)
        assert features["order_count"] == pytest.approx(1.0)
        assert features["is_whale"] is False


# ---------------------------------------------------------------------------
# Cross-validation: same data + assertions as test_e2e.py
# ---------------------------------------------------------------------------


class TestCrossValidationWithE2E:
    """Uses the same seed data and expected values as test_e2e.py to ensure
    MockContext produces identical results to the Rust engine."""

    def test_restaurant_features_match_e2e(self):
        from thyme.expectations import expect_column_values_to_be_between

        # Same definitions as examples/restaurant_reviews/features.py

        @dataset(version=1)
        class Review:
            restaurant_id: str = field(key=True)
            user_id: str = field()
            rating: float = field()
            review_text: str = field()
            timestamp: datetime = field(timestamp=True)

            @expectations
            def get_expectations(cls):
                return [
                    expect_column_values_to_be_between(
                        column="rating", min_value=1.0, max_value=5.0, mostly=0.95
                    ),
                ]

        @dataset(version=1, index=True)
        class RestaurantRatingStats:
            restaurant_id: str = field(key=True)
            avg_rating_30d: float = field()
            review_count_30d: int = field()
            review_count_7d: int = field()
            max_rating_30d: float = field()
            timestamp: datetime = field(timestamp=True)

            @pipeline(version=1)
            @inputs(Review)
            def compute_stats(cls, reviews):
                return reviews.groupby("restaurant_id").aggregate(
                    avg_rating_30d=Avg(of="rating", window="30d"),
                    review_count_30d=Count(window="30d"),
                    review_count_7d=Count(window="7d"),
                    max_rating_30d=Max(of="rating", window="30d"),
                )

        @featureset
        class RestaurantFeatures:
            restaurant_id: str = feature(id=1)
            avg_rating_30d: float = feature(id=2)
            review_count_30d: int = feature(id=3)
            is_highly_rated: bool = feature(id=4)
            review_count_7d: int = feature(id=5)
            max_rating_30d: float = feature(id=6)

            @extractor(deps=[RestaurantRatingStats])
            @extractor_inputs("restaurant_id")
            @extractor_outputs("avg_rating_30d", "review_count_30d", "review_count_7d", "max_rating_30d")
            def get_stats(cls, ts, restaurant_ids):
                stats = RestaurantRatingStats.lookup(ts, restaurant_id=restaurant_ids)
                return stats[["avg_rating_30d", "review_count_30d", "review_count_7d", "max_rating_30d"]]

            @extractor
            @extractor_inputs("avg_rating_30d", "review_count_30d")
            @extractor_outputs("is_highly_rated")
            def compute_highly_rated(cls, ts, avg_ratings, review_counts):
                return avg_ratings > 4.0 and review_counts > 3

        # Same seed data as test_e2e.py seed_jsonl fixture
        now = datetime.now(timezone.utc)
        events = []
        for i in range(5):
            ts = (now - timedelta(days=i)).isoformat().replace("+00:00", "Z")
            events.append({
                "restaurant_id": "r_high",
                "user_id": f"u{i:04d}",
                "rating": 4.8,
                "review_text": "great",
                "timestamp": ts,
            })
        for i in range(3):
            ts = (now - timedelta(days=i)).isoformat().replace("+00:00", "Z")
            events.append({
                "restaurant_id": "r_low",
                "user_id": f"u{i:04d}",
                "rating": 2.5,
                "review_text": "bad",
                "timestamp": ts,
            })
        # Violation record (rating 6.0)
        ts = (now - timedelta(hours=1)).isoformat().replace("+00:00", "Z")
        events.append({
            "restaurant_id": "r_high",
            "user_id": "u_violation",
            "rating": 6.0,
            "review_text": "violation",
            "timestamp": ts,
        })

        # When
        ctx = MockContext()
        violations = ctx.add_events(Review, events)

        # Then: expectations should catch the violation record
        assert len(violations) >= 1
        assert any(v.column == "rating" for v in violations)

        # Then: same assertions as test_e2e_restaurant_features
        highly = ctx.query(RestaurantFeatures, "r_high")
        # r_high: 5 reviews at 4.8 + 1 violation at 6.0 = 6 reviews
        # avg = (5*4.8 + 6.0) / 6 = 30.0 / 6 = 5.0
        assert highly["avg_rating_30d"] == pytest.approx(5.0, abs=0.01)
        assert highly["review_count_30d"] == pytest.approx(6, abs=0.01)
        assert highly["review_count_7d"] == pytest.approx(6, abs=0.01)
        assert highly["max_rating_30d"] == pytest.approx(6.0, abs=0.01)
        assert highly["is_highly_rated"] is True

        low = ctx.query(RestaurantFeatures, "r_low")
        assert low["avg_rating_30d"] == pytest.approx(2.5, abs=0.01)
        assert low["review_count_30d"] == pytest.approx(3, abs=0.01)
        assert low["review_count_7d"] == pytest.approx(3, abs=0.01)
        assert low["max_rating_30d"] == pytest.approx(2.5, abs=0.01)
        assert low["is_highly_rated"] is False


# ---------------------------------------------------------------------------
# MockContext.query_offline — point-in-time correct feature extraction
# ---------------------------------------------------------------------------


class TestMockContextQueryOffline:
    """Given events at multiple timestamps, when query_offline at a past timestamp,
    then only events visible at that timestamp are used for aggregation."""

    def test_query_offline_point_in_time_correct(self):
        # given: events at t1 and t2
        Source = _make_source()

        @dataset(version=1, index=True)
        class Stats:
            entity_id: str = field(key=True)
            cnt: float = field()
            timestamp: datetime = field(timestamp=True)

            @pipeline(version=1)
            @inputs(Source)
            def count_pipe(cls, src):
                return src.groupby("entity_id").aggregate(cnt=Count(window="30d"))

        @featureset
        class F:
            entity_id: str = feature(id=1)
            cnt: float = feature(id=2)

            @extractor(deps=[Stats])
            @extractor_inputs("entity_id")
            @extractor_outputs("cnt")
            def get_cnt(cls, ts, entity_ids):
                return Stats.lookup(ts, entity_id=entity_ids)

        now = datetime.now(timezone.utc)
        t1 = now - timedelta(days=5)
        t2 = now

        events = [
            {"entity_id": "e1", "value": 1.0, "timestamp": t1.isoformat()},
            {"entity_id": "e1", "value": 2.0, "timestamp": t2.isoformat()},
        ]

        ctx = MockContext()
        ctx.add_events(Source, events)

        # when: query at t1 (only 1 event visible)
        result = ctx.query_offline(
            F,
            [{"entity_id": "e1", "timestamp": t1.isoformat()}],
            entity_column="entity_id",
            timestamp_column="timestamp",
        )

        # then: count should be 1 (only t1 event visible)
        dicts = result.to_dict()
        assert dicts[0]["cnt"] == pytest.approx(1.0)

    def test_query_offline_at_latest_sees_all_events(self):
        # given
        Source = _make_source()

        @dataset(version=1, index=True)
        class Stats2:
            entity_id: str = field(key=True)
            total: float = field()
            timestamp: datetime = field(timestamp=True)

            @pipeline(version=1)
            @inputs(Source)
            def sum_pipe(cls, src):
                return src.groupby("entity_id").aggregate(total=Sum(of="value", window="30d"))

        @featureset
        class F2:
            entity_id: str = feature(id=1)
            total: float = feature(id=2)

            @extractor(deps=[Stats2])
            @extractor_inputs("entity_id")
            @extractor_outputs("total")
            def get_total(cls, ts, entity_ids):
                return Stats2.lookup(ts, entity_id=entity_ids)

        now = datetime.now(timezone.utc)
        events = [
            {"entity_id": "e1", "value": 10.0, "timestamp": (now - timedelta(days=2)).isoformat()},
            {"entity_id": "e1", "value": 20.0, "timestamp": (now - timedelta(days=1)).isoformat()},
            {"entity_id": "e1", "value": 30.0, "timestamp": now.isoformat()},
        ]

        ctx = MockContext()
        ctx.add_events(Source, events)

        # when: query at now (all 3 events visible)
        result = ctx.query_offline(
            F2,
            [{"entity_id": "e1", "timestamp": now.isoformat()}],
            entity_column="entity_id",
            timestamp_column="timestamp",
        )

        # then
        dicts = result.to_dict()
        assert dicts[0]["total"] == pytest.approx(60.0)

    def test_query_offline_returns_thyme_result(self):
        Source = _make_source()

        @dataset(version=1, index=True)
        class Stats3:
            entity_id: str = field(key=True)
            cnt: float = field()
            timestamp: datetime = field(timestamp=True)

            @pipeline(version=1)
            @inputs(Source)
            def count_pipe(cls, src):
                return src.groupby("entity_id").aggregate(cnt=Count(window="30d"))

        @featureset
        class F3:
            entity_id: str = feature(id=1)
            cnt: float = feature(id=2)

            @extractor(deps=[Stats3])
            @extractor_inputs("entity_id")
            @extractor_outputs("cnt")
            def get_cnt(cls, ts, entity_ids):
                return Stats3.lookup(ts, entity_id=entity_ids)

        now = datetime.now(timezone.utc)
        events = [
            {"entity_id": "e1", "value": 1.0, "timestamp": now.isoformat()},
        ]

        ctx = MockContext()
        ctx.add_events(Source, events)

        from thyme.result import ThymeResult
        result = ctx.query_offline(
            F3,
            [{"entity_id": "e1", "timestamp": now.isoformat()}],
            entity_column="entity_id",
            timestamp_column="timestamp",
        )
        assert isinstance(result, ThymeResult)

    def test_query_offline_multiple_entities(self):
        Source = _make_source()

        @dataset(version=1, index=True)
        class Stats4:
            entity_id: str = field(key=True)
            cnt: float = field()
            timestamp: datetime = field(timestamp=True)

            @pipeline(version=1)
            @inputs(Source)
            def count_pipe(cls, src):
                return src.groupby("entity_id").aggregate(cnt=Count(window="30d"))

        @featureset
        class F4:
            entity_id: str = feature(id=1)
            cnt: float = feature(id=2)

            @extractor(deps=[Stats4])
            @extractor_inputs("entity_id")
            @extractor_outputs("cnt")
            def get_cnt(cls, ts, entity_ids):
                return Stats4.lookup(ts, entity_id=entity_ids)

        now = datetime.now(timezone.utc)
        events = [
            {"entity_id": "e1", "value": 1.0, "timestamp": now.isoformat()},
            {"entity_id": "e2", "value": 2.0, "timestamp": now.isoformat()},
            {"entity_id": "e2", "value": 3.0, "timestamp": now.isoformat()},
        ]

        ctx = MockContext()
        ctx.add_events(Source, events)

        result = ctx.query_offline(
            F4,
            [
                {"entity_id": "e1", "timestamp": now.isoformat()},
                {"entity_id": "e2", "timestamp": now.isoformat()},
            ],
            entity_column="entity_id",
            timestamp_column="timestamp",
        )

        assert len(result) == 2
        dicts = result.to_dict()
        e1 = next(d for d in dicts if d["entity_id"] == "e1")
        e2 = next(d for d in dicts if d["entity_id"] == "e2")
        assert e1["cnt"] == pytest.approx(1.0)
        assert e2["cnt"] == pytest.approx(2.0)


# ---------------------------------------------------------------------------
# Phase 5: Polars engine — new DataFrame API
# ---------------------------------------------------------------------------


class TestPolarsDataFrameAPI:
    """Tests for the new Polars DataFrame-based add/get methods."""

    def test_add_events_df_accepts_polars(self):
        import polars as pl

        Source = _make_source()

        @dataset(version=1, index=True)
        class StatsDF1:
            entity_id: str = field(key=True)
            cnt: float = field()
            timestamp: datetime = field(timestamp=True)

            @pipeline(version=1)
            @inputs(Source)
            def count_pipe(cls, src):
                return src.groupby("entity_id").aggregate(cnt=Count(window="30d"))

        now = datetime.now(timezone.utc)
        events_df = pl.DataFrame({
            "entity_id": ["e1", "e1", "e1"],
            "value": [1.0, 2.0, 3.0],
            "timestamp": [
                now.isoformat(),
                (now - timedelta(days=1)).isoformat(),
                (now - timedelta(days=2)).isoformat(),
            ],
        })

        ctx = MockContext()
        ctx.add_events_df(Source, events_df)
        result = ctx.get_aggregates(StatsDF1, "e1")

        assert result["cnt"] == pytest.approx(3.0)

    def test_get_aggregates_df_returns_dataframe(self):
        import polars as pl

        Source = _make_source()

        @dataset(version=1, index=True)
        class StatsDF2:
            entity_id: str = field(key=True)
            total: float = field()
            timestamp: datetime = field(timestamp=True)

            @pipeline(version=1)
            @inputs(Source)
            def sum_pipe(cls, src):
                return src.groupby("entity_id").aggregate(total=Sum(of="value", window="30d"))

        now = datetime.now(timezone.utc)
        events = [
            {"entity_id": "e1", "value": 10.0, "timestamp": now.isoformat()},
            {"entity_id": "e2", "value": 20.0, "timestamp": now.isoformat()},
            {"entity_id": "e1", "value": 30.0, "timestamp": now.isoformat()},
        ]

        ctx = MockContext()
        ctx.add_events(Source, events)
        df = ctx.get_aggregates_df(StatsDF2)

        assert isinstance(df, pl.DataFrame)
        assert "entity_id" in df.columns
        assert "total" in df.columns
        assert len(df) == 2  # e1 and e2

        e1_row = df.filter(pl.col("entity_id") == "e1")
        assert e1_row["total"][0] == pytest.approx(40.0)

    def test_polars_engine_10k_events_under_200ms(self):
        """Benchmark: 10k events should process in under 200ms."""
        import time

        Source = _make_source()

        @dataset(version=1, index=True)
        class BenchStats:
            entity_id: str = field(key=True)
            cnt: float = field()
            total: float = field()
            timestamp: datetime = field(timestamp=True)

            @pipeline(version=1)
            @inputs(Source)
            def bench_pipe(cls, src):
                return src.groupby("entity_id").aggregate(
                    cnt=Count(window="30d"),
                    total=Sum(of="value", window="30d"),
                )

        now = datetime.now(timezone.utc)
        events = [
            {
                "entity_id": f"e{i % 100}",
                "value": float(i),
                "timestamp": (now - timedelta(seconds=i)).isoformat(),
            }
            for i in range(10_000)
        ]

        ctx = MockContext()
        start = time.monotonic()
        ctx.add_events(Source, events)
        # Force aggregation for all entities
        for i in range(100):
            ctx.get_aggregates(BenchStats, f"e{i}")
        elapsed = time.monotonic() - start

        assert elapsed < 0.2, f"10k events took {elapsed:.3f}s, expected < 0.2s"
