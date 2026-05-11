"""Catalog-demo workspace for the Thyme frontend.

A multi-domain seed (commerce + content + identity) designed to populate the
catalog UI with the kind of metadata variety you'd see in a real workspace:
multiple owners, dict-form tag axes, a deprecated featureset with a
replacement pointer, and both intra-domain and cross-domain lineage.

Extractors are stubs returning constants. This seed populates the catalog
without requiring the engine to be running — feature values are out of scope.

Usage:
    thyme commit examples/catalog_demo/workspace.py
"""
from datetime import datetime

from thyme.connectors import KinesisSource, PostgresSource, source
from thyme.dataset import dataset, field
from thyme.featureset import (
    extractor,
    extractor_inputs,
    extractor_outputs,
    feature,
    featureset,
)
from thyme.pipeline import Count, Max, Sum, inputs, pipeline


# ─── Domain A: commerce ──────────────────────────────────────────────────────

@source(
    PostgresSource(table="transactions"),
    cursor="event_time",
    every="60s",
    max_lateness="5m",
    description="Transactions ingestion from the payments service Postgres replica.",
    owner="payments@thyme.io",
    tags={"domain": "commerce", "tier": "production"},
    project="commerce",
)
@dataset(
    version=1,
    description="Per-user purchase transactions.",
    owner="payments@thyme.io",
    tags={"domain": "commerce", "tier": "production", "pii": "false"},
    project="commerce",
)
class Transaction:
    user_id: int = field(key=True)
    amount: float = field()
    merchant_id: str = field()
    event_time: datetime = field(timestamp=True)


@dataset(
    index=True,
    version=1,
    description="Aggregated transaction stats per user across rolling windows.",
    owner="payments@thyme.io",
    tags={"domain": "commerce", "tier": "production"},
    project="commerce",
)
class UserTxnStats:
    user_id: int = field(key=True)
    txn_count_24h: int = field()
    txn_count_7d: int = field()
    total_spend_7d: float = field()
    max_txn_amount_24h: float = field()
    timestamp: datetime = field(timestamp=True)

    @pipeline(version=1)
    @inputs(Transaction)
    def compute_user_txn_stats(cls, txn):
        return txn.groupby("user_id").aggregate(
            txn_count_24h=Count(window="24h"),
            txn_count_7d=Count(window="7d"),
            total_spend_7d=Sum(of="amount", window="7d"),
            max_txn_amount_24h=Max(of="amount", window="24h"),
        )


@featureset(
    description="Spend volume and frequency signals for a user, computed from transactions.",
    owner="payments@thyme.io",
    tags={"domain": "commerce", "tier": "production"},
    project="commerce",
)
class UserSpendFeatures:
    user_id: int = feature()
    txn_count_24h: int = feature(ref=UserTxnStats.txn_count_24h, default=0)
    total_spend_7d: float = feature(ref=UserTxnStats.total_spend_7d, default=0.0)
    avg_txn_value_7d: float = feature()

    @extractor
    @extractor_inputs("user_id")
    @extractor_outputs("avg_txn_value_7d")
    def derive_avg(cls, ts, user_id):
        return 42.0


@featureset(
    description="Risk-oriented derived signals built on top of UserSpendFeatures.",
    owner="risk@thyme.io",
    tags={"domain": "commerce", "tier": "production", "pii": "false"},
    project="risk",
)
class UserRiskFeatures:
    user_id: int = feature()
    max_txn_amount_24h: float = feature(ref=UserTxnStats.max_txn_amount_24h, default=0.0)
    velocity_score: float = feature()

    @extractor(deps=[UserSpendFeatures])
    @extractor_inputs("user_id")
    @extractor_outputs("velocity_score")
    def compute_velocity(cls, ts, user_id):
        return 0.37


# ─── Domain B: content ───────────────────────────────────────────────────────

@source(
    KinesisSource(stream_arn="arn:aws:kinesis:us-east-1:000000000000:stream/pageviews"),
    max_lateness="1h",
    description="Streaming pageview events from the web/mobile frontends via Kinesis.",
    owner="growth@thyme.io",
    tags={"domain": "content", "tier": "production"},
    project="content",
)
@dataset(
    version=1,
    description="Per-user pageview events.",
    owner="growth@thyme.io",
    tags={"domain": "content", "tier": "production", "pii": "false"},
    project="content",
)
class Pageview:
    user_id: int = field(key=True)
    article_id: str = field()
    dwell_ms: int = field()
    event_time: datetime = field(timestamp=True)


@source(
    PostgresSource(table="articles"),
    cursor="updated_at",
    every="5m",
    description="Slowly-changing article catalog mirrored from the CMS Postgres.",
    owner="content-platform@thyme.io",
    tags={"domain": "content", "tier": "production"},
    project="content",
)
@dataset(
    index=True,
    version=1,
    description="Article catalog (category, author, publication time).",
    owner="content-platform@thyme.io",
    tags={"domain": "content", "tier": "production"},
    project="content",
)
class Article:
    article_id: str = field(key=True)
    category: str = field()
    author: str = field()
    updated_at: datetime = field(timestamp=True)


@dataset(
    index=True,
    version=1,
    description="Engagement aggregates per user (counts and dwell time over recent windows).",
    owner="growth@thyme.io",
    tags={"domain": "content", "tier": "production"},
    project="content",
)
class UserEngagementStats:
    user_id: int = field(key=True)
    pageview_count_24h: int = field()
    total_dwell_ms_24h: int = field()
    pageview_count_7d: int = field()
    timestamp: datetime = field(timestamp=True)

    @pipeline(version=1)
    @inputs(Pageview)
    def compute_user_engagement(cls, pv):
        return (
            pv.join(Article, on="article_id", fields=["category"])
            .groupby("user_id")
            .aggregate(
                pageview_count_24h=Count(window="24h"),
                total_dwell_ms_24h=Sum(of="dwell_ms", window="24h"),
                pageview_count_7d=Count(window="7d"),
            )
        )


@featureset(
    description="User browsing intensity over recent windows.",
    owner="growth@thyme.io",
    tags={"domain": "content", "tier": "production"},
    project="content",
)
class UserEngagementFeatures:
    user_id: int = feature()
    pageview_count_24h: int = feature(ref=UserEngagementStats.pageview_count_24h, default=0)
    total_dwell_ms_24h: int = feature(ref=UserEngagementStats.total_dwell_ms_24h, default=0)
    pageview_count_7d: int = feature(ref=UserEngagementStats.pageview_count_7d, default=0)


@featureset(
    description="Earlier engagement featureset with coarser window semantics.",
    owner="growth@thyme.io",
    tags={"domain": "content", "tier": "deprecated"},
    project="content",
    deprecated=True,
    deprecation_reason="Replaced by UserEngagementFeatures with refined window definitions and dwell aggregation.",
    replacement="UserEngagementFeatures",
)
class UserEngagementFeaturesLegacy:
    user_id: int = feature()
    pageview_count_24h: int = feature(ref=UserEngagementStats.pageview_count_24h, default=0)


# ─── Domain C: identity ──────────────────────────────────────────────────────

@source(
    PostgresSource(table="user_profiles"),
    cursor="updated_at",
    every="5m",
    description="User profile mirror from the identity service Postgres.",
    owner="platform@thyme.io",
    tags={"domain": "identity", "tier": "production"},
    project="identity",
)
@dataset(
    index=True,
    version=1,
    description="Per-user profile snapshot (signup date, country, tier).",
    owner="platform@thyme.io",
    tags={"domain": "identity", "tier": "production", "pii": "true"},
    project="identity",
)
class UserProfile:
    user_id: int = field(key=True)
    signup_date: datetime = field()
    country: str = field()
    tier: str = field()
    updated_at: datetime = field(timestamp=True)


@featureset(
    description="User profile features: country, tier, derived account age.",
    owner="platform@thyme.io",
    tags={"domain": "identity", "tier": "production", "pii": "true"},
    project="identity",
)
class UserProfileFeatures:
    user_id: int = feature()
    country: str = feature(ref=UserProfile.country, default="unknown")
    tier: str = feature(ref=UserProfile.tier, default="standard")
    account_age_days: int = feature()

    @extractor
    @extractor_inputs("user_id")
    @extractor_outputs("account_age_days")
    def derive_account_age(cls, ts, user_id):
        return 365


@featureset(
    description="Composite ML feature combining spend, engagement, and profile signals.",
    owner="ml-platform@thyme.io",
    tags={"domain": "ml", "tier": "experimental"},
    project="ml",
)
class UserCompositeFeatures:
    user_id: int = feature()
    composite_value_score: float = feature()

    @extractor(deps=[UserSpendFeatures, UserEngagementFeatures, UserProfileFeatures])
    @extractor_inputs("user_id")
    @extractor_outputs("composite_value_score")
    def compute_composite(cls, ts, user_id):
        return 0.82
