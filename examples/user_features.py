"""Minimal demo featureset for smoke-testing the query path end-to-end.

Run against a local stack (Rust services + frontend):

    thyme commit -m examples.user_features
    thyme query -m examples/user_features.py x:UserFeatures -e user_1 -e user_2

The extractor returns constants, so every query returns non-null feature values
and the query-run appears in the UI with hit_count == row_count.
"""
from datetime import datetime

from thyme.dataset import dataset, field
from thyme.featureset import (
    extractor,
    extractor_inputs,
    extractor_outputs,
    feature,
    featureset,
)


@dataset(index=True, version=1)
class Purchase:
    user_id: int = field(key=True)
    amount: float = field()
    event_time: datetime = field(timestamp=True)


@featureset
class UserFeatures:
    user_id: int = feature(id=1)
    total_spend: float = feature(id=2)
    purchase_count: int = feature(id=3)

    @extractor
    @extractor_inputs("user_id")
    @extractor_outputs("total_spend", "purchase_count")
    def stub(cls, ts, user_id):
        # The query-server passes each @extractor_inputs value as a positional
        # arg (not a dict), and for multi-output extractors expects a dict
        # keyed by @extractor_outputs names.
        return {"total_spend": 99.5, "purchase_count": 3}
