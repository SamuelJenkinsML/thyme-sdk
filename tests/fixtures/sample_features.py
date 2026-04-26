"""Sample feature module for CLI integration tests."""
from datetime import datetime

from thyme.dataset import dataset, field
from thyme.featureset import feature, featureset


@dataset(index=True, version=1)
class Purchase:
    user_id: int = field(key=True)
    amount: float = field()
    event_time: datetime = field(timestamp=True)


@featureset
class UserFeatures:
    user_id: int = feature()
    total_spend: float = feature()
    purchase_count: int = feature()


class NotAFeatureset:
    """Plain class without the @featureset decorator — used for resolver error tests."""
    pass
