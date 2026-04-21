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
    user_id: int = feature(id=1)
    total_spend: float = feature(id=2)
    purchase_count: int = feature(id=3)


class NotAFeatureset:
    """Plain class without the @featureset decorator — used for resolver error tests."""
    pass
