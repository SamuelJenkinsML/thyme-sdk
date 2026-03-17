"""Sample feature module for CLI integration tests."""
from datetime import datetime

from thyme.dataset import dataset, field


@dataset(index=True, version=1)
class Purchase:
    user_id: int = field(key=True)
    amount: float = field()
    event_time: datetime = field(timestamp=True)
