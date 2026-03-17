# Thyme SDK

Python SDK for the [Thyme](https://github.com/SamuelJenkinsML/thyme) streaming feature platform.

## Installation

```bash
pip install thyme-sdk
```

## Quick Start

```python
from datetime import datetime
from thyme import dataset, field, pipeline, inputs, Avg, Count
from thyme import featureset, feature, extractor, extractor_inputs, extractor_outputs
from thyme import source, IcebergSource

@dataset(index=True, version=1)
class Event:
    user_id: str = field(key=True)
    amount: float = field()
    timestamp: datetime = field(timestamp=True)

@dataset(version=1)
class UserStats:
    user_id: str = field(key=True)
    avg_amount_1h: float = field()
    count_1h: float = field()
    timestamp: datetime = field(timestamp=True)

    @pipeline(version=1)
    @inputs(Event)
    def compute(cls, events):
        return events.groupby("user_id").aggregate(
            avg_amount_1h=Avg("amount", window="1h"),
            count_1h=Count("amount", window="1h"),
        )
```

## Development

```bash
uv sync
make test
make lint
make fmt
```

## License

MIT
