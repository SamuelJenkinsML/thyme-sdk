"""Broker-agnostic Kafka test cleanup (topics + consumer groups).

Replaces the Redpanda-specific ``docker exec ... rpk`` cleanup used in Thyme's
e2e/demo test suites so the tests run against any Kafka-protocol broker
(Redpanda, Apache Kafka, Tansu, ...). Uses the native ``confluent-kafka``
``AdminClient``, keyed off an explicit ``brokers`` address.

Requires the ``confluent-kafka`` extra: ``pip install "thyme-sdk[testing]"``.
"""

from __future__ import annotations


def _admin(brokers: str):
    try:
        from confluent_kafka.admin import AdminClient
    except ImportError as exc:  # pragma: no cover - exercised via the guarded path
        raise ImportError(
            "kafka_admin requires confluent-kafka. Install with: "
            'pip install "thyme-sdk[testing]"'
        ) from exc
    return AdminClient({"bootstrap.servers": brokers})


def list_topics(brokers: str) -> list[str]:
    """Return all topic names on the broker."""
    admin = _admin(brokers)
    return list(admin.list_topics(timeout=10).topics.keys())


def delete_topics(brokers: str, topics: list[str]) -> None:
    """Delete the given topics; silently ignores ones that don't exist."""
    if not topics:
        return
    # Bind the AdminClient to a name: the delete futures reference it, so it must
    # outlive the .result() calls (else librdkafka errors with _DESTROY).
    admin = _admin(brokers)
    for topic, future in admin.delete_topics(topics, operation_timeout=10).items():
        try:
            future.result()
        except Exception as exc:  # noqa: BLE001 - best-effort cleanup
            if "UNKNOWN_TOPIC_OR_PART" not in str(exc):
                print(f"  warning: failed to delete topic {topic}: {exc}")


def delete_consumer_groups(brokers: str, prefix: str = "thyme-") -> None:
    """Delete consumer groups whose id starts with ``prefix``."""
    admin = _admin(brokers)
    groups = admin.list_consumer_groups().result()
    matching = [g.group_id for g in groups.valid if g.group_id.startswith(prefix)]
    if not matching:
        return
    for group_id, future in admin.delete_consumer_groups(matching).items():
        try:
            future.result()
        except Exception as exc:  # noqa: BLE001 - best-effort cleanup
            print(f"  warning: failed to delete group {group_id}: {exc}")


def clean_kafka(brokers: str, topics: list[str], group_prefix: str = "thyme-") -> None:
    """Delete ``topics`` and any consumer groups matching ``group_prefix`` so
    stale data/offsets don't interfere between test runs. Broker-agnostic."""
    delete_topics(brokers, topics)
    delete_consumer_groups(brokers, group_prefix)
