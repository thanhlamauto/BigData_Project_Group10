"""Reliable, bounded Kafka delivery with an injectable client for offline tests."""

import json
import logging
import time
from datetime import UTC, datetime

from services.producer.events import utc_string, validate_event

LOGGER = logging.getLogger(__name__)


class EventProducer:
    def __init__(
        self,
        bootstrap_servers="localhost:9092",
        topic="streamrag.raw.v1",
        *,
        client=None,
        timeout=30.0,
        clock=None,
        monotonic=None,
    ):
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if client is None:
            from confluent_kafka import Producer

            client = Producer(
                {
                    "bootstrap.servers": bootstrap_servers,
                    "client.id": "streamrag-tv2-producer",
                    "acks": "all",
                    "enable.idempotence": True,
                    "delivery.timeout.ms": int(timeout * 1000),
                    "request.timeout.ms": min(10000, int(timeout * 1000)),
                }
            )
        self.client = client
        self.topic = topic
        self.timeout = timeout
        self.clock = clock or (lambda: datetime.now(UTC))
        self.monotonic = monotonic or time.monotonic
        self.sent = 0
        self.delivered = 0
        self.errors = []

    def _delivery(self, error, message):
        if error is not None:
            self.errors.append(str(error))
            return
        self.delivered += 1
        LOGGER.info(
            "delivered topic=%s partition=%s offset=%s",
            message.topic(),
            message.partition(),
            message.offset(),
        )

    def publish_raw(self, value: bytes, key: str) -> None:
        """Intentional validation bypass for the explicit --edge-cases mode only."""
        deadline = self.monotonic() + self.timeout
        while True:
            try:
                self.client.produce(
                    self.topic, key=key.encode("utf-8"), value=value, on_delivery=self._delivery
                )
                break
            except BufferError as exc:
                remaining = deadline - self.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Kafka local queue remained full") from exc
                self.client.poll(min(0.1, remaining))
        self.sent += 1
        self.client.poll(0)
        if self.errors:
            raise RuntimeError(f"Kafka delivery failed: {self.errors[0]}")

    def publish(self, event: dict) -> dict:
        validate_event(event)
        outgoing = dict(event, ingested_at=utc_string(self.clock()))
        validate_event(outgoing)
        self.publish_raw(
            json.dumps(outgoing, ensure_ascii=False).encode("utf-8"), outgoing["document_id"]
        )
        LOGGER.info(
            "queued event_id=%s document_id=%s version=%s ingested_at=%s",
            outgoing["event_id"],
            outgoing["document_id"],
            outgoing["version"],
            outgoing["ingested_at"],
        )
        return outgoing

    def flush(self) -> int:
        pending = self.client.flush(self.timeout)
        if self.errors:
            raise RuntimeError(f"Kafka delivery failed: {self.errors[0]}")
        if pending or self.delivered != self.sent:
            raise TimeoutError(
                f"Kafka delivery incomplete: {self.delivered}/{self.sent} acknowledged"
            )
        return self.delivered
