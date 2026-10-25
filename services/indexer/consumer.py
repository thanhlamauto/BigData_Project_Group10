"""At-least-once consumer; commit only after durable index write or version conflict."""

import json
import logging
import os
import time

import httpx
from confluent_kafka import Consumer, KafkaError

from services.indexer.embedding import Embedder
from services.indexer.upsert import initialize, upsert
from services.producer.events import validate_event

log = logging.getLogger("indexer")


def index_event(client, index, event, embedder):
    # Cleaned events carry Spark metadata beyond the raw contract.
    validate_event({k: v for k, v in event.items() if k not in {"processed_at", "ingest_date"}})
    vector = embedder.encode([event["title"] + "\n" + event["body"]])[0]
    return upsert(client, index, event, vector)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    index = os.getenv("OPENSEARCH_INDEX", "streamrag-docs-v1")
    embedder = Embedder()
    consumer = Consumer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            "group.id": os.getenv("INDEXER_GROUP", "streamrag-indexer-v1"),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "max.poll.interval.ms": 600000,
        }
    )
    with httpx.Client(
        base_url=os.getenv("OPENSEARCH_URL", "http://localhost:9200"), timeout=20
    ) as client:
        initialize(client, index, int(os.getenv("EMBEDDING_DIM", "384")))
        consumer.subscribe([os.getenv("CLEANED_TOPIC", "streamrag.cleaned.v1")])
        try:
            while True:
                message = consumer.poll(1)
                if message is None:
                    continue
                if message.error():
                    if message.error().code() in {
                        KafkaError.UNKNOWN_TOPIC_OR_PART,
                        KafkaError._UNKNOWN_TOPIC,
                    }:
                        log.warning("waiting for cleaned topic")
                        time.sleep(1)
                        continue
                    raise RuntimeError(str(message.error()))
                try:
                    event = json.loads(message.value())
                    # Validate before retrying infrastructure errors.
                    validate_event(
                        {k: v for k, v in event.items() if k not in {"processed_at", "ingest_date"}}
                    )
                except (ValueError, TypeError, AttributeError):
                    log.error(
                        "rejected partition=%s offset=%s", message.partition(), message.offset()
                    )
                    consumer.commit(message=message, asynchronous=False)
                    continue
                for attempt in range(5):
                    try:
                        result = index_event(client, index, event, embedder)
                        break
                    except (httpx.HTTPError, RuntimeError):
                        if attempt == 4:
                            raise
                        time.sleep(min(2**attempt, 10))
                consumer.commit(message=message, asynchronous=False)
                log.info(
                    "%s event_id=%s document_id=%s version=%s partition=%s offset=%s",
                    result,
                    event["event_id"],
                    event["document_id"],
                    event["version"],
                    message.partition(),
                    message.offset(),
                )
        finally:
            consumer.close()


if __name__ == "__main__":
    main()
