# Real Kafka ingestion verification — 26/10/2026

Run time: 00:20:08–00:20:11 Asia/Bangkok (UTC timestamps are in the
[machine-readable report](kafka-2026-10-26.json)). Result: **PASS**, exit code 0.

Environment: Windows 11 Pro build 22000, Ubuntu 22.04 on WSL2, Docker Engine
29.9.2, Docker Compose 5.6.1, Python 3.12.14 on the Windows host. Docker Engine
was installed from Docker's official Ubuntu apt repository. Docker Desktop was
not installed because the current release requires a newer Windows build.

Kafka used the repository's existing TV1 Compose service without overrides:
`apache/kafka:3.9.1`, container `streamrag-kafka`, port `localhost:9092`, health
status `healthy`. No Spark, MinIO, Indexer, or API service was started.

| Check | Acknowledged | Consumed | Result |
|---|---:|---:|---|
| Seed replay | 400 | 400 | UTF-8 JSON, key equals document ID, refreshed ingestion timestamp |
| Live fixture | 10 | 10 | Expected event IDs, five updates and five new documents |
| Single Atlas update | 1 | 1 | `inc-atlas-001` v2, 21:30 UTC |
| Retry first ten seed events | 10 | 10 | Original event IDs and versions preserved |
| Edge-case fixture | 26 | 26 | All bytes unchanged, including malformed input |
| **Total** | **447** | **447** | **PASS** |

Both topics have three partitions and replication factor 1. Re-running the topic
initializer accepted existing topics. Atlas seed v1 was read from partition 0,
offset 0; the v2 live update was read from the same partition, offset 150. The
edge-case Atlas sequence was v1 → v2 → stale v1, in that order. Normal payloads
matched the committed fixtures except for `ingested_at`, which fell inside the
actual publish interval.

Independent CLI confirmation (exit code 0) used:

```bash
docker exec streamrag-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic streamrag.raw.v1 --from-beginning --max-messages 2 --timeout-ms 15000 --property print.key=true
```

It printed two records, keys `inc-atlas-001` and `inc-storage-001`, with matching
JSON document IDs, followed by `Processed a total of 2 messages`.

To reproduce after starting Kafka, from the repository root with producer
dependencies installed:

```bash
python -m services.producer.verify_kafka --report data/generated/kafka-verification.json
```

The check snapshots current partition end offsets before publishing, so old
messages do not affect counts. It appends test fixtures to the raw topic and never
deletes topics or data. Use a development broker without concurrent producers.

This verifies the producer → Kafka → consumer path. It does not demonstrate that
Spark rejects bad events, that OpenSearch deduplicates or preserves the latest
version, or that `/ask` reflects an update. Those require the downstream owners'
integration run. No exactly-once claim is made.
