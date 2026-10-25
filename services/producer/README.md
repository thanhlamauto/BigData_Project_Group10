# TV2 — Data & Kafka ingestion

Owner: Nguyễn Thanh Lâm. Tasks: M1-05 (#7), M1-06 (#8), M1-07 (#9).
Run commands below from the repository root with **Python 3.11+**.

## Install and check offline

```bash
python -m pip install -r services/producer/requirements-test.txt
python -m pip install -r members/TV1-Pham-Chi-Bang-lead-architecture-devops/requirements/dev.txt
python scripts/generate_data.py --seed 42 --n 400 --all
python -m pytest tests/unit -q
python -m services.producer.cli --dry-run --count 2
python scripts/stream_simulator.py --update inc-atlas-001 --dry-run
```

Normal replay validates the entire file before publishing. `--dry-run` writes input
JSONL to stdout and a summary to stderr, never connects to Kafka, and does not
stamp a new ingestion time. The generator never uses the current time.

## Kafka: seed, consume, live update

Start the Kafka service from TV1's Compose file (no need to start the other services
for this module check). `.env.example` supplies development defaults:

```bash
docker compose --env-file members/TV1-Pham-Chi-Bang-lead-architecture-devops/.env.example -f members/TV1-Pham-Chi-Bang-lead-architecture-devops/infra/compose.yaml up -d --wait kafka
python infra/kafka/init_topics.py
python -m services.producer.cli
```

The topic initializer creates `streamrag.raw.v1` and `streamrag.cleaned.v1` with
3 partitions and replication factor 1. Running it again accepts existing topics;
it does not alter their configuration. A broker error returns nonzero.

Real-broker verification (publishes 447 fixture records; use a development broker):

```bash
python -m services.producer.verify_kafka --report data/generated/kafka-verification.json
```

This checks delivery acknowledgements, consumed counts, keys, payloads, timestamps,
per-document ordering and exact edge-case bytes. Existing records are skipped by
snapshotting end offsets. [Verification evidence](evidence/README.md) records a
successful run on 26/10/2026. Downstream Spark/Indexer/API behavior needs its own test.

On this Windows machine Docker Engine runs inside Ubuntu WSL2. From PowerShell,
prefix Docker commands with `wsl -d Ubuntu-22.04 -u root --`, for example:

```powershell
wsl -d Ubuntu-22.04 -u root -- docker ps
wsl -d Ubuntu-22.04 -u root -- service docker start
```

The second command starts the daemon again after a WSL shutdown. Python ingestion
can run on Windows using `localhost:9092`; run Compose commands inside WSL from the
repository's `/mnt/c/...` path.

Inspect actual keys and values with the Kafka CLI inside the running container:

```bash
docker exec streamrag-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic streamrag.raw.v1 --from-beginning --max-messages 2 --timeout-ms 15000 --property print.key=true
python scripts/stream_simulator.py --event-file data/sample/live_events.jsonl --rate 2
python scripts/stream_simulator.py --update inc-atlas-001
python scripts/stream_simulator.py --rate 5 --count 20 --seed 42
python scripts/stream_simulator.py --edge-cases --rate 2
```

The fixed update changes `inc-atlas-001` v1 (20:30 UTC) to v2 (21:30 UTC).
The live fixture contains five updates and five documents absent from the seed.
For Q&A validation, seed first, run the live fixture second, then evaluate the
corresponding `phase` in `test_cases.jsonl`. `--update` is stable/retryable;
the generated stream uses a unique run ID and current event time for new records.
`--seed` controls its content; runtime IDs and timestamps are intentionally fresh.

`--edge-cases` explicitly bypasses validation and sends the committed bytes,
including malformed JSON, unchanged. This preserves the intended bad timestamps,
missing fields, duplicates, and ordering. It must follow seed ingestion. Its
`ingested_at` fields are fixture timestamps, unsuitable for measuring freshness.

Configuration: `KAFKA_BOOTSTRAP_SERVERS` / `RAW_TOPIC` environment variables or
`--bootstrap-servers` / `--topic`. The producer runs on the host, so the default
is `localhost:9092`; a container on the Compose network uses `kafka:29092`.
The CLI reads environment variables, not `.env` files. Supply overrides explicitly
if you changed TV1's defaults.

## Make entry points and integration handoff

```bash
make -C members/TV2-Nguyen-Thanh-Lam-data-kafka-ingestion init-topics
make -C members/TV2-Nguyen-Thanh-Lam-data-kafka-ingestion seed
make -C members/TV2-Nguyen-Thanh-Lam-data-kafka-ingestion simulate ARGS="--update inc-atlas-001"
make -C members/TV2-Nguyen-Thanh-Lam-data-kafka-ingestion test
```

Set `PYTHON` to your virtual environment interpreter if needed. On Windows without
Make, use the equivalent Python commands above. The shared TV1 Makefile now also
provides `init-topics`, `seed`, and `simulate`:

```bash
cd members/TV1-Pham-Chi-Bang-lead-architecture-devops
make ENV_FILE=.env.example init-topics seed
make ENV_FILE=.env.example simulate INGEST_ARGS="--update inc-atlas-001"
```

Install producer requirements in the selected `PYTHON` interpreter first. Shared
targets export the selected environment file, including custom broker/topic values,
and accept `INGEST_ARGS` for CLI overrides. CRLF environment files are supported.
When calling a Windows Python executable from WSL, also export
`WSLENV=RAW_TOPIC:CLEANED_TOPIC:KAFKA_BOOTSTRAP_SERVERS` so these variables cross
the WSL/Windows boundary. A native Linux Python interpreter needs no such bridge.

The shared targets and continuous Kafka → Spark ingestion were exercised with
400 seed events, 10 live events and 26 edge cases. See the
[Spark integration evidence](evidence/spark-2026-10-26.md) for counts, rejected
records, reproduction commands and remaining downstream limitations.

## Delivery behavior and limits

Messages use UTF-8 JSON, key = `document_id`, `acks=all`, and idempotent Kafka
producer delivery. `ingested_at` is stamped at each normal publish; `event_id`,
`document_id`, `version`, and `event_time` survive replay. Queued messages are logged
with event/document/version/ingestion time; delivery callbacks log partition and
offset. The final count reports acknowledged records only. Delivery errors, queue
timeouts, or incomplete flush return failure. Queue pressure and flush are bounded
to 30 seconds by default. A partially successful run may be replayed with the same
event IDs, which requires downstream deduplication and version-aware upserts.

Kafka idempotence does not deduplicate separate application replays or make the
Kafka → Spark → Parquet → OpenSearch pipeline exactly-once. Spark/Indexer owners
must validate bad input, count rejects, define the late-event watermark policy,
and prevent old versions overwriting new versions. No end-to-end success is
claimed by the offline tests.

References: [Confluent Python client](https://docs.confluent.io/kafka-clients/python/current/overview.html),
[JSON Schema 2020-12](https://json-schema.org/draft/2020-12).
