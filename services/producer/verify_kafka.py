"""Opt-in real-broker ingestion check. Publishes fixtures; never deletes topics/data."""

import argparse
import json
import os
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient

from services.producer.events import ROOT, SAMPLE, utc_string, validate_event


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    parser.add_argument("--topic", default=os.getenv("RAW_TOPIC", "streamrag.raw.v1"))
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = {
        "started_at": utc_string(datetime.now(UTC)),
        "bootstrap_servers": args.bootstrap_servers,
        "topic": args.topic,
        "status": "failed",
        "checks": [],
    }
    consumer = None

    def command(arguments):
        result = subprocess.run(
            [sys.executable, *arguments],
            cwd=ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=90,
        )
        require(result.returncode == 0, f"Command failed: {arguments}: {result.stderr[-2000:]}")
        return result.stdout + result.stderr

    def receive(count):
        messages = []
        deadline = time.monotonic() + 45
        while len(messages) < count and time.monotonic() < deadline:
            message = consumer.poll(1)
            if message is None:
                continue
            require(not message.error(), str(message.error()))
            messages.append(message)
        require(len(messages) == count, f"Consumed {len(messages)}/{count} expected messages")
        return messages

    def end_offset(partition):
        # Topic creation can complete before a partition leader is ready to answer.
        deadline = time.monotonic() + 30
        while True:
            try:
                return consumer.get_watermark_offsets(
                    TopicPartition(args.topic, partition), timeout=5
                )[1]
            except KafkaException:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.25)

    def phase(name, arguments, expected):
        before = datetime.now(UTC)
        output = command(
            [*arguments, "--bootstrap-servers", args.bootstrap_servers, "--topic", args.topic]
        )
        messages = receive(len(expected))
        after = datetime.now(UTC)
        require(
            f"acknowledged: {len(expected)} records topic={args.topic}" in output,
            "Producer did not acknowledge the expected record count",
        )
        by_id = {json.loads(message.value())["event_id"]: message for message in messages}
        require(len(by_id) == len(expected), f"Unexpected duplicate IDs in {name}")
        for event in expected:
            message = by_id[event["event_id"]]
            actual = json.loads(message.value())
            validate_event(actual)
            require(message.key() == event["document_id"].encode("utf-8"), "Wrong Kafka key")
            require(
                {k: v for k, v in actual.items() if k != "ingested_at"}
                == {k: v for k, v in event.items() if k != "ingested_at"},
                "Payload changed",
            )
            ingested = datetime.fromisoformat(actual["ingested_at"].replace("Z", "+00:00"))
            require(before <= ingested <= after, "ingested_at was not stamped at publish time")
        report["checks"].append(
            {
                "name": name,
                "acknowledged": len(expected),
                "consumed": len(messages),
                "key_equals_document_id": True,
            }
        )
        print(f"PASS {name}: acknowledged={len(expected)} consumed={len(messages)}", flush=True)
        return by_id

    try:
        init_args = [
            "infra/kafka/init_topics.py",
            "--bootstrap-servers",
            args.bootstrap_servers,
            "--raw-topic",
            args.topic,
        ]
        command(init_args)
        second = command(init_args)
        require("exists topic=" in second, "Topic initialization was not idempotent")
        metadata = AdminClient({"bootstrap.servers": args.bootstrap_servers}).list_topics(
            timeout=15
        )
        require(args.topic in metadata.topics, "Raw topic is missing")
        partitions = sorted(metadata.topics[args.topic].partitions)
        require(len(partitions) == 3, "Expected three raw partitions")
        cleaned = os.getenv("CLEANED_TOPIC", "streamrag.cleaned.v1")
        require(
            cleaned in metadata.topics and len(metadata.topics[cleaned].partitions) == 3,
            "Expected three cleaned partitions",
        )
        report["checks"].append(
            {
                "name": "topic_initialization",
                "idempotent": True,
                "raw_partitions": len(partitions),
                "cleaned_partitions": 3,
            }
        )
        consumer = Consumer(
            {
                "bootstrap.servers": args.bootstrap_servers,
                "group.id": f"tv2-verify-{uuid.uuid4().hex}",
                "enable.auto.commit": False,
            }
        )
        starts = [
            TopicPartition(
                args.topic,
                partition,
                end_offset(partition),
            )
            for partition in partitions
        ]
        consumer.assign(starts)
        report["starting_offsets"] = {str(item.partition): item.offset for item in starts}
        seed = [
            json.loads(line) for line in (SAMPLE / "seed_events.jsonl").read_text().splitlines()
        ]
        live = [
            json.loads(line) for line in (SAMPLE / "live_events.jsonl").read_text().splitlines()
        ]
        seed_messages = phase("seed", ["-m", "services.producer.cli"], seed)
        live_messages = phase(
            "live_file",
            [
                "scripts/stream_simulator.py",
                "--event-file",
                "data/sample/live_events.jsonl",
                "--rate",
                "50",
            ],
            live,
        )
        atlas_seed = seed_messages[seed[0]["event_id"]]
        atlas_live = live_messages[live[0]["event_id"]]
        require(
            atlas_seed.partition() == atlas_live.partition()
            and atlas_seed.offset() < atlas_live.offset(),
            "Atlas update lost per-key ordering",
        )
        report["atlas"] = {
            "document_id": "inc-atlas-001",
            "seed_version": 1,
            "live_version": 2,
            "before": "20:30 UTC",
            "after": "21:30 UTC",
            "partition": atlas_live.partition(),
            "seed_offset": atlas_seed.offset(),
            "live_offset": atlas_live.offset(),
        }
        phase(
            "atlas_update", ["scripts/stream_simulator.py", "--update", "inc-atlas-001"], live[:1]
        )
        phase("seed_replay", ["-m", "services.producer.cli", "--count", "10"], seed[:10])
        output = command(
            [
                "scripts/stream_simulator.py",
                "--edge-cases",
                "--rate",
                "100",
                "--bootstrap-servers",
                args.bootstrap_servers,
                "--topic",
                args.topic,
            ]
        )
        expected_bytes = [
            line.encode("utf-8") for line in (SAMPLE / "edge_cases.jsonl").read_text().splitlines()
        ]
        edge_messages = receive(len(expected_bytes))
        require(
            sorted(message.value() for message in edge_messages) == sorted(expected_bytes),
            "Edge-case wire bytes changed",
        )
        require("acknowledged: 26 records" in output, "Edge-case delivery was not acknowledged")
        atlas = [
            json.loads(message.value())
            for message in edge_messages
            if message.key() == b"inc-atlas-001" and b"broken-json" not in message.value()
        ]
        versions = [
            event["version"] for event in atlas if not event["event_id"].startswith("evt-invalid")
        ]
        require(versions == [1, 2, 1], "Expected duplicate v1, update v2, then stale v1 for Atlas")
        report["checks"].append(
            {
                "name": "edge_cases",
                "acknowledged": 26,
                "consumed": 26,
                "wire_bytes_unchanged": True,
                "atlas_versions": versions,
            }
        )
        report["status"] = "passed"
        print("PASS edge_cases: acknowledged=26 consumed=26; exact bytes preserved", flush=True)
    except Exception as error:
        report["error"] = str(error)
        print(f"FAIL: {error}", file=sys.stderr)
    finally:
        if consumer is not None:
            consumer.close()
        report["finished_at"] = utc_string(datetime.now(UTC))
        report["scope"] = "Real Kafka ingestion only; Spark/Indexer/API behavior is not verified."
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        else:
            print(json.dumps(report, indent=2))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
