"""Seed replay and live simulator entry points. All modes support offline --dry-run."""

import argparse
import json
import logging
import math
import os
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

from services.producer.events import SAMPLE, live_events, seed_events, utc_string, validate_event
from services.producer.kafka_producer import EventProducer


def positive_rate(value):
    rate = float(value)
    if not math.isfinite(rate) or rate <= 0:
        raise argparse.ArgumentTypeError("rate must be finite and > 0")
    return rate


def positive_count(value):
    count = int(value)
    if count < 1:
        raise argparse.ArgumentTypeError("count must be >= 1")
    return count


def run(
    argv=None,
    *,
    simulator=False,
    producer_factory=EventProducer,
    sleep=time.sleep,
    monotonic=time.monotonic,
):
    parser = argparse.ArgumentParser(description="StreamRAG Kafka ingestion")
    parser.add_argument(
        "--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    parser.add_argument("--topic", default=os.getenv("RAW_TOPIC", "streamrag.raw.v1"))
    parser.add_argument("--dry-run", action="store_true", help="Write JSONL to stdout, no Kafka")
    parser.add_argument("--count", type=positive_count, default=None)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--rate", type=positive_rate, default=1.0)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--event-file", type=Path)
    if simulator:
        group.add_argument("--update", choices=["inc-atlas-001"])
        group.add_argument("--edge-cases", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stderr)
    try:
        edge = simulator and args.edge_cases
        if edge:
            lines = (SAMPLE / "edge_cases.jsonl").read_text(encoding="utf-8").splitlines()
        elif args.event_file or not simulator:
            path = args.event_file or SAMPLE / "seed_events.jsonl"
            lines = path.read_text(encoding="utf-8").splitlines()
        elif args.update:
            # A retryable update keeps its stable event ID and version.
            lines = [json.dumps(live_events(args.seed)[0])]
        else:
            # New simulation run, new document/event IDs; no collisions with the seed corpus.
            run_id = uuid.uuid4().hex
            events = seed_events(args.seed, args.count or 10)
            for event in events:
                event["event_id"] = f"evt-sim-{run_id}-{event['event_id']}"
                event["document_id"] = f"sim-{run_id}-{event['document_id']}"
                event["event_time"] = utc_string(datetime.now(UTC))
            lines = [json.dumps(event) for event in events]
        if args.count is not None:
            lines = lines[: args.count]
        if not lines:
            raise ValueError("input contains no events")
        # Validate the entire input before sending any part of a normal replay.
        if not edge:
            for line in lines:
                validate_event(json.loads(line))
        if args.dry_run:
            for line in lines:
                print(line)
            print(f"dry-run: {len(lines)} records; Kafka not contacted", file=sys.stderr)
            return 0
        producer = producer_factory(args.bootstrap_servers, args.topic)
        start = monotonic()
        try:
            for index, line in enumerate(lines):
                if simulator and index:
                    sleep(max(0, start + index / args.rate - monotonic()))
                if edge:
                    try:
                        record = json.loads(line)
                        key = record.get("document_id", f"invalid-line-{index + 1}")
                    except json.JSONDecodeError:
                        key = f"invalid-line-{index + 1}"
                    producer.publish_raw(line.encode("utf-8"), key)
                    logging.info("queued edge_line=%s key=%s", index + 1, key)
                else:
                    producer.publish(json.loads(line))
        finally:
            producer.flush()
        print(f"acknowledged: {producer.delivered} records topic={args.topic}", file=sys.stderr)
        return 0
    except (OSError, ValueError, RuntimeError, TimeoutError, ImportError) as exc:
        print(f"ingestion failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(run())
