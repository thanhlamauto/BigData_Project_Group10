"""Descriptive live smoke: only publish through Kafka, never write directly to the index."""

import argparse
import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.producer.events import edge_cases, live_events, seed_events, utc_string  # noqa: E402
from services.producer.kafka_producer import EventProducer  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument(
        "--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    parser.add_argument("--topic", default=os.getenv("RAW_TOPIC", "streamrag.raw.v1"))
    parser.add_argument(
        "--opensearch-url", default=os.getenv("OPENSEARCH_URL", "http://localhost:9200")
    )
    parser.add_argument("--index", default=os.getenv("OPENSEARCH_INDEX", "streamrag-docs-v1"))
    parser.add_argument("--report", type=Path, default=Path("data/generated/pipeline-report.json"))
    parser.add_argument("--timeout", type=float, default=120)
    args = parser.parse_args()
    report = {"started_at": utc_string(datetime.now(UTC)), "status": "failed", "checks": []}
    producer = EventProducer(args.bootstrap_servers, args.topic)
    question = "When does Atlas Search maintenance start? document_id=inc-atlas-001"
    suffix = uuid4().hex[:10]

    def check(condition, message):
        if not condition:
            raise RuntimeError(message)

    with httpx.Client(base_url=args.base_url, timeout=60) as client:

        def ask(query):
            response = client.post("/ask", json={"question": query, "top_k": 3})
            response.raise_for_status()
            return response.json()

        def wait_for(query, document_id, version, fact):
            deadline = time.monotonic() + args.timeout
            while time.monotonic() < deadline:
                result = ask(query)
                matches = [
                    source for source in result["sources"] if source["document_id"] == document_id
                ]
                if matches and matches[0]["version"] == version and fact in result["answer"]:
                    return result
                time.sleep(1)
            raise RuntimeError(f"Timed out waiting for {document_id} v{version}")

        def publish(event, query, fact):
            outgoing = producer.publish(event)
            producer.flush()
            result = wait_for(query, event["document_id"], event["version"], fact)
            first_visible = datetime.now(UTC)
            ingested = datetime.fromisoformat(outgoing["ingested_at"].replace("Z", "+00:00"))
            report["checks"].append(
                {
                    "document_id": event["document_id"],
                    "version": event["version"],
                    "ingested_at": outgoing["ingested_at"],
                    "first_visible_at": utc_string(first_visible),
                    "freshness_seconds": round((first_visible - ingested).total_seconds(), 3),
                    "response": result,
                }
            )
            print(f"PASS visible {event['document_id']} v{event['version']}", flush=True)

        try:
            health = client.get("/health")
            health.raise_for_status()
            check(health.json()["retriever_backend"] == "opensearch", "Real OpenSearch required")
            report["seed"] = wait_for(question, "inc-atlas-001", 1, "20:30")
            print("PASS seed v1 20:30", flush=True)
            event = dict(
                seed_events(n=1)[0],
                event_id=f"evt-live-{suffix}",
                document_id=f"quasar-{suffix}",
                title=f"Quasar Beacon {suffix}",
                body=f"Quasar Beacon {suffix} status: accepted. Recovery completed at 23:47 UTC.",
                category="status",
                event_time=utc_string(datetime.now(UTC)),
            )
            publish(event, f"Quasar Beacon {suffix} status?", "23:47")
            publish(live_events()[0], question, "21:30")
            for index, line in enumerate(edge_cases()[0]):
                try:
                    key = json.loads(line).get("document_id", f"invalid-{index}")
                except ValueError:
                    key = f"invalid-{index}"
                producer.publish_raw(line.encode(), key)
            producer.flush()
            barrier = dict(
                event,
                event_id=f"evt-barrier-{suffix}",
                document_id=f"barrier-{suffix}",
                title=f"Progress Barrier {suffix}",
                body=f"Progress Barrier {suffix} accepted.",
            )
            publish(barrier, f"Progress Barrier {suffix}", "accepted")
            final = wait_for(question, "inc-atlas-001", 2, "21:30")
            check(
                "20:30" not in final["answer"],
                "Old maintenance time remained in current document answer",
            )
            ids = [source["document_id"] for source in final["sources"]]
            check(len(ids) == len(set(ids)), "Duplicate API sources")
            with httpx.Client(base_url=args.opensearch_url, timeout=30) as search:
                search.post(f"/{args.index}/_refresh").raise_for_status()
                document = search.get(f"/{args.index}/_doc/inc-atlas-001")
                document.raise_for_status()
                check(document.json()["_version"] == 2, "Stale revision replaced version 2")
                count = search.get(f"/{args.index}/_count")
                count.raise_for_status()
                report["index_count"] = count.json()["count"]
            report["final"] = final
            report["status"] = "passed"
            print(
                "PASS edge cases, progress barrier, unique sources and preserved Atlas v2",
                flush=True,
            )
        except Exception as error:
            report["error"] = str(error)
            print(f"FAIL {error}", file=sys.stderr)
        finally:
            report["finished_at"] = utc_string(datetime.now(UTC))
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
