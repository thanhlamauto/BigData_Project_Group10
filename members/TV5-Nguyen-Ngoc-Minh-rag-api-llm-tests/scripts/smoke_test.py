"""API evaluation and live-pipeline checks. Never substitutes direct index writes for Kafka."""

import argparse
import json
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx

MODULE_ROOT = Path(__file__).resolve().parents[1]


class SmokeFailure(Exception):
    pass


def read_jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


def case_matches(result: dict, case: dict) -> bool:
    sources = result["sources"]
    ids = [source["document_id"] for source in sources]
    if len(ids) != len(set(ids)):
        raise SmokeFailure("Duplicate document_id in API sources")
    expected_id = case["expected_document_id"]
    matches = [source for source in sources if source["document_id"] == expected_id]
    correct_version = matches and (
        case.get("expected_version") is None or matches[0]["version"] == case["expected_version"]
    )
    return bool(correct_version) and all(
        fact.casefold() in result["answer"].casefold() for fact in case["expected_facts"]
    )


def ask(client: httpx.Client, case: dict, timeout: float) -> dict:
    response = client.post(
        "/ask",
        json={"question": case["question"], "top_k": case.get("top_k", 3)},
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def wait_case(
    client: httpx.Client, case: dict, timeout: float, interval: float
) -> tuple[dict, datetime]:
    deadline = time.monotonic() + timeout
    while (remaining := deadline - time.monotonic()) > 0:
        try:
            result = ask(client, case, min(remaining, 30))
            visible_at = datetime.now(UTC)
            if case_matches(result, case):
                return result, visible_at
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code not in {502, 503, 504}:
                raise SmokeFailure(f"/ask returned HTTP {exc.response.status_code}") from exc
        except httpx.RequestError:
            pass
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(min(interval, remaining))
    raise SmokeFailure(f"Timed out waiting for {case['expected_document_id']}")


def publish(command: list[str], events: list[dict], stamp: bool = True) -> list[dict]:
    records = [dict(event) for event in events]
    if stamp:
        for record in records:
            record["ingested_at"] = datetime.now(UTC).isoformat()
    completed = subprocess.run(
        command,
        input="".join(json.dumps(event) + "\n" for event in records),
        text=True,
        encoding="utf-8",
        capture_output=True,
        timeout=30,
        check=False,
    )
    if completed.returncode:
        raise SmokeFailure(f"Kafka publisher exited {completed.returncode}")
    return records


def record_freshness(event: dict, visible_at: datetime) -> dict:
    ingested = datetime.fromisoformat(event["ingested_at"].replace("Z", "+00:00"))
    if ingested.tzinfo is None:
        raise SmokeFailure("ingested_at must include a timezone")
    freshness = (visible_at - ingested).total_seconds()
    if freshness < 0:
        raise SmokeFailure("Negative freshness; check producer/client clock synchronization")
    evidence = {
        "document_id": event["document_id"],
        "version": event["version"],
        "event_time": event["event_time"],
        "ingested_at": event["ingested_at"],
        "first_visible_at": visible_at.isoformat(),
        "freshness_seconds": round(freshness, 3),
    }
    print(json.dumps(evidence))
    return evidence


def evaluate(client: httpx.Client, cases: list[dict]) -> dict:
    hits = facts = 0
    for case in cases:
        result = ask(client, case, 30)
        sources = result["sources"]
        ids = [source["document_id"] for source in sources]
        if len(ids) != len(set(ids)):
            raise SmokeFailure("Duplicate document_id in API sources")
        hits += case["expected_document_id"] in ids
        facts += all(
            fact.casefold() in result["answer"].casefold() for fact in case["expected_facts"]
        )
        if not case_matches(result, case):
            raise SmokeFailure(f"Seed case failed for {case['expected_document_id']}")
    metrics = {
        "cases": len(cases),
        "hit_at_k": hits / len(cases),
        "fact_pass_rate": facts / len(cases),
    }
    print(json.dumps(metrics))
    return metrics


def run_checks(args: argparse.Namespace) -> None:
    if args.timeout <= 0 or args.poll_interval <= 0:
        raise SmokeFailure("Timeout and poll interval must be positive")
    cases = read_jsonl(args.test_cases)
    seed_cases = [case for case in cases if case.get("phase") == "seed"]
    live_cases = [case for case in cases if case.get("phase") in {"stream", "revision", "update"}]
    if not seed_cases:
        raise SmokeFailure("At least one seed test case is required")
    command = None
    external_events = read_jsonl(args.events_file) if args.events_file else []
    if not args.seed_only:
        if not args.publish_command:
            raise SmokeFailure(
                "Full smoke requires --publish-command (JSON argv accepting JSONL stdin)"
            )
        command = json.loads(args.publish_command)
        if (
            not isinstance(command, list)
            or not command
            or not all(isinstance(part, str) and part for part in command)
        ):
            raise SmokeFailure("--publish-command must be a nonempty JSON array of strings")
        phases = {case["phase"] for case in live_cases}
        if "stream" not in phases or not phases & {"revision", "update"}:
            raise SmokeFailure("Full smoke requires stream and revision cases")

    with httpx.Client(base_url=args.base_url.rstrip("/"), timeout=30) as client:
        response = client.get("/health")
        response.raise_for_status()
        health = response.json()
        if health["status"] != "ok":
            raise SmokeFailure("API health is not ok")
        if not args.seed_only and health["retriever_backend"] != "opensearch":
            raise SmokeFailure("Full smoke requires the real OpenSearch backend")
        if args.require_real_llm and health["llm_provider"] == "mock":
            raise SmokeFailure("Real LLM check requested but API uses mock")
        print("PASS health")
        evaluate(client, seed_cases)
        print("PASS seed")
        if args.seed_only:
            print("PASS seed-only; stream/revision/fault/freshness not checked")
            return

        revision_event = revision_case = None
        for case in live_cases:
            events = case.get("events") or [
                event
                for event in external_events
                if event.get("document_id") == case["expected_document_id"]
                and event.get("version") == case.get("expected_version", event.get("version"))
            ]
            if not events:
                raise SmokeFailure(f"Missing publishable events for {case['expected_document_id']}")
            if case["phase"] == "stream":
                before = ask(client, case, 30)
                if case["expected_document_id"] in {
                    source["document_id"] for source in before["sources"]
                }:
                    raise SmokeFailure(
                        "Stream event already exists; use a clean index or a fresh ID"
                    )
            else:
                if not case.get("expected_version"):
                    raise SmokeFailure("Revision cases require expected_version")
                before = ask(client, case, 30)
                old = [
                    source
                    for source in before["sources"]
                    if source["document_id"] == case["expected_document_id"]
                ]
                if not old or old[0]["version"] >= case["expected_version"]:
                    raise SmokeFailure("Revision must start from a visible older version")
            sent = publish(command, events)
            result, visible_at = wait_case(client, case, args.timeout, args.poll_interval)
            matching = [
                event for event in sent if event["document_id"] == case["expected_document_id"]
            ]
            event = max(matching, key=lambda item: item["version"])
            evidence = record_freshness(event, visible_at)
            source = next(
                source
                for source in result["sources"]
                if source["document_id"] == event["document_id"]
            )
            if source.get("indexed_at"):
                print(json.dumps({"indexed_at": source["indexed_at"], **evidence}))
            if case["phase"] in {"revision", "update"}:
                revision_event, revision_case = event, case
            print(f"PASS {case['phase']}")

        # A valid barrier behind malformed/duplicate records proves the pipeline makes progress.
        suffix = uuid4().hex[:12]
        barrier = {
            **revision_event,
            "event_id": f"evt-fault-barrier-{suffix}",
            "document_id": f"fault-barrier-{suffix}",
            "version": 1,
            "title": f"Fault barrier {suffix}",
            "body": f"Fault barrier {suffix} accepted.",
            "event_time": datetime.now(UTC).isoformat(),
            "ingested_at": datetime.now(UTC).isoformat(),
        }
        publish(command, [{"event_id": "malformed-smoke"}, revision_event, barrier], stamp=False)
        barrier_case = {
            "question": f"Fault barrier {suffix}",
            "expected_document_id": barrier["document_id"],
            "expected_version": 1,
            "expected_facts": [suffix, "accepted"],
        }
        wait_case(client, barrier_case, args.timeout, args.poll_interval)
        if not case_matches(ask(client, revision_case, 30), revision_case):
            raise SmokeFailure("Revision changed or became unavailable after fault inputs")
        print("PASS fault (API sources + progress barrier; inspect indexer counters separately)")
        if health["llm_provider"] == "mock":
            print("SKIP Case 5 real generation: LLM_PROVIDER=mock")
        else:
            print(f"PASS Case 5 generation via {health['llm_provider']}")
        print("PASS full smoke Cases 1-4")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument(
        "--test-cases", type=Path, default=MODULE_ROOT / "fixtures/test_cases.jsonl"
    )
    parser.add_argument("--events-file", type=Path)
    parser.add_argument(
        "--publish-command", help="JSON argv; reads JSONL from stdin; publishes to Kafka"
    )
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--poll-interval", type=float, default=1)
    parser.add_argument("--seed-only", action="store_true")
    parser.add_argument("--require-real-llm", action="store_true")
    args = parser.parse_args(argv)
    try:
        run_checks(args)
    except (
        SmokeFailure,
        httpx.HTTPError,
        OSError,
        ValueError,
        KeyError,
        TypeError,
        IndexError,
        subprocess.SubprocessError,
    ) as exc:
        # Do not echo upstream bodies, publisher stderr, credentials or the publisher command.
        detail = str(exc) if isinstance(exc, SmokeFailure) else type(exc).__name__
        print(f"FAIL {detail}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
