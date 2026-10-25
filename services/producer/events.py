"""Deterministic fixtures and dependency-free validation of the v1 contract."""

import json
import random
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SAMPLE = ROOT / "data" / "sample"
BASE_TIME = datetime(2026, 10, 26, 9, tzinfo=UTC)
SERVICES = (
    ("atlas", "Atlas Search"),
    ("payment", "Payment API"),
    ("gateway", "Data Gateway"),
    ("identity", "Identity Service"),
    ("storage", "Object Storage"),
    ("notify", "Notification Hub"),
    ("analytics", "Analytics Engine"),
    ("scheduler", "Job Scheduler"),
)
REQUIRED = {
    "schema_version",
    "event_id",
    "document_id",
    "version",
    "operation",
    "source",
    "title",
    "body",
    "category",
    "event_time",
    "ingested_at",
}
TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")
CATEGORIES = {"incident", "maintenance", "resolved", "status"}


def utc_string(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def validate_event(event: object) -> None:
    """Reject malformed records before publishing; JSON Schema is the wire contract."""
    if not isinstance(event, dict):
        raise ValueError("event must be a JSON object")
    if REQUIRED - event.keys() or event.keys() - (REQUIRED | {"source_url"}):
        raise ValueError("missing required fields or unknown fields")
    if type(event["schema_version"]) is not int or event["schema_version"] != 1:
        raise ValueError("schema_version must be integer 1")
    if type(event["version"]) is not int or event["version"] < 1:
        raise ValueError("version must be an integer >= 1")
    if (
        event["operation"] != "upsert"
        or not isinstance(event["category"], str)
        or event["category"] not in CATEGORIES
    ):
        raise ValueError("unsupported operation/category")
    for field in ("event_id", "document_id", "source", "title", "body"):
        if not isinstance(event[field], str) or not event[field].strip():
            raise ValueError(f"{field} must be a nonblank string")
    if event.get("source_url") is not None and (
        not isinstance(event["source_url"], str) or not event["source_url"].strip()
    ):
        raise ValueError("source_url must be null or a nonblank string")
    for field in ("event_time", "ingested_at"):
        value = event[field]
        if not isinstance(value, str) or not TIMESTAMP.fullmatch(value):
            raise ValueError(f"{field} must be a UTC timestamp ending in Z")
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"invalid {field}") from exc


def seed_events(seed: int = 42, n: int = 400) -> list[dict]:
    if n < 1:
        raise ValueError("n must be >= 1")
    rng = random.Random(seed)
    events = []
    for index in range(n):
        slug, service = SERVICES[index % len(SERVICES)]
        number = index // len(SERVICES) + 1
        clock = (
            "20:30" if index == 0 else f"{rng.randrange(24):02d}:{rng.choice((0, 15, 30, 45)):02d}"
        )
        date = f"2026-10-{27 + index % 5:02d}"
        category = "maintenance" if index == 0 else rng.choice(sorted(CATEGORIES))
        body = {
            "maintenance": f"{service} maintenance starts at {clock} UTC on {date}. "
            "Expected duration is 30 minutes. Status: scheduled.",
            "incident": f"{service} is experiencing elevated error rates since {clock} UTC "
            f"on {date}. Status: investigating. Next update in 15 minutes.",
            "resolved": f"{service} incident was resolved at {clock} UTC on {date}. "
            "Status: operational. All requests are being processed normally.",
            "status": f"{service} status at {clock} UTC on {date}: operational. "
            "No customer impact is expected.",
        }[category]
        events.append(
            {
                "schema_version": 1,
                "event_id": f"evt-seed-{seed}-{index + 1:06d}",
                "document_id": f"inc-{slug}-{number:03d}",
                "version": 1,
                "operation": "upsert",
                "source": "synthetic-status",
                "source_url": None,
                "title": f"{service} {category} notice {number:03d}",
                "body": body,
                "category": category,
                "event_time": utc_string(BASE_TIME + timedelta(seconds=index)),
                "ingested_at": utc_string(BASE_TIME + timedelta(seconds=index + 3)),
            }
        )
    return events


def live_events(seed: int = 42) -> list[dict]:
    originals = seed_events(seed, 10)
    events = []
    for index, original in enumerate(originals):
        event = dict(original)
        event.update(
            event_id=f"evt-live-{seed}-{index + 1:06d}",
            version=2 if index < 5 else 1,
            document_id=original["document_id"] if index < 5 else f"live-{index + 1:03d}",
            event_time=utc_string(BASE_TIME + timedelta(days=1, minutes=index)),
            ingested_at=utc_string(BASE_TIME + timedelta(days=1, minutes=index, seconds=3)),
        )
        if index == 0:
            event["body"] = (
                "Atlas Search maintenance starts at 21:30 UTC on 2026-10-27. "
                "Expected duration is 30 minutes. Status: scheduled."
            )
        else:
            event["category"] = "resolved"
            service = SERVICES[index % len(SERVICES)][1]
            event["body"] = (
                f"{service} is operational after recovery at 22:{index:02d} UTC "
                "on 2026-10-27. Status: resolved. Error rate is below 0.1 percent."
            )
        event["title"] += " live update"
        events.append(event)
    return events


def question_cases(seed: int = 42) -> list[dict]:
    cases = []
    for phase, events in (("seed", seed_events(seed, 10)), ("after_stream", live_events(seed))):
        for index, event in enumerate(events):
            cases.append(
                {
                    "question": "When does Atlas Search maintenance start?"
                    if index == 0
                    else f"What does {event['title']} report?",
                    "expected_document_id": event["document_id"],
                    "expected_version": event["version"],
                    "expected_facts": [event["body"].split(". ")[0]],
                    "phase": phase,
                }
            )
    return cases


def edge_cases(seed: int = 42) -> tuple[list[str], list[dict]]:
    """26 ordered lines: 10 duplicate, 5 late, 5 update, 1 stale, 5 invalid."""
    originals = seed_events(seed, 10)
    lines, expectations = [], []

    def add(value: object, kind: str, expected: str, *, raw: bool = False) -> None:
        lines.append(value if raw else json.dumps(value, ensure_ascii=False, sort_keys=True))
        expectations.append({"line": len(lines), "kind": kind, "expected": expected})

    for event in originals:
        add(event, "duplicate", "Same event_id as seed; no additional searchable document.")
    for index, original in enumerate(originals[:5]):
        event = dict(
            original,
            event_id=f"evt-late-{seed}-{index}",
            document_id=f"late-{index:03d}",
            event_time="2026-10-20T09:00:00Z",
        )
        add(event, "late", "Valid late event; watermark policy belongs to Spark; log outcome.")
    for event in live_events(seed)[:5]:
        add(event, "update", "Version 2 replaces seed version 1.")
    add(
        dict(originals[0], event_id=f"evt-stale-{seed}-atlas"),
        "stale",
        "Version 1 arrives after version 2; never overwrite version 2.",
    )
    add('{"event_id": "broken-json",', "invalid", "Reject malformed JSON; count error.", raw=True)
    missing = dict(originals[0], event_id="evt-invalid-missing")
    del missing["document_id"]
    add(missing, "invalid", "Reject missing document_id; count error.")
    add(
        dict(originals[0], event_id="evt-invalid-body", body="   "),
        "invalid",
        "Reject whitespace-only body; count error.",
    )
    add(
        dict(originals[0], event_id="evt-invalid-time", event_time="not-a-date"),
        "invalid",
        "Reject invalid timestamp; count error.",
    )
    add(
        dict(originals[0], event_id="evt-invalid-version", version=0),
        "invalid",
        "Reject version below 1; count error.",
    )
    return lines, expectations


def write_jsonl(path: Path, events: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(e, ensure_ascii=False, sort_keys=True) + "\n" for e in events),
        encoding="utf-8",
        newline="\n",
    )
