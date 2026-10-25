import json

import pytest

from services.producer.events import ROOT, SAMPLE, seed_events, validate_event


@pytest.mark.parametrize("filename", ["fixtures.jsonl", "seed_events.jsonl", "live_events.jsonl"])
def test_valid_corpora(filename):
    events = [json.loads(line) for line in (SAMPLE / filename).read_text().splitlines()]
    for event in events:
        validate_event(event)
    assert len({event["event_id"] for event in events}) == len(events)


@pytest.mark.parametrize(
    "change",
    [
        {"body": " "},
        {"event_time": "2026-02-30T00:00:00Z"},
        {"ingested_at": "2026-10-26T00:00:00+00:00"},
        {"version": True},
        {"version": 0},
        {"version": 1.5},
        {"schema_version": True},
        {"schema_version": 2},
        {"operation": "delete"},
        {"category": "unknown"},
        {"category": []},
        {"source_url": ""},
        {"extra": "field"},
        {"document_id": ""},
    ],
)
def test_reject_contract_violations(change):
    with pytest.raises(ValueError):
        validate_event(dict(seed_events(n=1)[0], **change))


def test_optional_nullable_source_url_and_missing_required_field():
    event = seed_events(n=1)[0]
    del event["source_url"]
    validate_event(event)
    del event["document_id"]
    with pytest.raises(ValueError):
        validate_event(event)


def test_draft_2020_12_with_format_checker():
    # Shared CI installs only pytest/ruff. Optional conformance runs with TV2 test requirements.
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads((ROOT / "data/contracts/event.schema.json").read_text())
    jsonschema.Draft202012Validator.check_schema(schema)
    validator = jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker())
    for filename in ("fixtures.jsonl", "seed_events.jsonl", "live_events.jsonl"):
        for line in (SAMPLE / filename).read_text().splitlines():
            validator.validate(json.loads(line))
    for line in (SAMPLE / "edge_cases.jsonl").read_text().splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        try:
            validate_event(event)
        except ValueError:
            assert not validator.is_valid(event)
        else:
            validator.validate(event)
