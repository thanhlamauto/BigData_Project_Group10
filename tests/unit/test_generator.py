import json
import subprocess
import sys
from collections import Counter

from services.producer.events import (
    ROOT,
    SAMPLE,
    edge_cases,
    live_events,
    question_cases,
    seed_events,
    validate_event,
)


def test_generator_recreates_every_committed_fixture(tmp_path):
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/generate_data.py"),
            "--all",
            "--seed",
            "42",
            "--n",
            "400",
            "--out",
            str(tmp_path / "seed_events.jsonl"),
        ],
        check=True,
        capture_output=True,
    )
    for file in tmp_path.iterdir():
        assert file.read_bytes() == (SAMPLE / file.name).read_bytes(), file.name
    assert len(seed_events()) == 400
    assert seed_events(42) != seed_events(43)
    assert len({event["title"].split(" notice")[0] for event in seed_events()}) >= 8


def test_qa_ground_truth_before_and_after_stream():
    state = {event["document_id"]: event for event in seed_events()}
    cases = question_cases()
    assert Counter(case["phase"] for case in cases) == {"seed": 10, "after_stream": 10}
    for phase in ("seed", "after_stream"):
        if phase == "after_stream":
            state.update({event["document_id"]: event for event in live_events()})
        for case in cases:
            if case["phase"] == phase:
                event = state[case["expected_document_id"]]
                assert event["version"] == case["expected_version"]
                assert all(fact in event["body"] for fact in case["expected_facts"])
    assert "21:30" in state["inc-atlas-001"]["body"]
    assert "20:30" not in state["inc-atlas-001"]["body"]
    assert len(state) == 405


def test_edge_cases_have_exact_counts_and_order():
    lines, expectations = edge_cases()
    assert Counter(item["kind"] for item in expectations) == {
        "duplicate": 10,
        "late": 5,
        "update": 5,
        "stale": 1,
        "invalid": 5,
    }
    assert [json.loads(line) for line in lines[:10]] == seed_events(n=10)
    invalid = []
    for index, line in enumerate(lines):
        try:
            validate_event(json.loads(line))
        except ValueError:
            invalid.append(index + 1)
    assert invalid == [22, 23, 24, 25, 26]
    newest, stale = json.loads(lines[15]), json.loads(lines[20])
    assert newest["document_id"] == stale["document_id"] == "inc-atlas-001"
    assert newest["version"] == 2 and stale["version"] == 1
