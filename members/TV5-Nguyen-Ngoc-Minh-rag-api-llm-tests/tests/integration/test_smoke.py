import argparse
import json
import sys
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import httpx
import pytest
from fastapi.testclient import TestClient

from scripts import smoke_test as smoke
from services.api.config import MODULE_ROOT, Settings
from services.api.main import create_app
from services.api.retrieval import FakeRetriever


def arguments(**overrides):
    return argparse.Namespace(
        **{
            "base_url": "http://testserver",
            "test_cases": MODULE_ROOT / "fixtures/test_cases.jsonl",
            "events_file": None,
            "seed_only": True,
            "publish_command": None,
            "timeout": 0.1,
            "poll_interval": 0.001,
            "require_real_llm": False,
            **overrides,
        }
    )


def route_to_api(monkeypatch, api_client):
    monkeypatch.setattr(
        smoke,
        "httpx",
        SimpleNamespace(
            Client=lambda **kwargs: api_client,
            HTTPStatusError=httpx.HTTPStatusError,
            RequestError=httpx.RequestError,
        ),
    )


def test_seed_smoke_through_fastapi(monkeypatch, capsys):
    api_client = TestClient(create_app(Settings()))
    route_to_api(monkeypatch, api_client)
    smoke.run_checks(arguments())
    output = capsys.readouterr().out
    assert "PASS seed-only" in output
    assert '"hit_at_k": 1.0' in output
    assert "PASS full smoke" not in output


def test_full_smoke_exercises_stream_revision_fault(monkeypatch, documents, capsys):
    retriever = FakeRetriever(documents)
    api_client = TestClient(create_app(Settings(retriever_backend="opensearch"), retriever))
    route_to_api(monkeypatch, api_client)
    published_batches = []

    def fake_publisher(command, events, stamp=True):
        from services.api.models import Document

        records = [dict(event) for event in events]
        for event in records:
            if stamp:
                event["ingested_at"] = datetime.now(UTC).isoformat()
            if "body" in event:
                retriever.documents = FakeRetriever(
                    [*retriever.documents, Document.model_validate(event)]
                ).documents
        published_batches.append(records)
        return records

    monkeypatch.setattr(smoke, "publish", fake_publisher)
    smoke.run_checks(arguments(seed_only=False, publish_command='["test-publisher"]'))
    output = capsys.readouterr().out
    assert "PASS stream" in output and "PASS revision" in output and "PASS fault" in output
    assert '"freshness_seconds":' in output
    assert "SKIP Case 5" in output
    assert len(published_batches) == 3
    assert published_batches[-1][0] == {"event_id": "malformed-smoke"}


def test_full_smoke_rejects_fixture_backend(monkeypatch):
    api_client = TestClient(create_app(Settings()))
    route_to_api(monkeypatch, api_client)
    with pytest.raises(smoke.SmokeFailure, match="real OpenSearch"):
        smoke.run_checks(arguments(seed_only=False, publish_command='["unused"]'))


def test_real_generation_requirement_rejects_mock(monkeypatch):
    api_client = TestClient(create_app(Settings()))
    route_to_api(monkeypatch, api_client)
    with pytest.raises(smoke.SmokeFailure, match="uses mock"):
        smoke.run_checks(arguments(require_real_llm=True))


def test_live_poll_has_timeout():
    with TestClient(create_app(Settings())) as client:
        case = {"question": "Atlas", "expected_document_id": "missing", "expected_facts": []}
        with pytest.raises(smoke.SmokeFailure, match="Timed out"):
            smoke.wait_case(client, case, 0.03, 0.001)


def test_freshness_uses_ingestion_time():
    ingested = datetime(2026, 10, 26, tzinfo=UTC)
    event = {
        "document_id": "d",
        "version": 2,
        "event_time": ingested.isoformat(),
        "ingested_at": ingested.isoformat(),
    }
    evidence = smoke.record_freshness(event, ingested + timedelta(seconds=2.5))
    assert evidence["freshness_seconds"] == 2.5


def test_publisher_uses_stdin_without_shell(tmp_path):
    output = tmp_path / "published.jsonl"
    code = "import pathlib,sys; pathlib.Path(sys.argv[1]).write_text(sys.stdin.read())"
    records = smoke.publish([sys.executable, "-c", code, str(output)], [{"document_id": "d"}])
    assert json.loads(output.read_text()) == records[0]
    assert records[0]["ingested_at"]


def test_cli_failures_exit_nonzero(capsys):
    assert smoke.main([]) == 1
    assert "FAIL Full smoke requires" in capsys.readouterr().err
