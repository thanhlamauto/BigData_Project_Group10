import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from services.producer.cli import run
from services.producer.events import SAMPLE, seed_events
from services.producer.kafka_producer import EventProducer


class FakeClient:
    def __init__(self, *, full=0, error=None, pending=0):
        self.full = full
        self.error = error
        self.pending = pending
        self.messages = []
        self.callbacks = []

    def produce(self, topic, **kwargs):
        if self.full:
            self.full -= 1
            raise BufferError("queue full")
        self.messages.append((topic, kwargs))
        self.callbacks.append(kwargs["on_delivery"])

    def poll(self, timeout):
        pass

    def flush(self, timeout):
        if not self.pending:
            message = SimpleNamespace(topic=lambda: "raw", partition=lambda: 0, offset=lambda: 0)
            for callback in self.callbacks:
                callback(self.error, message)
            self.callbacks.clear()
        return self.pending


def test_publish_key_utf8_timestamp_and_acknowledgements():
    client = FakeClient(full=1)
    producer = EventProducer(client=client, clock=lambda: datetime(2026, 10, 27, tzinfo=UTC))
    original = dict(seed_events(n=1)[0], title="Nguyễn Thanh Lâm")
    before = dict(original)
    outgoing = producer.publish(original)
    assert producer.delivered == 0  # Queued is not delivered.
    assert producer.flush() == 1
    topic, message = client.messages[0]
    assert topic == "streamrag.raw.v1"
    assert message["key"] == original["document_id"].encode()
    assert json.loads(message["value"]) == outgoing
    assert "Nguyễn Thanh Lâm".encode() in message["value"]
    assert outgoing["ingested_at"] == "2026-10-27T00:00:00Z"
    assert original == before


@pytest.mark.parametrize(
    "options,error", [({"pending": 1}, TimeoutError), ({"error": "broker failed"}, RuntimeError)]
)
def test_delivery_failures_never_report_success(options, error):
    producer = EventProducer(client=FakeClient(**options))
    producer.publish(seed_events(n=1)[0])
    with pytest.raises(error):
        producer.flush()


def test_permanent_queue_full_is_bounded():
    ticks = iter([0, 0.5, 2])
    producer = EventProducer(client=FakeClient(full=100), timeout=1, monotonic=lambda: next(ticks))
    with pytest.raises(TimeoutError):
        producer.publish(seed_events(n=1)[0])


def test_normal_mode_prevalidates_entire_file(tmp_path):
    path = tmp_path / "bad.jsonl"
    path.write_text(json.dumps(seed_events(n=1)[0]) + "\n{broken\n")

    def forbidden(*args):
        pytest.fail("No producer should be constructed for invalid input")

    assert run(["--event-file", str(path)], producer_factory=forbidden) == 1


def test_edge_mode_sends_exact_invalid_bytes_and_waits_for_acks():
    client = FakeClient()
    producer = EventProducer(client=client)
    waits = []
    assert (
        run(
            ["--edge-cases", "--rate", "100"],
            simulator=True,
            producer_factory=lambda *args: producer,
            sleep=waits.append,
        )
        == 0
    )
    assert producer.delivered == 26
    assert len(waits) == 25
    expected = (SAMPLE / "edge_cases.jsonl").read_text().splitlines()
    assert [message["value"] for _, message in client.messages] == [e.encode() for e in expected]
    assert client.messages[21][1]["key"] == b"invalid-line-22"


def test_update_and_file_replay(capsys):
    assert run(["--update", "inc-atlas-001", "--dry-run"], simulator=True) == 0
    update = json.loads(capsys.readouterr().out)
    assert update["document_id"] == "inc-atlas-001" and update["version"] == 2
    assert "21:30" in update["body"]
    assert run(["--dry-run", "--count", "2"]) == 0
    assert len(capsys.readouterr().out.splitlines()) == 2


@pytest.mark.parametrize(
    "args", [["--rate", "0"], ["--rate", "nan"], ["--rate", "inf"], ["--count", "0"]]
)
def test_cli_rejects_invalid_arguments(args):
    with pytest.raises(SystemExit):
        run(args, simulator=True)


def test_simulation_runs_use_distinct_document_ids(capsys):
    run(["--count", "1", "--dry-run"], simulator=True)
    first = json.loads(capsys.readouterr().out)
    run(["--count", "1", "--dry-run"], simulator=True)
    second = json.loads(capsys.readouterr().out)
    assert first["event_id"] != second["event_id"]
    assert first["document_id"] != second["document_id"]
