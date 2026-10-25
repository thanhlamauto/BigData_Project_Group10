import httpx
import pytest

from services.indexer.upsert import initialize, upsert
from services.producer.events import seed_events


def test_atomic_version_upsert_ignores_duplicate_and_stale():
    state = {}

    def handler(request):
        version = int(request.url.params["version"])
        assert request.url.params["version_type"] == "external"
        key = request.url.path
        if state.get(key, 0) >= version:
            return httpx.Response(409)
        state[key] = version
        return httpx.Response(201)

    with httpx.Client(base_url="http://search", transport=httpx.MockTransport(handler)) as client:
        event = seed_events(n=1)[0]
        assert upsert(client, "test", event, [1.0]) == "indexed"
        assert upsert(client, "test", event, [1.0]) == "skipped_stale_or_duplicate"
        assert upsert(client, "test", {**event, "version": 2}, [1.0]) == "indexed"
        assert upsert(client, "test", event, [1.0]) == "skipped_stale_or_duplicate"
    assert list(state.values()) == [2]


def test_write_failure_must_not_be_acknowledged():
    with httpx.Client(
        base_url="http://search", transport=httpx.MockTransport(lambda request: httpx.Response(503))
    ) as client:
        with pytest.raises(httpx.HTTPStatusError):
            upsert(client, "test", seed_events(n=1)[0], [1.0])


def test_existing_index_dimension_mismatch_fails():
    def handler(request):
        if request.method == "PUT":
            return httpx.Response(
                400, json={"error": {"type": "resource_already_exists_exception"}}
            )
        return httpx.Response(
            200, json={"test": {"mappings": {"properties": {"embedding": {"dimension": 128}}}}}
        )

    with httpx.Client(base_url="http://search", transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(ValueError, match="dimension"):
            initialize(client, "test", 384)
