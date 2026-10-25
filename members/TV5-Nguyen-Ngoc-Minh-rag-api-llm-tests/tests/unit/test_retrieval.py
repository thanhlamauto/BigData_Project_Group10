import json
import sys
from types import SimpleNamespace

import httpx
import pytest
from services.api.retrieval import (
    FakeRetriever,
    OpenSearchRetriever,
    RetrievalError,
    load_embedder,
)


class SmallEmbedder:
    def encode(self, texts):
        assert len(texts) == 1
        return [[0.6, 0.8]]


def test_fixture_uses_newest_before_scoring(documents):
    old = documents[0].model_copy(update={"body": "supersededmarker 20:30"})
    latest = documents[0].model_copy(update={"version": 2, "body": "Maintenance changed to 21:30."})
    retriever = FakeRetriever([latest, old, old])
    assert retriever.retrieve("supersededmarker", 3) == []
    assert retriever.retrieve("21:30", 3)[0].version == 2


def test_opensearch_query_and_metadata_contract(documents):
    def handler(request):
        assert request.url.path == "/streamrag-docs-v1/_search"
        payload = json.loads(request.content)
        assert payload["size"] == 2
        assert payload["query"] == {"knn": {"embedding": {"vector": [0.6, 0.8], "k": 2}}}
        assert "embedding" not in payload["_source"]
        return httpx.Response(
            200,
            json={
                "hits": {"hits": [{"_source": documents[0].model_dump(mode="json"), "_score": 0.9}]}
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        retriever = OpenSearchRetriever(
            client, "http://search:9200", "streamrag-docs-v1", SmallEmbedder(), 2
        )
        result = retriever.retrieve("Atlas", 2)
        assert result[0].document_id == "inc-atlas-001"
        assert result[0].score == 0.9


@pytest.mark.parametrize("vector", [[1], [float("nan"), 1], [0, 0], [float("inf"), 1]])
def test_bad_embedding_never_calls_search(vector):
    class BadEmbedder:
        def encode(self, texts):
            return [vector]

    def must_not_call(request):
        pytest.fail("Invalid vectors must be rejected locally")

    with httpx.Client(transport=httpx.MockTransport(must_not_call)) as client:
        retriever = OpenSearchRetriever(client, "http://search", "docs", BadEmbedder(), 2)
        with pytest.raises(RetrievalError):
            retriever.retrieve("Atlas", 3)


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(503),
        httpx.Response(200, text="not-json"),
        httpx.Response(200, json={"hits": {"hits": []}, "timed_out": True}),
        httpx.Response(200, json={"hits": {"hits": []}, "_shards": {"failed": 1}}),
        httpx.Response(200, json={"hits": {"hits": [{"_source": {}, "_score": 1}]}}),
    ],
)
def test_search_failures_do_not_become_empty_context(response):
    with httpx.Client(transport=httpx.MockTransport(lambda request: response)) as client:
        retriever = OpenSearchRetriever(client, "http://search", "docs", SmallEmbedder(), 2)
        with pytest.raises(RetrievalError):
            retriever.retrieve("Atlas", 3)


def test_shared_embedder_factory(monkeypatch):
    monkeypatch.setitem(sys.modules, "tv4_shared", SimpleNamespace(Embedder=SmallEmbedder))
    assert load_embedder("tv4_shared:Embedder").encode(["Atlas"]) == [[0.6, 0.8]]
