import httpx
import pytest

from services.api.retrieval import OpenSearchRetriever


class Embedder:
    def encode(self, texts):
        return [[1.0] for _ in texts]


@pytest.mark.parametrize(
    "question,expected",
    [
        ("Atlas notice 001?", {"match_phrase": {"title": "notice 001"}}),
        ("Atlas notice 001 document_id=inc-atlas-001", {"term": {"document_id": "inc-atlas-001"}}),
    ],
)
def test_explicit_identifier_filters_vector_candidates(question, expected):
    def handler(request):
        import json

        payload = json.loads(request.content)
        assert payload["query"]["knn"]["embedding"]["filter"] == expected
        return httpx.Response(200, json={"hits": {"hits": []}})

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        retriever = OpenSearchRetriever(client, "http://search", "test", Embedder(), 1)
        assert retriever.retrieve(question, 3) == []
