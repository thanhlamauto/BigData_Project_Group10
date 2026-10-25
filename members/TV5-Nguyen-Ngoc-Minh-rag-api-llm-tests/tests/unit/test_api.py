import pytest
from fastapi.testclient import TestClient
from services.api.config import Settings
from services.api.llm import LLMError
from services.api.main import create_app
from services.api.prompt import INSUFFICIENT_CONTEXT
from services.api.retrieval import FakeRetriever, RetrievalError


def test_offline_api_contract_and_unknown_question():
    with TestClient(create_app(Settings())) as client:
        assert client.get("/health").json() == {
            "status": "ok",
            "retriever_backend": "fake",
            "llm_provider": "mock",
        }
        result = client.post("/ask", json={"question": "Atlas Search maintenance", "top_k": 1})
        assert result.status_code == 200
        data = result.json()
        assert "20:30" in data["answer"]
        assert data["llm_provider"] == "mock"
        assert data["retrieval_ms"] >= 0 and data["llm_ms"] >= 0
        assert data["sources"][0]["document_id"] == "inc-atlas-001"
        assert data["sources"][0]["version"] == 1
        assert "body" not in data["sources"][0]
        assert "embedding" not in data["sources"][0]
        unknown = client.post("/ask", json={"question": "zyxwvutsrq"}).json()
        assert unknown["sources"] == []
        assert unknown["answer"] == INSUFFICIENT_CONTEXT


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"question": "   "},
        {"question": "x" * 4001},
        {"question": "Atlas", "top_k": 0},
        {"question": "Atlas", "top_k": 21},
        {"question": "Atlas", "top_k": True},
        {"question": "Atlas", "top_k": "3"},
        {"question": "Atlas", "unexpected": "field"},
    ],
)
def test_invalid_request_returns_422(payload):
    with TestClient(create_app(Settings())) as client:
        assert client.post("/ask", json=payload).status_code == 422


def test_empty_context_never_calls_llm():
    class MustNotBeCalled:
        def generate(self, prompt):
            pytest.fail("LLM must not run without context")

    with TestClient(create_app(Settings(), FakeRetriever([]), MustNotBeCalled())) as client:
        result = client.post("/ask", json={"question": "Atlas"}).json()
        assert result["sources"] == []
        assert result["llm_ms"] == 0


def test_only_latest_version_is_cited(documents):
    v2 = documents[0].model_copy(update={"version": 2, "body": "Maintenance is now 21:30 UTC."})
    retriever = FakeRetriever([documents[0], v2, v2])
    with TestClient(create_app(Settings(), retriever)) as client:
        result = client.post("/ask", json={"question": "Atlas maintenance"}).json()
        assert "21:30" in result["answer"]
        assert "20:30" not in result["answer"]
        assert [source["version"] for source in result["sources"]] == [2]


@pytest.mark.parametrize("status", [502, 503])
def test_llm_failure_is_bounded_and_explicit(status):
    class BrokenLLM:
        def generate(self, prompt):
            raise LLMError("LLM request failed", status)

    with TestClient(create_app(Settings(), llm=BrokenLLM())) as client:
        response = client.post("/ask", json={"question": "Atlas"})
        assert response.status_code == status
        assert response.json() == {"detail": "LLM request failed"}


def test_retrieval_failure_returns_503():
    class BrokenRetriever:
        def retrieve(self, question, top_k):
            raise RetrievalError("private upstream details")

    with TestClient(create_app(Settings(), BrokenRetriever())) as client:
        response = client.post("/ask", json={"question": "Atlas"})
        assert response.status_code == 503
        assert "private" not in response.text


def test_environment_provider_validation(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "unsupported")
    with pytest.raises(ValueError):
        Settings.from_env()


def test_opensearch_does_not_silently_fall_back_to_fake():
    with pytest.raises(ValueError, match="EMBEDDER_FACTORY"):
        with TestClient(create_app(Settings(retriever_backend="opensearch"))):
            pass
