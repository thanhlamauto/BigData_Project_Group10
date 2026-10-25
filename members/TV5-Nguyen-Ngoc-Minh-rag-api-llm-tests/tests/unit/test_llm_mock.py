import json

import httpx
import pytest
from services.api.llm import HTTPLLM, LLMError, MockLLM
from services.api.prompt import SYSTEM_PROMPT, build_prompt


def test_mock_extracts_arbitrary_context_without_demo_answers(documents):
    document = documents[0].model_copy(update={"body": "A new arbitrary fact: 07:42 XYZ."})
    prompt = build_prompt("Tell me the fact", [document])
    assert MockLLM().generate(prompt) == MockLLM().generate(prompt)
    assert "07:42 XYZ" in MockLLM().generate(prompt)
    assert "20:30" not in MockLLM().generate(prompt)


@pytest.mark.parametrize("provider", ["ollama", "openai"])
def test_http_protocols(provider, documents):
    def handler(request):
        payload = json.loads(request.content)
        assert payload["model"] == "configured-model"
        assert payload["stream"] is False
        assert request.headers["Authorization"] == "Bearer unit-test-placeholder"
        if provider == "ollama":
            assert request.url.path == "/api/generate"
            assert payload["system"] == SYSTEM_PROMPT
            return httpx.Response(200, json={"response": " grounded response "})
        assert request.url.path == "/v1/chat/completions"
        assert payload["messages"][0]["role"] == "system"
        return httpx.Response(
            200, json={"choices": [{"message": {"content": "grounded response"}}]}
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        base = "http://llm" if provider == "ollama" else "http://llm/v1"
        llm = HTTPLLM(client, provider, base, "configured-model", "unit-test-placeholder")
        assert llm.generate(build_prompt("Atlas", documents)) == "grounded response"


@pytest.mark.parametrize(
    "response,status",
    [
        (httpx.Response(500, text="secret upstream content"), 502),
        (httpx.Response(429), 503),
        (httpx.Response(503), 503),
        (httpx.Response(200, json={"choices": []}), 502),
        (httpx.Response(200, json={"choices": [{"message": {"content": None}}]}), 502),
        (httpx.Response(200, text="broken json"), 502),
    ],
)
def test_upstream_errors_are_sanitized(response, status):
    with httpx.Client(transport=httpx.MockTransport(lambda request: response)) as client:
        with pytest.raises(LLMError) as caught:
            HTTPLLM(client, "openai", "http://llm/v1", "configured-model").generate("prompt")
        assert caught.value.status_code == status
        assert "secret" not in str(caught.value)


def test_timeout_becomes_503():
    def timeout(request):
        raise httpx.ReadTimeout("sensitive upstream details", request=request)

    with httpx.Client(transport=httpx.MockTransport(timeout)) as client:
        with pytest.raises(LLMError) as caught:
            HTTPLLM(client, "ollama", "http://llm", "configured-model").generate("prompt")
        assert caught.value.status_code == 503


def test_real_provider_requires_model():
    with httpx.Client() as client, pytest.raises(ValueError, match="LLM_MODEL"):
        HTTPLLM(client, "ollama", "http://llm", "")
