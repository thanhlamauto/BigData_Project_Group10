"""Bounded HTTP adapters; mock output is always labelled in the response."""

import json
from typing import Protocol

import httpx

from .prompt import INSUFFICIENT_CONTEXT, SYSTEM_PROMPT


class LLMError(Exception):
    def __init__(self, message: str, status_code: int = 502):
        super().__init__(message)
        self.status_code = status_code


class LLM(Protocol):
    def generate(self, prompt: str) -> str: ...


class MockLLM:
    def generate(self, prompt: str) -> str:
        context = json.loads(prompt)["context"]
        if not context:
            return INSUFFICIENT_CONTEXT
        return "[MOCK] " + "\n".join(
            f"{doc['body']} [{doc['document_id']} v{doc['version']}]" for doc in context
        )


class HTTPLLM:
    def __init__(
        self, client: httpx.Client, provider: str, base_url: str, model: str, api_key: str = ""
    ):
        if provider not in {"ollama", "openai"}:
            raise ValueError("Unsupported HTTP LLM provider")
        if not model:
            raise ValueError("LLM_MODEL is required for a real LLM provider")
        self.client = client
        self.provider = provider
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.api_key = api_key

    def generate(self, prompt: str) -> str:
        if self.provider == "ollama":
            endpoint = "/api/generate"
            payload = {
                "model": self.model,
                "system": SYSTEM_PROMPT,
                "prompt": prompt,
                "stream": False,
            }
        else:
            endpoint = "/chat/completions"
            payload = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                "stream": False,
            }
        headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}
        try:
            response = self.client.post(self.base_url + endpoint, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
            answer = (
                data["response"]
                if self.provider == "ollama"
                else data["choices"][0]["message"]["content"]
            )
            if not isinstance(answer, str) or not answer.strip():
                raise ValueError("LLM returned no text")
            return answer.strip()
        except httpx.TimeoutException as exc:
            raise LLMError("LLM request timed out", status_code=503) from exc
        except httpx.HTTPStatusError as exc:
            status = 503 if exc.response.status_code in {429, 503, 504} else 502
            raise LLMError("LLM upstream request failed", status_code=status) from exc
        except httpx.RequestError as exc:
            raise LLMError("LLM service unavailable", status_code=503) from exc
        except (ValueError, KeyError, TypeError, IndexError) as exc:
            raise LLMError("LLM returned an invalid response") from exc
