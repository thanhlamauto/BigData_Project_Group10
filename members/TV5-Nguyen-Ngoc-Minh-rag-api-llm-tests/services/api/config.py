"""Environment configuration; secrets are never returned by the API."""

import os
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, SecretStr

MODULE_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseModel):
    retriever_backend: Literal["fake", "opensearch"] = "fake"
    fixtures_path: Path = MODULE_ROOT / "fixtures" / "documents.jsonl"
    opensearch_url: str = "http://localhost:9200"
    opensearch_index: str = "streamrag-docs-v1"
    opensearch_vector_field: str = "embedding"
    opensearch_username: str = ""
    opensearch_password: SecretStr = SecretStr("")
    embedder_factory: str = ""
    embedding_dim: int = Field(default=384, ge=1)
    retrieval_timeout_seconds: float = Field(default=10, gt=0, le=120)
    llm_provider: Literal["mock", "ollama", "openai"] = "mock"
    llm_base_url: str = "http://localhost:11434"
    llm_model: str = ""
    llm_api_key: SecretStr = SecretStr("")
    llm_timeout_seconds: float = Field(default=60, gt=0, le=300)

    @classmethod
    def from_env(cls) -> "Settings":
        values = {
            name: os.environ[name.upper()]
            for name in cls.model_fields
            if name.upper() in os.environ
        }
        return cls.model_validate(values)
