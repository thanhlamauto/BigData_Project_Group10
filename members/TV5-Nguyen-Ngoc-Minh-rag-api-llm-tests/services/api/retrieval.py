"""Offline fixture retrieval and an OpenSearch 2.19 k-NN adapter."""

import importlib
import json
import math
import re
from pathlib import Path
from typing import Protocol
from urllib.parse import quote

import httpx
from pydantic import ValidationError

from .models import Document


class RetrievalError(Exception):
    """The retrieval backend failed; never includes upstream response text."""


class Embedder(Protocol):
    def encode(self, texts: list[str]) -> list[list[float]]: ...


class Retriever(Protocol):
    def retrieve(self, question: str, top_k: int) -> list[Document]: ...


def load_embedder(factory_path: str) -> Embedder:
    """Load TV4's implementation once, without duplicating model/normalization code."""
    if not factory_path or ":" not in factory_path:
        raise ValueError("OpenSearch requires EMBEDDER_FACTORY=module:factory from TV4")
    module_name, factory_name = factory_path.split(":", 1)
    embedder = getattr(importlib.import_module(module_name), factory_name)()
    if not callable(getattr(embedder, "encode", None)):
        raise ValueError("EMBEDDER_FACTORY must create an object with encode(texts)")
    return embedder


def newest_unique(documents: list[Document]) -> list[Document]:
    latest: dict[str, Document] = {}
    for document in documents:
        previous = latest.get(document.document_id)
        if previous is None or (document.version, document.score) > (
            previous.version,
            previous.score,
        ):
            latest[document.document_id] = document
    return sorted(latest.values(), key=lambda doc: (-doc.score, doc.document_id))


STOP_WORDS = set("what is the a an of for to at in on latest current when does how".split())


def tokens(text: str) -> set[str]:
    return set(re.findall(r"\w+", text.casefold())) - STOP_WORDS


class FakeRetriever:
    """Deterministic keyword retrieval for tests, never a substitute for vector search."""

    def __init__(self, documents: list[Document]):
        self.documents = newest_unique(documents)

    @classmethod
    def from_jsonl(cls, path: Path) -> "FakeRetriever":
        documents = [
            Document.model_validate_json(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        return cls(documents)

    def retrieve(self, question: str, top_k: int) -> list[Document]:
        query = tokens(question)
        matches = []
        for document in self.documents:
            overlap = query & tokens(f"{document.title}\n{document.body}")
            if overlap:
                matches.append(document.model_copy(update={"score": len(overlap) / len(query)}))
        return newest_unique(matches)[:top_k]


class OpenSearchRetriever:
    def __init__(
        self,
        client: httpx.Client,
        url: str,
        index: str,
        embedder: Embedder,
        dimension: int,
        vector_field: str = "embedding",
        auth: tuple[str, str] | None = None,
    ):
        self.client = client
        self.search_url = f"{url.rstrip('/')}/{quote(index, safe='')}/_search"
        self.embedder = embedder
        self.dimension = dimension
        self.vector_field = vector_field
        self.auth = auth

    def retrieve(self, question: str, top_k: int) -> list[Document]:
        try:
            vectors = self.embedder.encode([question])
            if len(vectors) != 1 or len(vectors[0]) != self.dimension:
                raise ValueError("Query embedding dimension differs from index contract")
            vector = [float(value) for value in vectors[0]]
            if not all(math.isfinite(value) for value in vector) or not any(vector):
                raise ValueError("Query vector must be finite and nonzero")
            response = self.client.post(
                self.search_url,
                auth=self.auth,
                json={
                    "size": top_k,
                    "_source": list(Document.model_fields.keys() - {"score"}),
                    "query": {"knn": {self.vector_field: {"vector": vector, "k": top_k}}},
                },
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("timed_out") or payload.get("_shards", {}).get("failed", 0):
                raise ValueError("Partial OpenSearch search results")
            documents = [
                Document.model_validate({**hit["_source"], "score": hit["_score"]})
                for hit in payload["hits"]["hits"]
            ]
            return newest_unique(documents)[:top_k]
        except (
            httpx.HTTPError,
            json.JSONDecodeError,
            ValidationError,
            ValueError,
            KeyError,
            TypeError,
            IndexError,
        ) as exc:
            raise RetrievalError("Retrieval backend unavailable or invalid response") from exc
