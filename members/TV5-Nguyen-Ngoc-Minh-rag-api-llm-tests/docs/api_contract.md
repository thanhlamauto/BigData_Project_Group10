# StreamRAG API v1 contract (TV5)

## GET /health

HTTP 200:

```json
{"status":"ok","retriever_backend":"fake","llm_provider":"mock"}
```

Process liveness and active configuration only. It does not probe OpenSearch/LLM readiness.
An application configured with a missing fixture file or an unavailable embedder fails startup.

## POST /ask

Content-Type: `application/json`

```json
{"question":"What is the Atlas Search maintenance time?","top_k":3}
```

- `question`: string, trim leading/trailing whitespace, 1–4000 characters after trimming.
- `top_k`: integer 1–20, default 3; booleans and numeric strings rejected.
- Unknown fields rejected with HTTP 422.

HTTP 200, example shape (latencies and score below are **illustrative**, not a benchmark):

```json
{
  "answer": "[MOCK] Atlas Search maintenance starts at 20:30 UTC. [inc-atlas-001 v1]",
  "sources": [{
    "document_id": "inc-atlas-001",
    "version": 1,
    "title": "Atlas Search maintenance",
    "event_time": "2026-10-26T09:00:00Z",
    "score": 0.8,
    "source_url": null,
    "ingested_at": "2026-10-26T09:00:03Z",
    "indexed_at": null
  }],
  "retrieval_ms": 0.5,
  "llm_provider": "mock",
  "llm_ms": 0.1
}
```

`sources` never includes document bodies or vectors. Timestamps must include a timezone.
`source_url`, `ingested_at` and `indexed_at` may be null. Missing URLs are never invented.
Scores are backend-specific relevance values, not probabilities; fake scores are keyword overlap.
Each `document_id` appears at most once, preferring the newest version among retrieved hits.
TV4 must still enforce `_id=document_id` and version-aware upserts: API deduplication cannot
guarantee the global newest version if the index contains multiple stale physical documents.

An empty result skips generation entirely and returns HTTP 200:

```json
{
  "answer":"Không đủ thông tin để trả lời câu hỏi này.",
  "sources":[],
  "retrieval_ms":0.5,
  "llm_provider":"mock",
  "llm_ms":0
}
```

The grounded prompt asks real LLMs to cite `[document_id vN]` and use only provided context.
This is a prompting constraint, not a guarantee of model factuality; validate `expected_facts`
and citations in live evidence. Mock deterministically echoes context with a `[MOCK]` prefix.

## Errors

| HTTP | Meaning |
|---|---|
| 422 | Request validation error; FastAPI's standard `detail` array |
| 503 | Retrieval timeout, upstream failure, malformed/partial search results or invalid query vector |
| 503 | LLM timeout, connection failure, upstream 429/503/504 |
| 502 | Other LLM HTTP failure or invalid/empty generation payload |

Operational errors use `{"detail":"..."}`. Provider response bodies, auth credentials and URLs
are not included in these messages. A backend failure is never reported as an empty successful
retrieval or silently replaced by mock generation.

## Retrieval / LLM handoff

- `Retriever.retrieve(question: str, top_k: int) -> list[Document]`.
- Shared `Embedder.encode(texts: list[str]) -> list[list[float]]`, loaded via
  `EMBEDDER_FACTORY=module:factory` once per worker; TV4 owns model/normalization.
- OpenSearch request: `size=top_k`, `query.knn.embedding={vector, k:top_k}`;
  metadata allowlist in `_source`, default index `streamrag-docs-v1`.
- `LLM.generate(prompt: str) -> str`; provider selected through `LLM_PROVIDER`.
- Ollama: `POST {LLM_BASE_URL}/api/generate` with `stream=false`.
- OpenAI-compatible: `POST {LLM_BASE_URL}/chat/completions`; base includes `/v1`,
  messages contain system instructions and JSON question/context.
- `LLM_API_KEY` is an optional bearer credential, read from environment only.
- Environment fields/defaults and startup/run commands are in
  [the service README](../services/api/README.md).

This draft v1 is implemented for review; team-wide freeze and real-service validation remain
the milestone's integration gates.
