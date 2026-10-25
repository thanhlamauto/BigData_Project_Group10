# TV5 validation and remaining integration gates

Validation date: **25/10/2026**, Asia/Saigon. Owner: Nguyen Ngoc Minh (TV5).
Related issues: M1-14 (#16), M1-15 (#17), M1-16 (#18). This is implementation evidence for
review, not a claim that all milestone acceptance criteria or M1-17 are complete.

## Executed locally

Environment: Windows, CPython 3.11.15, isolated `.venv`, dependencies pinned in
`requirements/runtime.txt`; Ruff 0.6.9 and pytest 8.3.3 match the existing repo tools.

From the repository root:

```powershell
.venv/Scripts/python.exe -m ruff check --config members/TV1-Pham-Chi-Bang-lead-architecture-devops/pyproject.toml .
.venv/Scripts/python.exe -m ruff format --config members/TV1-Pham-Chi-Bang-lead-architecture-devops/pyproject.toml --check .
.venv/Scripts/python.exe -m pytest -c members/TV1-Pham-Chi-Bang-lead-architecture-devops/pyproject.toml --rootdir . members/TV1-Pham-Chi-Bang-lead-architecture-devops/tests members/TV5-Nguyen-Ngoc-Minh-rag-api-llm-tests/tests -q
uv pip check --python .venv/Scripts/python.exe
```

Results: Ruff checks passed, formatting passed, **53 tests passed**, installed dependencies
compatible. Four deprecation warnings come from Starlette's current HTTPX TestClient support
and its timeout argument; they do not affect the app's HTTPX runtime adapters.

Coverage includes request validation, empty context skipping generation, latest-version source
selection, no vector/body exposure, shared embedder loading, vector validation, OpenSearch
request/response handling, partial-search rejection, both LLM HTTP protocols, sanitized upstream
errors/timeouts, mock output derived from arbitrary context, smoke exit codes, freshness
arithmetic, publisher stdin and the simulated stream/revision/fault workflow.

Also launched **a real Uvicorn process on localhost** and checked it via HTTP:

- `GET /health`: 200, `retriever_backend=fake`, `llm_provider=mock`.
- `POST /ask`: 200, source `inc-atlas-001` v1, answer containing fixture `20:30 UTC`.
- Whitespace-only question: 422. `/openapi.json`: both API paths present.
- `scripts/smoke_test.py --seed-only`: exit 0, two tiny API fixture cases pass,
  `hit_at_k=1.0`, `fact_pass_rate=1.0` **on those two fixtures only**.
- Adding `--require-real-llm`: exit 1 because the provider is mock, as intended.

The fixture scores and tiny-case metrics do not establish semantic retrieval quality, live
freshness or a production benchmark. No live freshness/p50/p95 values have been measured.

## Still required before marking milestone tasks done

| Gate | Needed handoff / evidence |
|---|---|
| M1-14 real retrieval / Gate B | TV4 mapping, index fixture and shared `Embedder`; run `/ask` against OpenSearch 2.19.4 with the matching model/normalization |
| M1-15 genuine generation / Case 5 | Configure an existing Ollama model or compatible API credentials in local environment; save provider/model and actual `/ask` response |
| M1-16 live smoke / Gate C | TV2 raw-JSONL stdin publisher wrapper + TV3 streaming + TV4 indexing; run Cases 1–4 on a clean, seeded stack and save actual freshness output |
| Container / Compose | Build the TV5 Dockerfile; TV1 adds API and shared encoder/dependencies to full-stack Compose |
| Fault quality | Inspect Spark rejection counters and OpenSearch/indexer version/duplicate state; API source checks alone do not prove storage behavior |
| M1-17 / Gate D | TV4 cross-review, TV1 review of shared CI/Makefile hooks, another person runs from a clean clone, rehearsal/evidence |

Docker, `make`, TV2/TV3/TV4 service implementations and a real LLM configuration were not
available on this machine. The user confirmed that a real LLM is not yet configured and asked
to finish the code while recording that validation gap. No credentials, model downloads,
runtime data or `.env` files are committed.

The CI change installs the API's pinned runtime before the existing repository-wide tests;
the TV1 Makefile change replaces only TV5's owned `smoke` placeholder. These shared-file hooks
need TV1 review; changes are proposed on a feature branch, not merged into `main`.
