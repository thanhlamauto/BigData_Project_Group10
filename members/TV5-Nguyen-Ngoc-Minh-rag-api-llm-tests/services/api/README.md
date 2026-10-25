# StreamRAG API — TV5 / Nguyen Ngoc Minh

Implements M1-14, M1-15 and the executable checks for M1-16. The API receives a question,
retrieves documents, builds grounded context and returns an answer with source metadata.
Offline mode is usable now. Integration with TV2/TV4 and a real LLM must be verified separately;
see [validation status](../../docs/validation.md).

## Quick start on Windows (PowerShell)

Run from the repository root. Python 3.11 is the CI version. If Python is already installed,
`python -m venv .venv` and `.venv/Scripts/python.exe -m pip install ...` are equivalent.

```powershell
uv venv --python 3.11 .venv
uv pip install --python .venv/Scripts/python.exe -r members/TV5-Nguyen-Ngoc-Minh-rag-api-llm-tests/requirements/dev.txt
.venv/Scripts/python.exe -m uvicorn services.api.main:app --app-dir members/TV5-Nguyen-Ngoc-Minh-rag-api-llm-tests --host 127.0.0.1 --port 8000
```

In a second terminal at the repository root:

```powershell
Invoke-RestMethod http://localhost:8000/health
$body = @{ question = 'What is the Atlas Search maintenance time?'; top_k = 3 } | ConvertTo-Json
Invoke-RestMethod http://localhost:8000/ask -Method Post -ContentType application/json -Body $body
.venv/Scripts/python.exe members/TV5-Nguyen-Ngoc-Minh-rag-api-llm-tests/scripts/smoke_test.py --seed-only
```

The example returns `[MOCK]` text containing the fixture's `20:30 UTC` and source
`inc-atlas-001` version 1. This is a synthetic fixture, not a measured live-pipeline result.
Interactive API documentation: `http://localhost:8000/docs`.

## Linux / macOS

From the repository root:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r members/TV5-Nguyen-Ngoc-Minh-rag-api-llm-tests/requirements/dev.txt
cd members/TV5-Nguyen-Ngoc-Minh-rag-api-llm-tests
python -m uvicorn services.api.main:app --host 127.0.0.1 --port 8000
```

## Configuration

See the module's [`.env.example`](../../.env.example) and [API contract](../../docs/api_contract.md).
The Python app reads **process environment variables**, not `.env` automatically. Set variables
in the shell, or pass a local `.env` with Docker's `--env-file`. Never commit credentials.

| Variable | Default / meaning |
|---|---|
| `RETRIEVER_BACKEND` | `fake` or `opensearch`; never silently falls back |
| `FIXTURES_PATH` | Module's `fixtures/documents.jsonl`; JSONL matching `Document` |
| `OPENSEARCH_URL` | `http://localhost:9200`; use `http://opensearch:9200` inside Compose |
| `OPENSEARCH_INDEX` | `streamrag-docs-v1` |
| `OPENSEARCH_VECTOR_FIELD` | `embedding` |
| `OPENSEARCH_USERNAME`, `OPENSEARCH_PASSWORD` | Optional basic auth; TLS verification stays enabled |
| `EMBEDDER_FACTORY` | Required in OpenSearch mode: `importable_module:zero_argument_factory` |
| `EMBEDDING_DIM` | `384`; must match TV4's index |
| `RETRIEVAL_TIMEOUT_SECONDS` | `10`; HTTPX connect/read/write/pool timeout |
| `LLM_PROVIDER` | `mock`, `ollama`, `openai` |
| `LLM_BASE_URL` | Ollama default `http://localhost:11434`; compatible API base must include `/v1` |
| `LLM_MODEL` | Required for a real provider; use a model available on your server/account |
| `LLM_API_KEY` | Optional bearer credential for the configured LLM endpoint |
| `LLM_TIMEOUT_SECONDS` | `60`; HTTPX connect/read/write/pool timeout |

HTTP timeouts bound network inactivity per operation; they are not an absolute generation
deadline. Generation is nonstreaming and there are no hidden upstream retries.

### TV4 handoff: share the exact embedder

TV4 owns model selection, model installation and normalization. TV5 does not implement a second
encoder or guess an embedding space. Add TV4's directory containing `embedding.py` to
`PYTHONPATH`, and configure the zero-argument factory that constructs its `Embedder`:

```powershell
$env:PYTHONPATH = 'ABSOLUTE_PATH_TO_TV4_SERVICES_INDEXER'
$env:EMBEDDER_FACTORY = 'embedding:Embedder'
$env:EMBEDDING_DIM = '384'
$env:RETRIEVER_BACKEND = 'opensearch'
$env:OPENSEARCH_URL = 'http://localhost:9200'
```

The factory must expose `encode(texts: list[str]) -> list[list[float]]` and use the **same model
and normalization** as the indexing worker. It is constructed once per API worker at startup.
Install TV4's runtime/model dependencies into the same environment. Missing factory/model fails
startup; a dimension mismatch/nonfinite/zero query vector returns a retrieval error.
The current repo has no TV4 implementation yet, so the factory is an explicit handoff point.

### Real LLM

For an existing Ollama server, after pulling a model available on that server:

```powershell
$env:LLM_PROVIDER = 'ollama'
$env:LLM_BASE_URL = 'http://localhost:11434'
$env:LLM_MODEL = 'YOUR_INSTALLED_MODEL'
```

For an OpenAI-compatible server:

```powershell
$env:LLM_PROVIDER = 'openai'
$env:LLM_BASE_URL = 'https://YOUR_PROVIDER/v1'
$env:LLM_MODEL = 'YOUR_AVAILABLE_MODEL'
# Set LLM_API_KEY locally using your secret management or local environment.
```

Restart the API after configuration changes. `GET /health` reports the configured backend/provider.
Run `scripts/smoke_test.py --seed-only --require-real-llm` to verify generation against seed
fixtures; this does not verify the Kafka/Spark pipeline. Preserve the `/ask` response and
provider/model details as real-LLM evidence.

## Tests and live smoke

From the repository root:

```powershell
.venv/Scripts/python.exe -m pytest -c members/TV1-Pham-Chi-Bang-lead-architecture-devops/pyproject.toml --rootdir . members/TV1-Pham-Chi-Bang-lead-architecture-devops/tests members/TV5-Nguyen-Ngoc-Minh-rag-api-llm-tests/tests
```

Unit and in-process integration tests use fake retrieval, deterministic generation and HTTPX
mock transports: they need no Docker, network, credentials or model downloads. The smoke-flow
unit test simulates publication; it is not live-pipeline evidence.

`make smoke` works from TV1 or TV5's directory, using `SMOKE_ARGS` for arguments. Defaults run
the **full** checks and fail unless a Kafka publisher command is supplied. For offline API
checks, use `make smoke SMOKE_ARGS="--seed-only"` with the API running.

Full smoke requires:

1. TV1 stack plus TV2 producer, TV3 Spark and TV4 indexer running; index seeded at version 1.
2. API configured with `RETRIEVER_BACKEND=opensearch` and TV4's embedder.
3. A TV2-compatible publisher wrapper accepting **raw JSONL on stdin**, forwarding records
   unchanged to the raw Kafka topic, and exiting 0 only after broker acknowledgement.
   Use `document_id` as the Kafka key when present; malformed fixtures need a fallback key.
   It must forward malformed fixtures too, so validation happens in Spark, not the wrapper.
4. Test cases with `phase`, `question`, `expected_document_id`, `expected_facts`; revision cases
   also need `expected_version`. Supply event records using each case's `events` array or a
   separate `--events-file`. TV2's larger Q&A set can replace the tiny API-specific fixtures.

For example, from TV5's directory in a POSIX shell, **after TV2 provides the wrapper**:

```bash
python scripts/smoke_test.py \
  --test-cases fixtures/test_cases.jsonl \
  --publish-command '["python","PATH_TO_TV2_JSONL_STDIN_WRAPPER.py"]' \
  --require-real-llm --timeout 120 --poll-interval 1
```

The command is a JSON array and is executed without a shell. The wrapper path is a handoff
placeholder, not a script claimed to exist. Ensure the publisher preserves the stamped
`ingested_at`; timestamps use UTC. Polling observes `/ask` including generation time, so freshness
measures when the answer/source is visible to this client, with polling granularity and clock
accuracy limitations. The script prints `event_time`, `ingested_at`, `first_visible_at`,
freshness and `indexed_at` when returned by the indexer.

The script checks seed, new event, revision and malformed/duplicate inputs, then publishes a
valid barrier and confirms continued API visibility. It checks unique **API sources**; inspect
Spark/indexer counters and OpenSearch separately to prove physical index uniqueness and
malformed-record rejection. A barrier on another Kafka partition does not establish ordering
for every fault input. Reinitialize test data or use fresh stream IDs before repeating a run.
Full smoke with mock generation explicitly skips Case 5. `--require-real-llm` makes mock fail.
Exit code 0 means the selected checks passed; exit code 1 means failure.

## Container handoff to TV1

From TV5's directory:

```bash
cp .env.example .env
docker build -t streamrag-api:tv5 -f services/api/Dockerfile .
docker run --rm --env-file .env -p 127.0.0.1:8000:8000 streamrag-api:tv5
```

The Docker build context must be **TV5's directory**. The image runs as a nonroot user and
contains offline fixtures. For Compose integration TV1 should configure the internal OpenSearch
URL, expose port 8000, and include TV4's shared module/dependencies/model cache in the image or
mounts. Use `host.docker.internal` for a host Ollama server where supported. No Compose integration
or Docker build is claimed as tested on a machine without Docker.

Runtime dependencies are fully pinned in `requirements/runtime.txt`. To intentionally update:

```bash
uv pip compile --python-version 3.11 --universal requirements/runtime.in -o requirements/runtime.txt
```

## References

- [OpenSearch 2.19 k-NN query](https://docs.opensearch.org/2.19/query-dsl/specialized/k-nn/)
- [Ollama generate endpoint](https://docs.ollama.com/api/generate)
- [OpenAI Chat Completions reference](https://developers.openai.com/api/reference/resources/chat)
- [HTTPX timeout semantics](https://www.python-httpx.org/advanced/timeouts/)
