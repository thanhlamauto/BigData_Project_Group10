# Integration evidence — 26/10/2026

Observed on one Windows 11 / Ubuntu 22.04 WSL2 machine (8 GB host RAM), using
Docker Engine 29.9.2, Kafka 3.9.1, Spark 3.5.9, MinIO, OpenSearch 2.19.4,
MiniLM-L6-v2 ONNX embeddings, TV5's API, and Ollama 0.6.8 with Qwen2.5 0.5B.

## Real streaming path

`pipeline-2026-10-26.json` records API visibility after publishing **only to raw
Kafka**. Spark read the stream, wrote Bronze/Silver/rejected Parquet to MinIO,
and produced the cleaned topic; the indexer consumed cleaned records and wrote
version-aware OpenSearch documents. API retrieval used real normalized embeddings
and the real vector index. This deterministic smoke used the labelled mock LLM.

- 400 seed records were indexed; Atlas v1 reported 20:30 UTC.
- A previously absent Quasar Beacon document became visible in 12.794 seconds.
- Atlas v2 became visible in 8.799 seconds and reported 21:30 UTC.
- 26 edge cases plus a progress barrier were published. The pipeline continued.
- OpenSearch preserved canonical Atlas `_version=2` when stale v1 arrived;
  duplicate/equal versions were logged as 409 no-ops. The index contained 407
  unique logical documents after five new late documents and two probe documents.
- `lake-2026-10-26.json` independently reads **429 Bronze, 424 Silver, 5 rejected**
  from `s3a://streamrag-e2e-20261026`, with all five expected rejection reasons.

These are observations from a single small run, not a latency benchmark or an
exactly-once proof. Historical late fixtures intentionally remain separate logical
documents. Some share titles with the current notice; broad queries can retrieve
them. The first smoke report retains that historical context rather than hiding it.
The API now honors explicit `notice NNN` and `document_id=ID` selectors. Canonical
revision verification uses `document_id=inc-atlas-001`, avoiding ambiguity among
different documents. General answer quality has not been evaluated.

## Genuine generation

`real-llm-2026-10-26.json` is an HTTP `/ask` response using **Ollama**, not mock:

> Atlas Search maintenance starts at 21:30 UTC on October 27, 2026.

Its only source is `inc-atlas-001`, version 2. `llm_ms=8814.775` is the observed
generation time for this one response. The endpoint queried OpenSearch through
the same MiniLM implementation used for indexing. This proves an actual local
LLM call for one known case; it does not establish production model quality.

## Recovery and packaged stack

The host's C drive ran out of space while loading the LLM, disrupting WSL/Docker.
Runtime/model caches were moved to D, pip download cache was cleared and redirected
to D, and WSL swap/crash output was redirected to D. Ubuntu's VHD and Docker data
were already on D. The stack was then restored without deleting lake/index data.

`scripts/integration_stack.sh up` subsequently built and started the packaged
Compose services. Kafka, MinIO, OpenSearch, Spark, indexer and API reached healthy
or running state. Indexer/API ran in Docker for this recovery check; the first
smoke and real-LLM check used native Python clients/API on Windows with Docker
backends. This distinction is intentional and recorded.

`recovery-2026-10-26.json` verifies the canonical answer remains v2/21:30 after
restart, then publishes a new event through Kafka. Spark resumed at **batch 4,
input=1**, using the existing MinIO checkpoint. OpenSearch count changed from
407 to 408, and the new document appeared through the containerized API. The
previous 400-record seed was not replayed into the lake.

## Checks and remaining work

- Python tests: **92 passed**; the Spark module is skipped in the host Python
  environment because PySpark is not installed there.
- Spark tests in the pinned Docker image: **30 passed** separately.
- Ruff lint/format and shell syntax checks pass; Compose base and integration
  configuration are validated.
- Lake and cleaned-topic writes remain at-least-once. Global Silver deduplication
  and watermark behavior are not implemented; external versioning protects search.
- No independent reviewer approval or second person's clean-machine rehearsal is
  represented by this evidence. M1-17 remains open. No milestone completion or
  release tag is asserted.

Reproduction and fresh-namespace instructions: [indexer/stack README](../../services/indexer/README.md).
