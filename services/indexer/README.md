# Shared indexer and end-to-end stack

The merged TV2 producer and TV3 Spark job now connect to TV5's API through a
version-aware OpenSearch indexer. TV5's existing API stays in its member folder;
the shared `services` namespace includes it through `PYTHONPATH`. Model inference
uses FastEmbed's ONNX MiniLM-L6-v2 (384 dimensions), loaded once per process.
Both indexer and query API import the same normalized `Embedder` implementation.

From the repository root in Linux/WSL, with Docker Engine and Compose installed:

```bash
bash scripts/integration_stack.sh up
bash scripts/integration_stack.sh seed
# Wait for /health and for the initial documents to reach OpenSearch.
bash scripts/integration_stack.sh smoke
```

First startup downloads images and the real embedding model. The automated smoke
needs an index whose Atlas document is still v1; it appends new/live/edge records
through Kafka and checks API visibility, version replacement and a progress
barrier after malformed input. Reports go to ignored `data/generated/`.
It uses a mock LLM for deterministic assertions while Kafka, Spark, MinIO,
embedding and OpenSearch are real. To use an actual local LLM afterward:

```bash
bash scripts/integration_stack.sh real-llm
curl http://localhost:8000/ask -H 'Content-Type: application/json' \
  -d '{"question":"When does maintenance start? document_id=inc-atlas-001","top_k":1}'
bash scripts/integration_stack.sh down
```

`real-llm` pulls Qwen2.5 0.5B into the Ollama model volume and recreates the API with
`LLM_PROVIDER=ollama`. Give it time to finish loading the model. Output from this
small CPU model needs review; successful generation does not establish general
answer quality. The stack uses several GB of RAM; on an 8 GB machine, close other
heavy applications and keep caches/swap on a drive with adequate free space.

`down` preserves volumes. Never reset an index or checkpoint just to make the
smoke green. For another clean scenario, copy the TV1 environment template to an
ignored local file, choose fresh `RAW_TOPIC`, `CLEANED_TOPIC`, `OPENSEARCH_INDEX`,
`INDEXER_GROUP`, and `LAKE_BUCKET`, and pass its absolute path through `ENV_FILE`
for every command. The script derives a source-specific checkpoint path. The
embedding cache and LLM weights are Docker volumes and are never committed.

## Correctness and limitations

- Spark writes Bronze/Silver/rejected Parquet to MinIO, then sends valid events to
  the cleaned topic. Timestamp strings use UTC `Z`; Spark metadata is retained.
  Within one micro-batch it deduplicates cleaned events by `event_id`. Silver still
  retains raw replays across batches. This is not global watermark deduplication.
- There is no distributed transaction between Parquet, Kafka and OpenSearch.
  A retried batch can duplicate lake files or cleaned events. Kafka consumption is
  at-least-once; checkpoints recover offsets. Do not claim end-to-end exactly-once.
- OpenSearch `_id=document_id`, `version_type=external` makes higher versions
  replace lower versions atomically. Equal/older versions return 409 and are
  acknowledged as no-ops. The consumer commits its Kafka offset only after a
  successful write or such a conflict. Transient write failures retry with bounded
  backoff, then stop without acknowledging the failed record.
- Source URLs remain null for synthetic data. API sources expose document ID,
  version and times. Explicit `notice NNN` queries filter that notice number;
  `document_id=ID` filters the canonical logical document before vector search.
  Historical fixtures can share titles with current notices, so broad ambiguous
  questions may retrieve multiple historical documents. Use the returned IDs to
  disambiguate a follow-up; do not infer current state from title alone.
- Rebuild by creating a new index name and indexer consumer group, then replaying
  retained cleaned Kafka events. Never reset a live group's offsets while it runs.
  Index initialization checks embedding dimension and refuses incompatible indexes.
  Switching embedding models requires a new index, not just a changed environment.

See `docs/evidence/` for the observed run and its precise scope. Independent human
review and a second person's clean-machine rehearsal remain outstanding.
