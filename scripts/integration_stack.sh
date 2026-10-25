#!/usr/bin/env bash
# Run from Linux/WSL with Docker Engine + Compose; keeps all data on down.
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
ENV_FILE=${ENV_FILE:-$ROOT/members/TV1-Pham-Chi-Bang-lead-architecture-devops/.env.example}
set -a
source <(sed 's/\r$//' "$ENV_FILE")
RAW_TOPIC=${RAW_TOPIC:-streamrag.raw.v1}
CLEANED_TOPIC=${CLEANED_TOPIC:-streamrag.cleaned.v1}
OPENSEARCH_INDEX=${OPENSEARCH_INDEX:-streamrag-docs-v1}
LAKE_BUCKET=${LAKE_BUCKET:-streamrag-lake}
CHECKPOINT_ROOT=${CHECKPOINT_ROOT:-s3a://$LAKE_BUCKET/checkpoints/$RAW_TOPIC}
set +a
cd "$ROOT"
compose=(docker compose --project-directory "$ROOT" --env-file "$ENV_FILE"
  -f "$ROOT/members/TV1-Pham-Chi-Bang-lead-architecture-devops/infra/compose.yaml"
  -f "$ROOT/infra/integration.compose.yaml")
run_python() {
  docker run --rm --network streamrag_default \
    -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 -e RAW_TOPIC="$RAW_TOPIC" -e CLEANED_TOPIC="$CLEANED_TOPIC" \
    -e OPENSEARCH_URL=http://opensearch:9200 -e OPENSEARCH_INDEX="$OPENSEARCH_INDEX" \
    -v "$ROOT/data/generated:/reports" streamrag-python:dev "$@"
}
case ${1:-help} in
  up)
    mkdir -p data/generated
    "${compose[@]}" up -d --wait kafka minio opensearch
    "${compose[@]}" run --rm -e LAKE_BUCKET="$LAKE_BUCKET" minio-init
    DOCKER_BUILDKIT=0 docker build -f services/indexer/Dockerfile -t streamrag-python:dev .
    run_python python infra/kafka/init_topics.py
    run_python python infra/opensearch/init_index.py
    DOCKER_BUILDKIT=0 docker build -f members/TV3-Nguyen-Phu-An-spark-datalake/services/stream_processor/Dockerfile \
      -t streamrag-stream-processor:dev members/TV3-Nguyen-Phu-An-spark-datalake/services
    "${compose[@]}" up -d --wait --wait-timeout 300 spark indexer api
    ;;
  seed) run_python python -m services.producer.cli ;;
  smoke)
    run_python python scripts/verify_pipeline.py --base-url http://api:8000 --report /reports/pipeline-report.json
    ;;
  real-llm)
    "${compose[@]}" --profile llm up -d --wait --wait-timeout 120 ollama
    "${compose[@]}" exec -T ollama ollama pull "${LLM_MODEL:-qwen2.5:0.5b}"
    LLM_PROVIDER=ollama "${compose[@]}" up -d --force-recreate api
    ;;
  status) "${compose[@]}" --profile llm ps ;;
  logs) "${compose[@]}" logs --tail 50 spark indexer api ;;
  down) "${compose[@]}" --profile llm down ;;
  *) echo "Usage: ENV_FILE=/absolute/path/to/env bash scripts/integration_stack.sh {up|seed|smoke|real-llm|status|logs|down}" ;;
esac
