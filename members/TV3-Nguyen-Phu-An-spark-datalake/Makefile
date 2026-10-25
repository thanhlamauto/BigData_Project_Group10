# TV3 stream processor commands. Run from this folder:  make <target>
# Everything runs in Docker with the pinned Spark image, so no local Java/Spark is needed.
# run-* targets need Kafka + MinIO up (`make up` in the TV1 folder) and the raw topic created.

SHELL       := /bin/bash
REPO_ROOT   := $(shell git rev-parse --show-toplevel)
TV1_DIR     := $(REPO_ROOT)/members/TV1-Pham-Chi-Bang-lead-architecture-devops
TV3_REL     := members/$(notdir $(CURDIR))
SPARK_IMAGE := apache/spark:3.5.9-scala2.12-java17-python3-ubuntu
IMAGE       := streamrag-stream-processor:dev
NETWORK     := streamrag_default
ENV_FILE    ?= $(firstword $(wildcard $(TV1_DIR)/.env) $(TV1_DIR)/.env.example)
RAW_TOPIC   ?= streamrag.raw.v1
FIXTURES    ?= tests/fixtures/raw_events_dev.jsonl
KAFKA_CLI   := docker exec -i streamrag-kafka /opt/kafka/bin
# Inside the Compose network services are reached by name, not localhost.
RUN_JOB     := docker run --rm --network $(NETWORK) --env-file $(ENV_FILE) \
                 -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 -e MINIO_ENDPOINT=http://minio:9000

.DEFAULT_GOAL := help
.PHONY: help test image topic produce-dev run-once run inspect

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-12s %s\n", $$1, $$2}'

test: ## Unit tests (transforms) on a local SparkSession inside the Spark image
	docker run --rm -u root -e PYTHONDONTWRITEBYTECODE=1 \
	  -e PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
	  -v $(REPO_ROOT):/repo -w /repo --entrypoint bash $(SPARK_IMAGE) -c \
	  "pip install -q --disable-pip-version-check pytest==8.3.3 && \
	   python3 -m pytest -p no:cacheprovider --rootdir . \
	   -c members/TV1-Pham-Chi-Bang-lead-architecture-devops/pyproject.toml $(TV3_REL)/tests"

image: ## Build the stream processor image (bakes in the Kafka + S3A JARs)
	docker build -q -f services/stream_processor/Dockerfile -t $(IMAGE) services

topic: ## Spike only: create the raw topic if missing (the real script is owned by TV2)
	$(KAFKA_CLI)/kafka-topics.sh --bootstrap-server localhost:29092 --create --if-not-exists \
	  --topic $(RAW_TOPIC) --partitions 3 --replication-factor 1

produce-dev: ## Publish FIXTURES to the raw topic, key = document_id
	python3 -c 'import re, sys; \
	  [print((m.group(1) if (m := re.search(r"\"document_id\": *\"([^\"]+)\"", l)) else "no-key") + "|" + l.rstrip("\n")) for l in sys.stdin]' \
	  < $(FIXTURES) | $(KAFKA_CLI)/kafka-console-producer.sh --bootstrap-server localhost:29092 \
	  --topic $(RAW_TOPIC) --property parse.key=true --property key.separator='|'
	@echo "published $$(wc -l < $(FIXTURES) | tr -d ' ') records to $(RAW_TOPIC)"

run-once: image ## Process everything new on the raw topic, then exit (trigger availableNow)
	$(RUN_JOB) -e TRIGGER=availableNow $(IMAGE)

run: image ## Run the streaming job continuously (5 s micro-batches); Ctrl-C to stop
	$(RUN_JOB) -it --name streamrag-stream-processor $(IMAGE)

inspect: image ## Read Bronze/Silver back from the lake and print counts + a Silver sample
	$(RUN_JOB) $(IMAGE) /opt/spark/bin/spark-submit /opt/streamrag/stream_processor/inspect_lake.py
