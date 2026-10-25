# StreamRAG — developer commands (Milestone 1).
# Targets owned by other members print "not implemented yet" and exit 1 until their owner fills them in.

SHELL   := /bin/bash
PYTHON  ?= python3
ENV_FILE ?= .env
COMPOSE := docker compose --env-file $(ENV_FILE) -f infra/compose.yaml

define not_implemented
	@echo "make $@: not implemented yet (owner: $(1))" >&2; exit 1
endef

.DEFAULT_GOAL := help
.PHONY: help check-env up down ps logs init init-bucket init-topics init-index \
        seed simulate install-dev lint format test smoke reset

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-12s %s\n", $$1, $$2}'

check-env:
	@test -f $(ENV_FILE) || { echo "Missing $(ENV_FILE). Run: cp .env.example .env" >&2; exit 1; }

# --- Infrastructure (owner: TV1) ------------------------------------------------

up: check-env ## Start the stack, wait until healthy, create the lake bucket
	$(COMPOSE) up -d --wait
	@$(MAKE) --no-print-directory init-bucket

down: check-env ## Stop the stack (keeps data volumes)
	$(COMPOSE) down

ps: check-env ## Show service status
	$(COMPOSE) ps -a

logs: check-env ## Follow logs (optionally: make logs SERVICE=kafka)
	$(COMPOSE) logs -f --tail=100 $(SERVICE)

reset: check-env ## DANGER: stop the stack and DELETE all data volumes
	@read -r -p "This DELETES Kafka, MinIO and OpenSearch data volumes. Type 'yes' to continue: " ans; \
	  [ "$$ans" = "yes" ] || { echo "Aborted."; exit 1; }
	$(COMPOSE) down -v

# --- Bootstrap ------------------------------------------------------------------

init: init-bucket init-topics init-index ## Create bucket, Kafka topics and OpenSearch index

init-bucket: check-env ## Create the data lake bucket (idempotent)
	$(COMPOSE) run --rm minio-init

init-topics: ## Create Kafka topics (owner: TV2)
	$(call not_implemented,TV2 Nguyen Thanh Lam)

init-index: ## Create the OpenSearch index (owner: TV4)
	$(call not_implemented,TV4 Kieu Son Tung)

# --- Data ----------------------------------------------------------------------

seed: ## Publish seed events to the raw topic (owner: TV2)
	$(call not_implemented,TV2 Nguyen Thanh Lam)

simulate: ## Stream live events / updates (owner: TV2)
	$(call not_implemented,TV2 Nguyen Thanh Lam)

# --- Quality -------------------------------------------------------------------

install-dev: ## Install lint/test tools
	$(PYTHON) -m pip install -r requirements/dev.txt

lint: ## Lint and check formatting
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

format: ## Auto-format code
	$(PYTHON) -m ruff format .
	$(PYTHON) -m ruff check --fix .

test: ## Run unit tests
	$(PYTHON) -m pytest tests/unit

smoke: ## End-to-end PASS/FAIL check (owner: TV5)
	$(call not_implemented,TV5 Nguyen Ngoc Minh)
