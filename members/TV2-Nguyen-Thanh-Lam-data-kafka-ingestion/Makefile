# TV2 entry points; run from the repository root with make -C members/TV2-... <target>.
PYTHON ?= python
REPO_ROOT := ../..
ARGS ?=

.PHONY: init-topics seed simulate generate test
init-topics:
	cd $(REPO_ROOT) && $(PYTHON) infra/kafka/init_topics.py $(ARGS)
seed:
	cd $(REPO_ROOT) && $(PYTHON) -m services.producer.cli $(ARGS)
simulate:
	cd $(REPO_ROOT) && $(PYTHON) scripts/stream_simulator.py $(ARGS)
generate:
	cd $(REPO_ROOT) && $(PYTHON) scripts/generate_data.py --seed 42 --n 400 --all
test:
	cd $(REPO_ROOT) && $(PYTHON) -m pytest tests/unit -q
