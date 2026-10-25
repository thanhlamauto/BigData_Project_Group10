PYTHON ?= python3
SMOKE_ARGS ?=

.PHONY: install run test smoke

install:
	$(PYTHON) -m pip install -r requirements/dev.txt

run:
	$(PYTHON) -m uvicorn services.api.main:app --host 127.0.0.1 --port 8000

test:
	$(PYTHON) -m pytest tests

smoke:
	$(PYTHON) scripts/smoke_test.py $(SMOKE_ARGS)
