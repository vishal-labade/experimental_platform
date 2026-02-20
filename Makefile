SHELL := /bin/bash
PY := python3
VENV := .venv
PIP := $(VENV)/bin/pip
PYV := $(VENV)/bin/python

.PHONY: help setup install synth lint test clean

help:
	@echo "Targets:"
	@echo "  make setup     - create venv + install deps"
	@echo "  make install   - install editable + dev deps"
	@echo "  make synth     - generate synthetic parquet (local or minio) via module entrypoint"
	@echo "  make lint      - run ruff (if configured)"
	@echo "  make test      - run pytest"
	@echo "  make clean     - remove venv + caches"

setup:
	$(PY) -m venv $(VENV)
	$(PIP) install -U pip
	$(MAKE) install

install:
	$(PIP) install -e ".[dev]"

# Default config is configs/dev.yaml. Override like:
# make synth CFG=configs/dev.yaml TARGET=minio
synth:
	$(PYV) -m exp_platform.generate_synth --config $${CFG:-configs/dev.yaml} --target $${TARGET:-local}

lint:
	$(PYV) -m ruff check exp_platform || true

test:
	$(PYV) -m pytest -q || true

clean:
	rm -rf $(VENV)
	rm -rf .pytest_cache .ruff_cache **/__pycache__
