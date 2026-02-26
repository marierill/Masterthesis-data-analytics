.PHONY: help install install-dev lint format test

help:
	@echo "Targets:"
	@echo "  install     Install runtime dependencies"
	@echo "  install-dev Install dev dependencies"
	@echo "  lint        Run ruff lint"
	@echo "  format      Run black formatting + ruff autofix"
	@echo "  test        Run pytest"

install:
	python -m pip install --upgrade pip
	pip install -r requirements.txt

install-dev:
	pip install -r requirements-dev.txt

lint:
	ruff check .

format:
	black .
	ruff check . --fix

test:
	pytest