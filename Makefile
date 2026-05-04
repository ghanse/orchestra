f.PHONY: clean dev ci test integration fmt help

clean:
	rm -rf .venv .pytest_cache .ruff_cache .mypy_cache __pycache__
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

dev:
	uv sync

ci:
	uv sync --frozen

test:
	PYTHONPATH=lib uv run pytest tests/unit -v

integration:
	PYTHONPATH=lib uv run pytest tests/integration -v -m "not slow"

fmt:
	uv run ruff format lib/ tests/
	uv run ruff check lib/ tests/ --fix
	uv run mypy lib/orchestra/

help:
	@echo "Available targets:"
	@echo "  dev          Install dependencies"
	@echo "  ci           Install dependencies (frozen lockfile)"
	@echo "  test         Run unit tests"
	@echo "  integration  Run integration tests"
	@echo "  fmt          Format and lint code"
	@echo "  clean        Remove build artifacts"
