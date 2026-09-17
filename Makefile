.PHONY: format format-check lint type test check

format:
	uv run black src tests
	uv run ruff check --fix src tests

format-check:
	uv run black --check src tests

lint:
	uv run ruff check src tests

type:
	uv run mypy src
	uv run pyright

test:
	uv run pytest --cov=parallax

check: format-check lint type test
