.PHONY: format format-check lint type test check

format:
	uv run ruff check --fix src tests
	uv run ruff format src tests

format-check:
	uv run ruff format --check src tests

lint:
	uv run ruff check src tests

type:
	uv run mypy src
	uv run basedpyright

test:
	uv run pytest --cov=parallax

check: format-check lint type test
