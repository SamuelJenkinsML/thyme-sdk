.PHONY: test lint fmt

test:
	uv run pytest -v

lint:
	uv run ruff check .

fmt:
	uv run ruff format .
