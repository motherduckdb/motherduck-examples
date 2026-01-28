.PHONY: tests load-md-small load-md-large

load-md-small:
	uv run python -m python_ingestion.load_to_motherduck_small

load-md-large:
	uv run python -m python_ingestion.load_to_motherduck_large

test:
	uv run pytest tests
