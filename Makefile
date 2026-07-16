.PHONY: ingest test lint

START_DATE ?=
END_DATE ?=
RAW_ROOT ?= data/raw

ingest:
	uv run python -m energy_pipeline.ingest.ree --start-date $(START_DATE) --end-date $(END_DATE) --raw-root $(RAW_ROOT)

test:
	uv run pytest -q

lint:
	uv run ruff check .
