.PHONY: ingest ingest-aemet test lint

START_DATE ?=
END_DATE ?=

INGEST_ARGS := --start-date $(START_DATE) --end-date $(END_DATE)
ifneq ($(strip $(RAW_ROOT)),)
INGEST_ARGS += --raw-root $(RAW_ROOT)
endif

ingest:
	uv run python -m energy_pipeline.ingest.ree $(INGEST_ARGS)

ingest-aemet:
	uv run python -m energy_pipeline.ingest.aemet $(INGEST_ARGS)

test:
	uv run pytest -q

lint:
	uv run ruff check .
