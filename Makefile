.PHONY: help install dev daily backfill backfill-years backfill-all chart test lint format

CLI ?= uv run zoo-index
CHART ?= uv run zoo-chart
DATE ?=
BACKFILL_YEARS ?= 5
OUTPUT_DIR ?= .build/data

help:
	@echo "Targets:"
	@echo "  make install         Install deps via uv"
	@echo "  make dev             Install deps via uv"
	@echo "  make daily           Run daily update (DATE=YYYYMMDD optional)"
	@echo "  make backfill        Backfill default window"
	@echo "  make backfill-years  Backfill BACKFILL_YEARS (default 5)"
	@echo "  make backfill-all    Backfill and recompute all"
	@echo "  make chart           Redraw chart from nav.csv"
	@echo "  make test            Run pytest"
	@echo "  make lint            Run ruff check"
	@echo "  make format          Run ruff format"

install dev:
	uv sync

daily:
	$(CLI) --output-dir $(OUTPUT_DIR) $(if $(DATE),--date $(DATE),)

backfill:
	$(CLI) --output-dir $(OUTPUT_DIR) --backfill

backfill-years:
	$(CLI) --output-dir $(OUTPUT_DIR) --backfill-years $(BACKFILL_YEARS)

backfill-all:
	$(CLI) --output-dir $(OUTPUT_DIR) --backfill --backfill-mode all

chart:
	$(CHART)

test:
	uv run pytest

lint:
	uv run ruff check .

format:
	uv run ruff format .
