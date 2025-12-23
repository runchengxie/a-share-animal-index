.PHONY: help install dev daily backfill backfill-years backfill-all chart test lint

CLI ?= zoo-index
CHART ?= zoo-chart
DATE ?=
BACKFILL_YEARS ?= 5

help:
	@echo "Targets:"
	@echo "  make install         Install package"
	@echo "  make dev             Install dev deps"
	@echo "  make daily           Run daily update (DATE=YYYYMMDD optional)"
	@echo "  make backfill        Backfill default window"
	@echo "  make backfill-years  Backfill BACKFILL_YEARS (default 5)"
	@echo "  make backfill-all    Backfill and recompute all"
	@echo "  make chart           Redraw chart from nav.csv"
	@echo "  make test            Run pytest"
	@echo "  make lint            Run ruff"

install:
	pip install .

dev:
	pip install -e ".[dev]"

daily:
	$(CLI) $(if $(DATE),--date $(DATE),)

backfill:
	$(CLI) --backfill

backfill-years:
	$(CLI) --backfill-years $(BACKFILL_YEARS)

backfill-all:
	$(CLI) --backfill --backfill-mode all

chart:
	$(CHART)

test:
	pytest

lint:
	ruff check .
