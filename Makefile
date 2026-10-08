PY ?= python

.PHONY: help setup generate-day1 generate-day2 pipeline demo test lint lint-sql tf-static check clean

help: ## list the available targets
	@grep -E '^[a-z0-9-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-16s %s\n", $$1, $$2}'

setup: ## install the package and the dev tools into the current environment
	$(PY) -m pip install -e ".[dev]"

generate-day1: ## generate the full historical landing batch (synthetic)
	$(PY) -m hdp.generate --day 1

generate-day2: ## generate the next-day incremental extract (needs day 1 first)
	$(PY) -m hdp.generate --day 2

pipeline: ## run bronze -> silver -> gold on whatever is new in the landing zone
	$(PY) -m hdp.run

demo: clean ## two-day walkthrough: full load, then an incremental load
	$(MAKE) generate-day1
	$(MAKE) pipeline
	$(MAKE) generate-day2
	$(MAKE) pipeline

test: ## unit and end-to-end tests (needs Java 17+ for Spark)
	$(PY) -m pytest

lint: ## Python lint and format check
	$(PY) -m ruff check .
	$(PY) -m ruff format --check .

lint-sql: ## T-SQL lint
	$(PY) -m sqlfluff lint sql

tf-static: ## Terraform cross-checks that need no cloud access or Terraform binary
	$(PY) scripts/check_terraform_static.py

check: lint lint-sql tf-static test ## everything CI runs locally, except terraform validate

clean: ## remove generated data and Spark scratch files
	rm -rf data spark-warehouse metastore_db derby.log
