# Shortcuts for the Kafka playground. Run `make` to list them.
#
# Every target is a thin wrapper around docker compose; see README.md for
# what the services do.

COMPOSE ?= docker compose
# Extra flags for `make up` (e.g. UP_FLAGS= to start without rebuilding).
UP_FLAGS ?= --build
# Arguments for `make dbt`, e.g. make dbt ARGS="test --select source:raw".
ARGS ?= build

.DEFAULT_GOAL := help
.PHONY: help up down restart ps logs dbt spark superset panel invalid clean

help: ## List the targets
	@grep -hE '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  make %-10s %s\n", $$1, $$2}'

up: ## Build and start everything (Kafka, ELK, consumers, control panel, Superset)
	$(COMPOSE) up -d $(UP_FLAGS)
	@echo
	@echo "Control panel (links to all other tools): http://localhost:8090"

down: ## Stop everything (data is kept)
	$(COMPOSE) --profile dbt down

restart: down up ## Stop and start everything again

ps: ## Show the status of the services
	$(COMPOSE) ps

logs: ## Follow the logs of the producer and consumers (Ctrl+C to stop)
	$(COMPOSE) logs -f producer consumer protobuf-consumer

dbt: ## Run dbt on Spark: `dbt build` by default, or make dbt ARGS="..."
	$(COMPOSE) run --rm dbt $(ARGS)

spark: ## Open a Spark SQL shell on the dbt tables
	$(COMPOSE) run --rm spark-sql

superset: ## Show where Superset runs and how to log in
	@echo "Superset: http://localhost:8088 (log in with admin / admin; SQL Lab: http://localhost:8088/sqllab/)"

panel: ## Show where the control panel runs
	@echo "Control panel: http://localhost:8090"

invalid: ## Publish a few invalid messages, to see the dbt source tests fail
	$(COMPOSE) run --rm --no-deps producer python -u producer/send_invalid.py

clean: ## Stop everything and delete ALL data: Kafka, Elasticsearch, Superset, ./data
	$(COMPOSE) --profile dbt down -v --remove-orphans
	@# ./data is written by the containers (as root), so delete it from a container too.
	docker run --rm -v "$(CURDIR):/work" postgres:16-alpine rm -rf /work/data
	@echo "All data deleted. Start again with: make up"
