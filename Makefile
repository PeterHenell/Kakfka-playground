# Shortcuts for the Kafka playground. Run `make` to list them.
#
# Every target is a thin wrapper around `podman compose` or `docker compose`;
# see README.md for what the services do.

# Podman if it's installed, otherwise Docker. Override with e.g. ENGINE=docker.
ENGINE ?= $(shell command -v podman >/dev/null 2>&1 && echo podman || echo docker)
COMPOSE ?= $(ENGINE) compose
# Extra flags for `make up` (e.g. UP_FLAGS= to start without rebuilding).
UP_FLAGS ?= --build
# Arguments for `make dbt`, e.g. make dbt ARGS="test --select source:raw".
ARGS ?= build

# Databricks Asset Bundle (dbt/databricks.yml). Put your own values in
# databricks.mk next to this file (it's gitignored), for example:
#   CATALOG = workspace
#   WAREHOUSE_ID = 1234567890abcdef
-include databricks.mk
DATABRICKS ?= databricks
DAB_TARGET ?= dev
CATALOG ?=
WAREHOUSE_ID ?=
DAB_FLAGS = -t $(DAB_TARGET) $(if $(CATALOG),--var catalog=$(CATALOG)) $(if $(WAREHOUSE_ID),--var warehouse_id=$(WAREHOUSE_ID))

.DEFAULT_GOAL := help
.PHONY: help up down restart ps logs dbt spark superset panel invalid clean dab-validate dab-deploy dab-upload dab-run

help: ## List the targets
	@grep -hE '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  make %-13s %s\n", $$1, $$2}'

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
	$(ENGINE) run --rm -v "$(CURDIR):/work" docker.io/library/postgres:16-alpine rm -rf /work/data
	@echo "All data deleted. Start again with: make up"

dab-validate: ## Check the Databricks bundle (log in first: see dbt/README.md)
	cd dbt && $(DATABRICKS) bundle validate $(DAB_FLAGS)

dab-deploy: ## Deploy the Databricks bundle: schema, volume and job
	cd dbt && $(DATABRICKS) bundle deploy $(DAB_FLAGS)

dab-upload: ## Upload ./data/protobuf to the bundle's volume (deploy first)
	@# Ask the bundle for the schema name: in dev it includes your user name.
	@volume=$$(cd dbt && $(DATABRICKS) bundle summary $(DAB_FLAGS) -o json \
		| python3 -c 'import json, sys; s = json.load(sys.stdin)["resources"]["schemas"]["vehicle_telemetry"]; print("/Volumes/" + s["catalog_name"] + "/" + s["name"] + "/raw")') && \
	echo "Uploading ./data/protobuf/vehicle_telemetry to $$volume" && \
	$(DATABRICKS) fs cp -r --overwrite data/protobuf/vehicle_telemetry "dbfs:$$volume/vehicle_telemetry"

dab-run: ## Run the dbt job on Databricks
	cd dbt && $(DATABRICKS) bundle run vehicle_telemetry_dbt_job $(DAB_FLAGS)
