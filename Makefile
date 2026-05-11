.PHONY: up down seed bootstrap ingest demo cdc-smoke tls-cert lint typecheck test clear-dlt-state erasure

up:
	docker compose up -d --wait

down:
	docker compose down -v

seed:
	python generate-data.py
	docker compose exec postgres psql -U postgres -d aidn -f /seed/init.sql

tls-cert:
	mkdir -p seed/tls
	openssl req -x509 -newkey rsa:2048 -keyout seed/tls/server.key \
	  -out seed/tls/server.crt -days 365 -nodes \
	  -subj "/CN=localhost"

bootstrap:
	poetry run aidn bootstrap

ingest:
	poetry run aidn ingest

cdc-smoke: ## Run CDC smoke test — assumes `make demo` already ran
	poetry run python scripts/cdc_smoke.py

clear-dlt-state: ## Reset persisted dlt pipeline schema — run before `make demo` after schema-level config changes
	rm -rf .dlt/pipelines/aidn_ingest/ aidn.duckdb

erasure: ## Run GDPR Art. 17 erasure sweep — hard-deletes all raw rows for pending erasure_requests
	cd dbt_aidn && poetry run dbt run-operation purge_erased_patients

lint:
	poetry run ruff check aidn/ tests/

typecheck:
	poetry run mypy --strict aidn/

test:
	poetry run pytest tests/

demo: tls-cert up seed bootstrap ingest
	poetry run python -c "import duckdb; conn = duckdb.connect('$${DUCKDB_PATH:-aidn.duckdb}', read_only=True); print(conn.execute('SELECT status, count(*) FROM raw._dlt_loads GROUP BY 1').fetchall())"
