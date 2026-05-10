.PHONY: up down seed ingest demo tls-cert lint typecheck test

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

ingest:
	poetry run aidn ingest

lint:
	poetry run ruff check aidn/ tests/

typecheck:
	poetry run mypy --strict aidn/

test:
	poetry run pytest tests/

demo: tls-cert up seed ingest
	poetry run duckdb $${DUCKDB_PATH:-aidn.duckdb} \
	  -c "SELECT status, count(*) FROM raw._dlt_loads GROUP BY 1;"
