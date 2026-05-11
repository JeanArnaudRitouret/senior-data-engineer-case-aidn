# Aidn Senior Data Engineer — Pipeline

---

## 1. Setup & Run

### 1.1 Prerequisites

| Requirement | Notes |
|---|---|
| **Docker** | Tested with Docker Desktop 4.x; `docker compose` v2 required |
| **Python 3.11** | Must be `>= 3.11, < 3.13`; pinned in `pyproject.toml` |
| **Poetry** | Dependency and virtualenv management; `pip install poetry` |
| **openssl** | Must be on `PATH`; required by `make tls-cert`. Absent → clear error at cert generation. macOS ships it; Linux: `apt install openssl` |
| **duckdb CLI** _(optional)_ | For manual queries against `aidn.duckdb`; not required to run the pipeline |

---

### 1.2 First-time setup

```bash
git clone <repo-url>
cd senior-data-engineer-aidn
cp .env.example .env        # dev-safe defaults; no edits needed for local runs
poetry install
```

---

### 1.3 `make demo` — golden path

```bash
make demo
```

Executes this chain in order:

1. **`make tls-cert`** — generates a self-signed TLS cert in `seed/tls/` using `openssl`. Re-running is a no-op if the cert already exists.
2. **`make up`** — starts Postgres with `wal_level=logical` and SSL enabled; waits for the healthcheck to pass before returning.
3. **`make seed`** — runs `generate-data.py` to produce `seed/*.csv`, then `COPY`s them into Postgres via `seed/init.sql`.
4. **`make bootstrap`** — creates one replication slot and publication per CDC table; loads initial snapshots into DuckDB via `init_replication(persist_snapshots=True)`. Idempotent: slot-existence pre-check logs `bootstrap_skip reason=slot_exists` and returns without re-initialising.
5. **`make ingest`** — runs steady-state CDC ingest; populates `raw.*` in `aidn.duckdb`.

After completion, prints a load-status summary:

```python
# Output shape:
[('loaded', 4)]   # one row per status; count = number of committed dlt load packages
```

To query the destination directly after the run:

```bash
duckdb aidn.duckdb "SELECT status, count(*) FROM raw._dlt_loads GROUP BY 1"
```

---

### 1.4 `make cdc-smoke` — second-load verification

**Precondition:** `make demo` has already run (replication slots and `aidn.duckdb` exist).

```bash
make cdc-smoke
```

The script (`scripts/cdc_smoke.py`) applies exactly **1 INSERT + 1 UPDATE + 1 DELETE per source table** (via `seed/cdc_smoke.sql`), runs `aidn ingest`, then asserts 12 conditions across all four tables. Prints a fixed-width report; exits 1 on any assertion failure.

#### Mutations applied

```sql
-- providers: INSERT new, UPDATE specialty on one existing, DELETE one existing
INSERT INTO providers (provider_id, name, specialty)
VALUES ('SMOKE_PRV_INS', 'Dr Smoke Test', 'gp');

UPDATE providers SET specialty = 'cardiology'
WHERE provider_id = (SELECT provider_id FROM providers
                     WHERE provider_id NOT LIKE 'SMOKE_%'
                     ORDER BY provider_id LIMIT 1);

DELETE FROM providers
WHERE provider_id = (SELECT provider_id FROM providers
                     WHERE provider_id NOT LIKE 'SMOKE_%'
                     ORDER BY provider_id LIMIT 1 OFFSET 1);

-- appointments: INSERT new event, UPDATE status + ingested_at, DELETE one
-- patients: INSERT new version row (bumps updated_at), UPDATE updated_at on existing
--           (DELETE deliberately omitted — source is append-only)
-- patient_consents: INSERT new patient, UPDATE (flip consent_research), DELETE one
```

#### Expected outcome per table

| Table | After second load |
|---|---|
| `raw.providers` | Inserted row present; updated row has new `specialty`; deleted row **preserved** with `deleted_ts IS NOT NULL` (soft-delete, not removed) |
| `raw.appointments` | Inserted event present; updated event has new `status` and `ingested_at`; deleted event has `deleted_ts IS NOT NULL` |
| `raw.patients` | New WAL event emitted as a new lsn-distinct row (prior row retained); WAL UPDATE delivers a new row with `updated_at` and a unique `lsn` |
| `raw.patient_consents` | Deleted patient: prior open SCD2 row closed (`_dlt_valid_to` set), no new row opened; consent flip: old row closed, new row opened with updated flag |

#### Manual verification queries (copy-pasteable into `duckdb aidn.duckdb`)

```sql
-- Soft-deleted provider still visible (row preserved, not removed):
SELECT provider_id, deleted_ts FROM raw.providers WHERE deleted_ts IS NOT NULL;

-- SCD2 closure on patient_consents (deleted patient row retired):
SELECT patient_id, consent_research, _dlt_valid_from, _dlt_valid_to
FROM raw.patient_consents
WHERE _dlt_valid_to IS NOT NULL;

-- Appointment soft-delete:
SELECT event_id, deleted_ts FROM raw.appointments WHERE deleted_ts IS NOT NULL LIMIT 5;
```

---

### 1.5 `make erasure` — GDPR

```bash
make erasure
```

Populate `dbt_aidn/seeds/erasure_requests.csv` with the target `patient_id`, then run:

```bash
make erasure
# equivalent to: cd dbt_aidn && poetry run dbt run-operation purge_erased_patients
```

The `purge_erased_patients` macro hard-deletes all raw rows for that patient across `raw.patient_consents`, `raw.appointments`, and `raw.patients` in that order, then stamps `erased_at` on the seed row. The operation is idempotent: re-running against an already-erased `patient_id` is a no-op (`WHERE erased_at IS NULL` guard).

---

### 1.6 Other Makefile targets

| Target | What it does |
|---|---|
| `make down` | Stops containers and drops all volumes |
| `make test` | `pytest tests/` — unit + integration suites (requires Postgres running) |
| `make lint` | `ruff check aidn/ tests/` |
| `make typecheck` | `mypy --strict aidn/` |
| `make clear-dlt-state` | Removes `.dlt/pipelines/aidn_ingest/` and `aidn.duckdb`; use before re-running `make demo` after schema-level configuration changes |

---

## 2. Assumptions & Impact

### 2.1 Source data assumptions

- **Appointments source has unexplained duplicate rows — treated as an upstream bug, not expected delivery semantics.** There is no business reason to deliver multiple rows for the same `event_id`. Decision: de-duplicate at extraction via pg_replication's `dedup_sort` on `lsn asc` (WAL ordering key); duplicates are not preserved in `raw` because retaining them would create confusion downstream without conveying information. *Trade-off accepted: if the duplicates ever turn out to encode something semantic, that signal would be lost.*

---

### 2.2 Audit-trail vs analytics — SCD2 / soft-delete strategy

Healthcare regulations require an auditable history of every change to patient and consent records, on top of the analytical need for point-in-time queries. This drives different retention strategies per table:

- **`patients`, `patient_consents`, `appointments`** — SCD2 + soft-delete. Every change appends a new row; deleted rows are flagged (`deleted_ts` / `_dlt_valid_to`) but never removed. The full history is retained for audit.
- **`providers`** — soft-delete only (no SCD2). There is no regulatory audit-trail requirement for provider records — only an analytical need to filter active providers. This means prior `name` or `specialty` values are **not retained**: a provider name change overwrites in place. If a prior value is ever needed for recovery, the Postgres WAL backup is the only source; the warehouse cannot reconstruct it.

---

### 2.3 Privacy

This pipeline processes Norwegian healthcare data and is subject to GDPR and Normen (Norsk norm for informasjonssikkerhet i helse- og omsorgstjenesten).

#### PII fields and handling

| Field | Classification | Handling |
|---|---|---|
| `patients.name` | Direct identifier | **Never ingested.** Dropped by `_strip_name` preprocessor (`aidn/ingest/preprocess.py`) at the dlt extraction boundary; `Patient` Pydantic model has no `name` field (`extra="forbid"`) as a second guard; never enters `raw`, staging, or any derived layer |
| `patients.postcode` | Quasi-identifier | Retained in `raw.patients`; removed by GDPR erasure sweep (`dbt run-operation purge_erased_patients`) |
| `patients.patient_id` | Pseudonymous key | Retained as the stable join key; cascade-deleted on GDPR erasure |
| `appointments.patient_id` | Pseudonymous key (links to patient) | Same cascade-delete on GDPR erasure |
| `patient_consents.patient_id` | Pseudonymous key | Same cascade-delete on GDPR erasure |

#### Consent-flag enforcement

Consent flags (`consent_research`, `consent_marketing`, `consent_partner_share`) are tracked as SCD2 history in `raw.patient_consents`. A flag flip closes the prior row (`_dlt_valid_to` set) and opens a new one — the full audit trail is retained.

Consent enforcement at the analytical layer (`marts` and `serve`) is deferred — the `intm/`, `marts/`, and `serve/` dbt layers are not yet implemented. The design specifies a `consented(flag_column)` macro applied at two layers for defense-in-depth, but that macro and the dependent models are not currently on disk. When implemented, enforcement would follow this design:

- **Enforcement point #1 — `marts`:** fact and dimension models join `intm.intm_patient_consents` via `consented(flag_column)` and filter on the named flag.
- **Enforcement point #2 — `serve`:** every OBT model re-applies `consented(flag_column)`.

Currently, the only consent-related artefact on disk is the `raw.patient_consents` SCD2 table and its staging projection `staging.stg_patient_consents`.

#### Production privacy delta

The following controls are absent from this case-study implementation and would be required before any real patient data is processed:

| Gap | Regulatory driver |
|---|---|
| **Right-to-erasure propagation** — `purge_erased_patients` today only sweeps `raw.*`; dbt downstream layers (staging, intm, marts, serve) would need a cascade purge | GDPR; Normen |
| **Encryption at rest** — `aidn.duckdb` unencrypted on local filesystem; production requires encrypted block storage and column-level encryption for `postcode` | GDPR Art. 32; Normen minimum controls |
| **Strong authentication** — single Postgres superuser with password auth; production requires 2FA or BankID/Feide for anyone accessing patient data | Normen (identity management requirements) |

---

### 2.4 Security & access control assumptions

- **Single Postgres superuser** — all connections use one role. RBAC not implemented for the purpose of this case. Production separates ingest, consumer, and admin roles with least-privilege grants.
- **Self-signed TLS with `sslmode=require`** — `make tls-cert` generates a dev-only cert. The pipeline refuses to start if `sslmode` is `disable`, `allow`, or `prefer` (enforced in `aidn/config.py`). The assumption is that data must be transferred securely; `sslmode=require` meets that bar for a dev environment. Production uses a CA-signed cert with `sslmode=verify-full` — `require` alone is MITM-vulnerable even if the cert is valid, because the client does not verify the server's certificate against a trusted CA.

---

## 3. Design & Architecture

### 3.1 Overview diagram

```
Postgres (OLTP)
    │
    │  dlt-hub: pg_replication (appointments, providers, patients)
    │           sql_table full-scan (patient_consents — SCD2)
    │           schema_contract=freeze on all four tables
    │           Pydantic v2 row validation (two-tier: drop+WARN / ERROR+raise)
    ▼
aidn/ingest/  (dlt pipeline + aidn_source() factory wrapping four resource builders)
    │  One replication slot + publication per CDC table (independent LSN tracking)
    │  make bootstrap: init_replication(persist_snapshots=True) → atomic snapshot
    │  run_id = uuid4() per CLI invocation; JSON logs to stdout; PII filter active
    ▼
raw.* (DuckDB — dlt-managed)
    │  _dlt_loads: authoritative committed-run record
    │  _dlt_pipeline_state: incremental cursors per table
    │  appointments / patients: full event history retained (lsn-distinct WAL rows via pg_replication)
    │  providers: latest state per provider_id (merge on provider_id)
    │  patient_consents: SCD2 history (full-snapshot; _dlt_valid_to marks retired rows)
    ▼
staging.stg_* (dbt views — 1-1 typed projection of raw; no dedup, no business logic)
```

---

### 3.2 Layering

| Layer | Schema | Write style | Dedup point | Consent enforced | PII allowed |
|---|---|---|---|---|---|
| Ingest | `raw.*` | dlt-managed: merge / scd2 per table | Appointments and patients: `lsn asc` (pg_replication internal); providers: merge on `provider_id` | No | Yes (except `patients.name` — never ingested) |
| Staging | `staging.*` | dbt views (1-1 projection of raw) | None | No | Yes |

---

### 3.3 Ingest layer mechanics

**Source definition.** One `@dlt.source` factory (`aidn_source`, dlt source name `"aidn_ingest"`) returning four resource builders — one per source table. The resource builders wrap upstream `pg_replication.replication_resource` or `sql_table` objects; they are not themselves `@dlt.resource`-decorated. A single `pipeline.run(aidn_source())` call produces one `_dlt_loads` row per committed run. Individual tables can be re-run via `aidn_source().with_resources("<table>")`.

**`make bootstrap` — first-time initialisation.** Must be run once per clean deployment (or after `make clear-dlt-state`):

1. Creates one dedicated replication slot and publication per CDC table (`aidn_providers_slot`, `aidn_appointments_slot`, `aidn_patients_slot`).
2. Calls `init_replication(persist_snapshots=True)` — uses a Postgres-native exported snapshot for an atomic handoff between snapshot LSN and WAL start: zero gap, zero overlap.
3. Slot-existence pre-check via `pg_replication_slots` prevents double-initialisation; if a slot already exists, bootstrap logs `bootstrap_skip reason=slot_exists` and returns without error.

**`make ingest` — steady-state.** Each CDC resource (`appointments`, `providers`, `patients`) consumes its dedicated WAL slot independently via pg_replication, with `merge` disposition on `lsn`. `patient_consents` runs a full `sql_table` SELECT every run — no cursor, because no source timestamp exists.

**Schema contract.** All four tables use `schema_contract={"columns": "freeze"}`. An unexpected source column raises a pipeline-blocking `PipelineStepFailed`; it is never silently absorbed.

---

### 3.4 Per-table ingest strategy

| Table | Disposition | Change signal | Delete handling | History at raw |
|---|---|---|---|---|
| `appointments` | `merge` on `event_id` (pg_replication) | WAL events (`lsn` ordering, no cursor) | `deleted_ts` set; row retained (soft-delete) | Full event history (status-change events are separate rows) |
| `patients` | `merge` on `lsn` (pg_replication) | WAL events (`lsn` — no source-timestamp cursor) | Source append-only contract; GDPR erasure via dbt macro | Full event history via lsn-distinct WAL rows |
| `providers` | `merge` on `provider_id` (pg_replication) | WAL events only | `deleted_ts` set; row retained (soft-delete) | Latest state per provider (no history) |
| `patient_consents` | `merge` + `scd2` (full-snapshot, no `merge_key`) | `_dlt_loaded_at` (no source timestamp) | Absent row → `_dlt_valid_to` set; GDPR erasure = separate hard-delete | Full SCD2 history |

**Key notes per table:**

- `appointments`, `providers`, and `patients` all require `REPLICA IDENTITY FULL` — see `seed/init.sql:15,19,22`. Each for a different reason: `appointments` has no Postgres PK; `providers` WAL DEFAULT omits non-PK columns on DELETE (causing `ValidationError` and a silent drop); `patients` has no source PK, so DEFAULT sends no identifying columns on UPDATE/DELETE.
- `patients.name` is dropped by the `_strip_name` preprocessor in `aidn/ingest/preprocess.py` via `resource.add_map(_strip_name)` before rows reach raw. The `Patient` Pydantic model has no `name` field with `extra="forbid"` as a second guard.
- `patient_consents` full-snapshot SCD2: dlt closes any row absent from the current SELECT by setting `_dlt_valid_to`; no separate DELETE signal is needed.
- Each CDC table has its own dedicated slot (`aidn_providers_slot`, `aidn_appointments_slot`, `aidn_patients_slot`) — independent `confirmed_flush_lsn` per table.

---

### 3.5 Transformation layer (dbt)

**Staging (`staging.*`)** — four dbt views (`stg_appointments`, `stg_patients`, `stg_patient_consents`, `stg_providers`) that project `raw.*` with typed casts only. No dedup, no joins, no consent filtering. Materialized as views per `dbt_project.yml`.

Singular test: `tests/test_no_erased_patient_in_raw.sql` verifies that a completed erasure sweep leaves no rows for the erased `patient_id` in any `raw.*` table.

**GDPR erasure macro (`purge_erased_patients`)** — implemented in `dbt_aidn/macros/purge_erased_patients.sql`:

- Reads `main.erasure_requests` (populated via `dbt seed` from `dbt_aidn/seeds/erasure_requests.csv`) for pending requests (`erased_at IS NULL`).
- Hard-deletes all raw rows in order: `raw.patient_consents → raw.appointments → raw.patients` (child before parent).
- Stamps `erased_at = current_timestamp` on completed requests; idempotent — re-runs are no-ops for already-erased patients.
- Invoke via `make erasure` (`cd dbt_aidn && poetry run dbt run-operation purge_erased_patients`).

The `intm/`, `marts/`, and `serve/` dbt layers are not yet implemented.

---

### 3.6 Observability

- **`run_id`** — `uuid4()` generated once per `aidn ingest` CLI invocation; injected into every log record via `bind_run_id`. Not propagated into derived dbt layers — cross-run joins would produce stale or ambiguous values; the `_dlt_loads` row is the durable audit record per run.
- **Structured JSON logs** — `python-json-logger` writes to stdout; key=value pairs on every state-changing operation. Every summary line carries `run_id`, `table`, `rows_loaded`, and (when applicable) `rows_dropped` with a `reason` string.
- **`PiiSafeFilter`** — logging filter that redacts `name`, `postcode`, and any free-text patient-linked field before it reaches stdout.
- **`raw._dlt_loads`** — dlt's authoritative committed-run record. One row per successful `pipeline.run()`. Rows in `raw.*` tables carry `_dlt_load_id` as the join key back to this table.
- **Known gap** — a run that crashes before the dlt package commit leaves no `_dlt_loads` row. The failure is visible in the process exit code and stderr JSON only; no durable failure record is written to DuckDB in the current implementation.

---

### 3.7 Failure & recovery

- **DB connection failures** — connect failures surface as `psycopg2`/`psycopg3` exceptions; not retried at the source level. The CLI wraps `pipeline.run()` in `try/except PipelineStepFailed`, logs at ERROR with `exc_info=True`, and re-raises. Retry-with-backoff on connect is a known production gap (see §3.8).
- **Pipeline run failures** — `pipeline.run()` raises `PipelineStepFailed` on any hard dlt error; caught at the CLI boundary, logged at ERROR with `exc_info=True`, and re-raised. `PipelineStepFailed` is the default dlt contract; `has_failed_jobs` / `raise_on_failed_jobs()` are reserved for the explicit `raise_on_failed_jobs=False` opt-out path.
- **Crash-resume** — dlt persists load packages in `DLT_DATA_DIR`. A run that dies mid-flight leaves a partial package; the next `pipeline.run()` resumes automatically from the last committed checkpoint. `DLT_DATA_DIR` must be on persistent storage (not container-local ephemeral).
- **Idempotent re-runs by disposition:**
  - `merge` — re-processing the same WAL events produces no new rows.
  - `scd2` (patient_consents) — no `_dlt_valid_to` mutations on re-run of an identical snapshot.
  - `merge` on `lsn` (patients, pg_replication) — same as appointments: re-processing the same WAL events produces no new rows.

---

### 3.8 Trade-offs & production delta

| Category | This implementation | Production-grade |
|---|---|---|
| **Orchestration** | `make demo` / manual CLI invocations | Airflow or Prefect DAG; retries per task; SLA-paging on missed runs; separate staging and production environments |
| **Monitoring** | stdout JSON | Datadog / Grafana; alert on `rows_dropped` spike, `_dlt_loads` gap (missed run), or `PipelineStepFailed` per table |
| **Secrets management** | `.env` (git-ignored) | Vault / AWS Secrets Manager; automatic rotation; no plaintext credentials in any mounted volume |
| **Schema evolution** | dlt `freeze` → `PipelineStepFailed` on new column | Schema registry; source DDL changes require a PR against column-hint declarations; migration tooling for additive changes |
| **Scaling** | Single-host DuckDB | Partitioned Parquet on object storage + Spark / Trino for `intm` joins on large event tables; multiple dlt workers per table; dedicated Postgres read replica for CDC to isolate load from the OLTP primary |
| **Security** | Single Postgres superuser; self-signed TLS | Least-privilege roles (ingest / consumer / admin); `sslmode=verify-full` with CA-signed cert; column-level encryption for PII at rest; row-level access controls in `serve.*`; immutable audit log on every query touching patient-linked models |

---
