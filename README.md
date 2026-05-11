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
| `raw.patient_consents` | Deleted patient: WAL DELETE event row present with `deleted_ts IS NOT NULL` (prior rows retained — event log is append-only); consent flip: new WAL UPDATE event row appended with higher `lsn` and updated flag |

#### Manual verification queries (copy-pasteable into `duckdb aidn.duckdb`)

```sql
-- Soft-deleted provider still visible (row preserved, not removed):
SELECT provider_id, deleted_ts FROM raw.providers WHERE deleted_ts IS NOT NULL;

-- CDC DELETE event on patient_consents (deleted patient: row preserved with deleted_ts set):
SELECT patient_id, lsn, deleted_ts FROM raw.patient_consents WHERE deleted_ts IS NOT NULL;

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

- **`patients`, `appointments`** — full event history retained in raw via lsn-distinct WAL rows (pg_replication CDC). Deleted rows are flagged (`deleted_ts IS NOT NULL`) but never removed. The full history is retained for audit.
- **`patient_consents`** — pg_replication CDC event log: one row per WAL event (INSERT/UPDATE/DELETE), ordered by `lsn`. DELETE events produce a row with `deleted_ts IS NOT NULL`; prior rows are retained. SCD2 validity windows are reconstructed at the `intm` layer (deferred — `int_patient_consents_scd2`), not in raw.
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

Consent flags (`consent_research`, `consent_marketing`, `consent_partner_share`) are tracked as a CDC event log in `raw.patient_consents`. Each WAL event (INSERT/UPDATE/DELETE) lands as a separate row identified by `lsn`. A flag flip produces a new event row with the updated value; the prior row is retained — the full audit trail is preserved in the event log. SCD2 validity windows (`valid_from`/`valid_to`) are reconstructed at the `intm` layer (deferred — `int_patient_consents_scd2`).

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
    │  dlt-hub: pg_replication (appointments, providers, patients, patient_consents)
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
    │  patient_consents: CDC event log (one row per WAL event; lsn as merge key)
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

**Source definition.** One `@dlt.source` factory (`aidn_source`, dlt source name `"aidn_ingest"`) returning four resource builders — one per source table. Each resource builder wraps a `pg_replication.replication_resource`; none uses `sql_table`. They are not themselves `@dlt.resource`-decorated. A single `pipeline.run(aidn_source())` call produces one `_dlt_loads` row per committed run. Individual tables can be re-run via `aidn_source().with_resources("<table>")`.

**`make bootstrap` — first-time initialisation.** Must be run once per clean deployment (or after `make clear-dlt-state`):

1. Creates one dedicated replication slot and publication per CDC table (`aidn_providers_slot`, `aidn_appointments_slot`, `aidn_patients_slot`, `aidn_patient_consents_slot`).
2. Calls `init_replication(persist_snapshots=True)` — uses a Postgres-native exported snapshot for an atomic handoff between snapshot LSN and WAL start: zero gap, zero overlap.
3. Slot-existence pre-check via `pg_replication_slots` prevents double-initialisation; if a slot already exists, bootstrap logs `bootstrap_skip reason=slot_exists` and returns without error.

**`make ingest` — steady-state.** All four CDC resources (`appointments`, `providers`, `patients`, `patient_consents`) consume their dedicated WAL slots independently via pg_replication, with `merge` disposition on `lsn`. Each resource has its own slot and confirmed flush LSN, giving independent cadence and failure isolation.

**Schema contract.** All four tables use `schema_contract={"columns": "freeze"}`. An unexpected source column raises a pipeline-blocking `PipelineStepFailed`; it is never silently absorbed.

---

### 3.4 Per-table ingest strategy

| Table | Disposition | Change signal | Delete handling | History at raw |
|---|---|---|---|---|
| `appointments` | `merge` on `event_id` (pg_replication) | WAL events (`lsn` ordering, no cursor) | `deleted_ts` set; row retained (soft-delete) | Full event history (status-change events are separate rows) |
| `patients` | `merge` on `lsn` (pg_replication) | WAL events (`lsn` — no source-timestamp cursor) | Source append-only contract; GDPR erasure via dbt macro | Full event history via lsn-distinct WAL rows |
| `providers` | `merge` on `provider_id` (pg_replication) | WAL events only | `deleted_ts` set; row retained (soft-delete) | Latest state per provider (no history) |
| `patient_consents` | `merge` on `lsn` (pg_replication) | WAL events (lsn ordering) | `deleted_ts` set; row retained (event log append-only); GDPR erasure = hard-delete | Full WAL event history (one row per WAL event) |

**Key notes per table:**

- `appointments`, `providers`, and `patients` all require `REPLICA IDENTITY FULL` — see `seed/init.sql:15,19,22`. Each for a different reason: `appointments` has no Postgres PK; `providers` WAL DEFAULT omits non-PK columns on DELETE (causing `ValidationError` and a silent drop); `patients` has no source PK, so DEFAULT sends no identifying columns on UPDATE/DELETE.
- `patients.name` is dropped by the `_strip_name` preprocessor in `aidn/ingest/preprocess.py` via `resource.add_map(_strip_name)` before rows reach raw. The `Patient` Pydantic model has no `name` field with `extra="forbid"` as a second guard.
- `patient_consents` WAL DELETE events carry only `patient_id`, `lsn`, and `deleted_ts` — consent flag columns are absent; the Pydantic model types them as `bool | None` so DELETE events do not Tier-1 drop. SCD2 reconstruction (valid_from/valid_to) is deferred to the `intm` layer (`int_patient_consents_scd2`).
- Each CDC table has its own dedicated slot (`aidn_providers_slot`, `aidn_appointments_slot`, `aidn_patients_slot`, `aidn_patient_consents_slot`) — independent `confirmed_flush_lsn` per table.

---

### 3.5 Transformation layer (dbt)

**Staging (`staging.*`)** — four dbt views (`stg_appointments`, `stg_patients`, `stg_patient_consents`, `stg_providers`) that project `raw.*` with typed casts only. No dedup, no joins, no consent filtering. Materialized as views per `dbt_project.yml`.

Singular test: `tests/test_no_erased_patient_in_raw.sql` verifies that a completed erasure sweep leaves no rows for the erased `patient_id` in any `raw.*` table.

**GDPR erasure macro (`purge_erased_patients`)** — implemented in `dbt_aidn/macros/purge_erased_patients.sql`:

- Reads `main.erasure_requests` (populated via `dbt seed` from `dbt_aidn/seeds/erasure_requests.csv`) for pending requests (`erased_at IS NULL`).
- Hard-deletes all raw rows in order: `raw.patient_consents → raw.appointments → raw.patients` (child before parent).
- Stamps `erased_at = current_timestamp` on completed requests; idempotent — re-runs are no-ops for already-erased patients.
- Invoke via `make erasure` (`cd dbt_aidn && poetry run dbt run-operation purge_erased_patients`).

**Deferred intm layer — `int_patient_consents_scd2`** — `raw.patient_consents` is now an append-only WAL event log (one row per WAL event, ordered by `lsn`). The `intm` layer will reconstruct the SCD2 validity window using a window function:

```sql
-- int_patient_consents_scd2 (deferred — TO_DO Q.deferred)
select
    patient_id,
    lsn,
    consent_research, consent_marketing, consent_partner_share,
    deleted_ts,
    l.inserted_at                                                    as valid_from,
    coalesce(
        deleted_ts,                                                  -- WAL DELETE: close at actual deletion moment
        lead(l.inserted_at) over (partition by patient_id order by lsn)  -- otherwise: close when next event arrived
    )                                                                as valid_to
from stg_patient_consents s
left join raw._dlt_loads l on s._dlt_load_id = l.load_id
```

`valid_from` is `_dlt_loads.inserted_at` (load time, not source-change time — precision gap equals ingest latency; production fix is adding `updated_at` to the Postgres table). `valid_to = NULL` means the row is currently active. The `marts` layer will enforce consent via the `consented(flag_column)` macro applied to this intm model.

The `intm/`, `marts/`, and `serve/` dbt layers are not yet implemented beyond this design.

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
  - `merge` on `lsn` (patients, patient_consents, pg_replication) — re-processing the same WAL events produces no new rows.

---

### 3.8 Trade-offs & production delta

| Category | This implementation | Production-grade |
|---|---|---|
| **Raw layer storage** | Native DuckDB tables (`raw.*` schema) — erasure can be handled from dbt; one storage system to explain (Postgres → DuckDB, no intermediate filesystem layer) | Parquet could be a production choice (portable, cloud-native, tool-agnostic), but deletes on immutable files require partition rewrites or a table format (Iceberg / Delta Lake). The trade-off accepted here is no native versioning (mitigated by raw tables being append-like; dbt models never `UPDATE`/`DELETE` against `raw.*`) |
| **Orchestration** | `make demo` / manual CLI invocations | |
| **Monitoring** | Structured JSON to stdout — deliberate. Stdout is the universal log contract in containerised environments; any aggregator (Datadog, CloudWatch, Loki) can consume it without a sidecar | Ship the stdout stream to an aggregator |
| **Secrets management** | `.env` (git-ignored) | Vault / AWS Secrets Manager; automatic rotation; no plaintext credentials in any mounted volume |
| **Schema evolution** | dlt `freeze` → `PipelineStepFailed` on new column | Maybe schema registry (although it has big tradeoff too) |
| **Scaling** | Single-host DuckDB | Partitioned Parquet on object storage; dedicated Postgres read replica for CDC to isolate load from the OLTP primary |
| **Security** | Single Postgres superuser; self-signed TLS | Least-privilege roles (ingest / consumer / admin); `sslmode=verify-full` with CA-signed cert; column-level encryption for PII at rest; row-level access controls; immutable audit log on every query touching patient-linked models |
| **CDC slot design** | One replication slot + publication per source table (4 slots for 4 tables) — clean per-table LSN isolation and independent failure domains | At this scale the operational overhead is low and the isolation benefit is real. At production scale with tens of tables, slots multiply: each unconsumed slot holds WAL on disk and a stuck slot can fill the Postgres disk entirely. Production would consolidate to fewer slots (e.g. one slot per consumer group) with slot-lag monitoring and automated alerts on `pg_replication_slots.wal_status = 'lost'` |
| **`patient_consents` SCD2 `valid_from` precision** | `_dlt_loads.inserted_at` (the dlt load timestamp) is used as `valid_from` in the deferred `int_patient_consents_scd2` model — the closest available proxy when the source table has no `updated_at` column. Two events in the same ingest run share the same `valid_from`; `lsn` ordering resolves ties within a load. The validity gap equals the pipeline's ingest latency. | Add `updated_at TIMESTAMPTZ DEFAULT now()` to the `patient_consents` Postgres table; surface it as a CDC column; use it as `valid_from` directly — eliminates the load-latency gap and makes the SCD2 boundary precise to the millisecond. Decision: `decision-documentation.md` Q43. |

---

## 4. Working with AI

This project was developed using [Claude Code](https://claude.ai/code) (Claude Opus 4) as the primary AI assistant. Every pull request was additionally reviewed by a custom GitHub Actions workflow calling **DeepSeek-reasoner (R1)** with a versioned, project-specific system prompt covering correctness, observability, privacy, robustness, and architecture. The reviewer is advisory — it never blocks merge — but its findings fed directly into subsequent iterations. Having two independent models in the loop caught several design gaps that a single-model workflow would have missed. The overall plan was also reviewed by Gemini via Cursor's agent mode to get a third-model perspective on architecture and sequencing. To enforce project-specific standards consistently across sessions, a set of custom Claude Code skills was created covering data integrity, failure robustness, logging, privacy, and pipeline architecture.

---
