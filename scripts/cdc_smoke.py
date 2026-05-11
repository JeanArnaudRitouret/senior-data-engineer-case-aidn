"""CDC smoke test — mutates Postgres, re-ingests, reports propagation to DuckDB raw layer.

Run `make demo` once before this script to establish a baseline load.
This script is a one-shot demonstration tool, not an idempotent re-runnable test.
A second invocation without resetting the container will fail on duplicate PK inserts.
"""

from __future__ import annotations

import logging
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from aidn.config import Settings
from aidn.logging_setup import bind_run_id, configure_logging, get_logger

_REPO_ROOT = Path(__file__).parents[1]
_SMOKE_SQL = _REPO_ROOT / "seed" / "cdc_smoke.sql"
_LOG = get_logger(__name__)

_TICK = "✓"
_CROSS = "✗"


@dataclass
class _TargetIds:
    """Deterministic row identifiers captured from DuckDB before mutations are applied."""

    first_provider_id: str
    second_provider_id: str
    first_event_id: str
    second_event_id: str
    first_patient_id: str
    first_consent_patient_id: str
    delete_patient_id: str
    delete_consent_patient_id: str


@dataclass
class MutationResult:
    """One row in the CDC smoke diff report."""

    table: str
    mutation: str
    expected: str
    observed: str
    result: str


def _fetchone_str(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    params: list[Any] | None = None,
) -> str:
    """Execute sql and return the first column of the first row as str.

    Args:
        conn: Open DuckDB connection.
        sql: SQL statement to execute.
        params: Optional positional parameters.

    Returns:
        First column of the first row as a string.

    Raises:
        RuntimeError: If the query returns no rows or a null value.
    """
    row = conn.execute(sql, params or []).fetchone()
    if row is None or row[0] is None:
        raise RuntimeError(f"Expected a non-null row from: {sql!r}")
    return str(row[0])


def _fetchone_int(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    params: list[Any] | None = None,
) -> int:
    """Execute sql and return the first column of the first row as int.

    Args:
        conn: Open DuckDB connection.
        sql: SQL statement to execute.
        params: Optional positional parameters.

    Returns:
        First column of the first row as an integer.

    Raises:
        RuntimeError: If the query returns no rows or a null value.
    """
    row = conn.execute(sql, params or []).fetchone()
    if row is None or row[0] is None:
        raise RuntimeError(f"Expected a non-null row from: {sql!r}")
    return int(row[0])


def _check_baseline(conn: duckdb.DuckDBPyConnection) -> str:
    """Return the load_id of the most recent committed dlt load.

    Args:
        conn: Open DuckDB connection.

    Returns:
        Load ID of the most recent committed dlt load package.
    """
    try:
        return _fetchone_str(
            conn,
            "SELECT load_id FROM raw._dlt_loads"
            " WHERE status=0 ORDER BY inserted_at DESC LIMIT 1",
        )
    except RuntimeError:
        sys.exit(
            "No committed loads found in raw._dlt_loads.\n"
            "Run `make demo` first to establish a baseline."
        )


def _read_target_ids(conn: duckdb.DuckDBPyConnection) -> _TargetIds:
    """Read mutation target row IDs from DuckDB to correlate with post-ingest results.

    IDs are resolved with the same ORDER BY / NOT LIKE guard used in seed/cdc_smoke.sql
    so the DuckDB-side lookups target exactly the rows that were mutated in Postgres.
    The NOT LIKE 'SMOKE_%' guard ensures pre-existing seed rows are selected regardless
    of how SMOKE_* IDs sort relative to seed IDs.

    Args:
        conn: Open DuckDB connection.

    Returns:
        _TargetIds with one identifier per table mutation target.
    """
    return _TargetIds(
        first_provider_id=_fetchone_str(
            conn,
            "SELECT provider_id FROM raw.providers"
            " WHERE provider_id NOT LIKE 'SMOKE_%' ORDER BY provider_id LIMIT 1",
        ),
        second_provider_id=_fetchone_str(
            conn,
            "SELECT provider_id FROM raw.providers"
            " WHERE provider_id NOT LIKE 'SMOKE_%' ORDER BY provider_id LIMIT 1 OFFSET 1",
        ),
        first_event_id=_fetchone_str(
            conn,
            "SELECT event_id FROM raw.appointments"
            " WHERE event_id NOT LIKE 'SMOKE_%' ORDER BY event_id LIMIT 1",
        ),
        second_event_id=_fetchone_str(
            conn,
            "SELECT event_id FROM raw.appointments"
            " WHERE event_id NOT LIKE 'SMOKE_%' ORDER BY event_id LIMIT 1 OFFSET 1",
        ),
        first_patient_id=_fetchone_str(
            conn,
            "SELECT patient_id FROM raw.patients"
            " WHERE patient_id NOT LIKE 'SMOKE_%' ORDER BY patient_id LIMIT 1",
        ),
        first_consent_patient_id=_fetchone_str(
            conn,
            "SELECT patient_id FROM raw.patient_consents"
            " WHERE patient_id NOT LIKE 'SMOKE_%' AND deleted_ts IS NULL"
            " ORDER BY patient_id LIMIT 1",
        ),
        delete_patient_id=_fetchone_str(
            conn,
            "SELECT patient_id FROM raw.patients"
            " WHERE patient_id NOT LIKE 'SMOKE_%' ORDER BY patient_id LIMIT 1 OFFSET 1",
        ),
        delete_consent_patient_id=_fetchone_str(
            conn,
            "SELECT patient_id FROM raw.patient_consents"
            " WHERE patient_id NOT LIKE 'SMOKE_%' AND deleted_ts IS NULL"
            " ORDER BY patient_id LIMIT 1 OFFSET 1",
        ),
    )


def _apply_mutations(log: logging.LoggerAdapter[logging.Logger]) -> None:
    """Apply cdc_smoke.sql to the source Postgres database via docker compose exec.

    Args:
        log: Run-id-bound logger adapter.
    """
    log.info("mutations_start sql_file=%s", _SMOKE_SQL.name)
    result = subprocess.run(
        [
            "docker", "compose", "exec", "postgres",
            "psql", "-U", "postgres", "-d", "aidn",
            "-f", "/seed/cdc_smoke.sql",
        ],
        capture_output=True,
        text=True,
        cwd=str(_REPO_ROOT),
    )
    if result.returncode != 0:
        log.error(
            "mutations_failed returncode=%d stderr=%s",
            result.returncode,
            result.stderr[:400],
        )
        raise SystemExit(f"psql exited {result.returncode}")
    log.info("mutations_complete sql_file=%s", _SMOKE_SQL.name)


def _run_ingest(log: logging.LoggerAdapter[logging.Logger]) -> None:
    """Run `poetry run aidn ingest` from the repo root.

    Args:
        log: Run-id-bound logger adapter.
    """
    log.info("ingest_start")
    result = subprocess.run(
        ["poetry", "run", "aidn", "ingest"],
        cwd=str(_REPO_ROOT),
    )
    if result.returncode != 0:
        log.error("ingest_failed returncode=%d (see traceback above)", result.returncode)
        raise SystemExit(f"aidn ingest exited {result.returncode}")
    log.info("ingest_complete")


def _resolve_run_id(conn: duckdb.DuckDBPyConnection, prior_load_id: str) -> str:
    """Return the load_id of the newest committed load created after the smoke ingest.

    Args:
        conn: Open DuckDB connection.
        prior_load_id: Load ID that was current before the smoke ingest ran.

    Returns:
        New load ID string, or ``"noop"`` if no new packages were committed.
    """
    row = conn.execute(
        "SELECT load_id FROM raw._dlt_loads WHERE status=0 AND load_id != ?"
        " ORDER BY inserted_at DESC LIMIT 1",
        [prior_load_id],
    ).fetchone()
    if row is None:
        _LOG.warning("smoke_noop reason=no_new_load_after_ingest")
        return "noop"
    return str(row[0])


def _check_providers(
    conn: duckdb.DuckDBPyConnection, ids: _TargetIds
) -> list[MutationResult]:
    """Assert INSERT, UPDATE, and DELETE propagation for raw.providers.

    Args:
        conn: Open DuckDB connection.
        ids: Pre-mutation target row identifiers.

    Returns:
        Three MutationResult entries (INSERT / UPDATE / DELETE).
    """
    ins_n = _fetchone_int(
        conn, "SELECT count(*) FROM raw.providers WHERE provider_id='SMOKE_PRV_INS'"
    )
    specialty = _fetchone_str(
        conn,
        "SELECT specialty FROM raw.providers WHERE provider_id=?",
        [ids.first_provider_id],
    )
    del_n = _fetchone_int(
        conn,
        "SELECT count(*) FROM raw.providers WHERE provider_id=? AND deleted_ts IS NOT NULL",
        [ids.second_provider_id],
    )
    return [
        MutationResult("providers", "INSERT", "count=1", f"count={ins_n}", _TICK if ins_n == 1 else _CROSS),
        MutationResult("providers", "UPDATE", "specialty=cardiology", f"specialty={specialty}", _TICK if specialty == "cardiology" else _CROSS),
        MutationResult("providers", "DELETE", "deleted_ts IS NOT NULL", f"rows_with_deleted_ts={del_n}", _TICK if del_n >= 1 else _CROSS),
    ]


def _check_appointments(
    conn: duckdb.DuckDBPyConnection, ids: _TargetIds
) -> list[MutationResult]:
    """Assert INSERT, UPDATE, and DELETE propagation for raw.appointments.

    Args:
        conn: Open DuckDB connection.
        ids: Pre-mutation target row identifiers.

    Returns:
        Three MutationResult entries (INSERT / UPDATE / DELETE).
    """
    ins_n = _fetchone_int(
        conn, "SELECT count(*) FROM raw.appointments WHERE event_id='SMOKE_APT_INS'"
    )
    status = _fetchone_str(
        conn,
        "SELECT status FROM raw.appointments WHERE event_id=?",
        [ids.first_event_id],
    )
    del_n = _fetchone_int(
        conn,
        "SELECT count(*) FROM raw.appointments WHERE event_id=? AND deleted_ts IS NOT NULL",
        [ids.second_event_id],
    )
    return [
        MutationResult("appointments", "INSERT", "count=1", f"count={ins_n}", _TICK if ins_n == 1 else _CROSS),
        MutationResult("appointments", "UPDATE", "status=completed", f"status={status}", _TICK if status == "completed" else _CROSS),
        MutationResult("appointments", "DELETE", "deleted_ts IS NOT NULL", f"rows_with_deleted_ts={del_n}", _TICK if del_n >= 1 else _CROSS),
    ]


def _check_patients(
    conn: duckdb.DuckDBPyConnection, ids: _TargetIds
) -> list[MutationResult]:
    """Assert INSERT, UPDATE, and DELETE propagation for raw.patients.

    DELETE is asserted as a tombstone row: pg_replication CDC with REPLICA IDENTITY FULL
    emits DELETE WAL events; the resource writes a row with deleted_ts IS NOT NULL.

    Args:
        conn: Open DuckDB connection.
        ids: Pre-mutation target row identifiers.

    Returns:
        Three MutationResult entries (INSERT / UPDATE / DELETE).
    """
    ins_n = _fetchone_int(
        conn, "SELECT count(*) FROM raw.patients WHERE patient_id='SMOKE_PAT_INS'"
    )
    upd_n = _fetchone_int(
        conn,
        "SELECT count(*) FROM raw.patients WHERE patient_id=? AND postcode='0000'",
        [ids.first_patient_id],
    )
    del_n = _fetchone_int(
        conn,
        "SELECT count(*) FROM raw.patients WHERE patient_id=? AND deleted_ts IS NOT NULL",
        [ids.delete_patient_id],
    )
    return [
        MutationResult("patients", "INSERT", "count>=1", f"count={ins_n}", _TICK if ins_n >= 1 else _CROSS),
        MutationResult("patients", "UPDATE", "new SCD2 row postcode=0000", f"rows_postcode_0000={upd_n}", _TICK if upd_n >= 1 else _CROSS),
        MutationResult("patients", "DELETE", "deleted_ts IS NOT NULL", f"rows_with_deleted_ts={del_n}", _TICK if del_n >= 1 else _CROSS),
    ]


def _check_patient_consents(
    conn: duckdb.DuckDBPyConnection, ids: _TargetIds
) -> list[MutationResult]:
    """Assert INSERT, UPDATE, and DELETE CDC propagation for raw.patient_consents.

    CDC event log shape: each WAL event lands as a separate row identified by lsn.
    DELETE events set deleted_ts IS NOT NULL; prior rows are retained (append-only log).
    UPDATE events add a new row with lsn IS NOT NULL alongside the original snapshot row.

    Args:
        conn: Open DuckDB connection.
        ids: Pre-mutation target row identifiers.

    Returns:
        Three MutationResult entries (INSERT / UPDATE / DELETE).
    """
    # INSERT: new row for SMOKE_CNS_INS must be present and not deleted.
    ins_n = _fetchone_int(
        conn,
        "SELECT count(*) FROM raw.patient_consents"
        " WHERE patient_id='SMOKE_CNS_INS' AND deleted_ts IS NULL",
    )
    # UPDATE: a new WAL event row (lsn IS NOT NULL) must exist for the updated patient_id.
    upd_event_rows = conn.execute(
        "SELECT consent_research FROM raw.patient_consents"
        " WHERE patient_id=? AND lsn IS NOT NULL AND deleted_ts IS NULL",
        [ids.first_consent_patient_id],
    ).fetchall()
    upd_ok = len(upd_event_rows) >= 1
    # DELETE: the WAL DELETE event must land as a row with deleted_ts IS NOT NULL.
    del_n = _fetchone_int(
        conn,
        "SELECT count(*) FROM raw.patient_consents"
        " WHERE patient_id=? AND deleted_ts IS NOT NULL",
        [ids.delete_consent_patient_id],
    )
    return [
        MutationResult("patient_consents", "INSERT", "deleted_ts IS NULL count=1", f"count={ins_n}", _TICK if ins_n == 1 else _CROSS),
        MutationResult("patient_consents", "UPDATE", "new event row (lsn IS NOT NULL)", f"new_event_rows={len(upd_event_rows)}", _TICK if upd_ok else _CROSS),
        MutationResult("patient_consents", "DELETE", "deleted_ts IS NOT NULL count>=1", f"rows_with_deleted_ts={del_n}", _TICK if del_n >= 1 else _CROSS),
    ]


def _print_report(results: list[MutationResult], run_id: str) -> None:
    """Print the CDC smoke diff report as a fixed-width table.

    Uses print() — the sole exemption from the no-print() rule — because this
    function produces a human-facing report, not a machine log line.

    Args:
        results: One MutationResult per mutation assertion.
        run_id: Load ID of the smoke ingest run.
    """
    print(f"\n## CDC Smoke Report  run_id={run_id}\n")
    w = (20, 8, 36, 38, 22)
    header = ("table", "mutation", "expected", "observed", "result")
    print("  ".join(h.ljust(w[i]) for i, h in enumerate(header)))
    print("  ".join("-" * wi for wi in w))
    for r in results:
        row = (r.table, r.mutation, r.expected, r.observed, r.result)
        print("  ".join(str(v).ljust(w[i]) for i, v in enumerate(row)))
    failures = [r for r in results if r.result == _CROSS]
    print()
    if failures:
        print(f"FAILED: {len(failures)} assertion(s) did not pass.")
        sys.exit(1)
    print("All assertions passed.")


def main() -> None:
    """Orchestrate the CDC smoke test: baseline → mutations → ingest → verify → report."""
    configure_logging("INFO")
    settings = Settings()  # type: ignore[call-arg]

    with duckdb.connect(str(settings.duckdb_path), read_only=True) as conn:
        prior_load_id = _check_baseline(conn)
        ids = _read_target_ids(conn)
    _LOG.info("smoke_baseline_captured prior_load_id=%s", prior_load_id)

    log: logging.LoggerAdapter[logging.Logger] = bind_run_id(_LOG, prior_load_id)

    _apply_mutations(log)
    _run_ingest(log)

    with duckdb.connect(str(settings.duckdb_path), read_only=True) as conn:
        run_id = _resolve_run_id(conn, prior_load_id)
        log = bind_run_id(_LOG, run_id)
        log.info("smoke_verification_start")

        results: list[MutationResult] = (
            _check_providers(conn, ids)
            + _check_appointments(conn, ids)
            + _check_patients(conn, ids)
            + _check_patient_consents(conn, ids)
        )

    log.info("smoke_verification_complete assertions=%d", len(results))
    _print_report(results, run_id)


if __name__ == "__main__":
    main()
