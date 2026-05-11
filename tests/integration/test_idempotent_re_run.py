"""Integration tests — pipeline idempotency on second run with no source changes.

Prerequisite: ``make up && make seed`` must have run before executing these tests.
The tests create real replication slots against the Postgres container and write
to a temporary DuckDB file; they do NOT touch ``aidn.duckdb``.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from aidn.config import Settings
from aidn.ingest.bootstrap import CDC_TABLES, bootstrap_table
from aidn.ingest.pipeline import aidn_source, make_pipeline, run_pipeline
from aidn.logging_setup import bind_run_id, configure_logging, get_logger

logger = get_logger(__name__)



@pytest.fixture
def idempotent_settings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Settings:
    """Settings pointing at a temp DuckDB and DLT state directory.

    Provides isolated destination storage so tests do not touch ``aidn.duckdb``
    or the project-level ``.dlt/`` state.  Postgres credentials are read from
    the environment and must point at a running container.
    """
    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "test.duckdb"))
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
    return Settings()  # type: ignore[call-arg]



def _run_ingest(settings: Settings) -> str:
    """Run the full aidn_source through the pipeline; return the run_id (or empty)."""
    configure_logging(settings.log_level)
    run_log = bind_run_id(get_logger(__name__), "test-run")
    return run_pipeline(aidn_source(settings), settings=settings, run_logger=run_log)


def _row_counts(db_path: str) -> dict[str, int]:
    """Return row counts for all four raw tables plus total _dlt_loads packages."""
    with duckdb.connect(db_path, read_only=True) as conn:
        return {
            "providers": conn.execute(
                "SELECT count(*) FROM raw.providers"  # noqa: S608
            ).fetchone()[0],  # type: ignore[index]
            "patients": conn.execute(
                "SELECT count(*) FROM raw.patients"  # noqa: S608
            ).fetchone()[0],  # type: ignore[index]
            "appointments": conn.execute(
                "SELECT count(*) FROM raw.appointments"  # noqa: S608
            ).fetchone()[0],  # type: ignore[index]
            "patient_consents_current": conn.execute(
                "SELECT count(*) FROM raw.patient_consents WHERE _dlt_valid_to IS NULL"  # noqa: S608
            ).fetchone()[0],  # type: ignore[index]
            "dlt_loads": conn.execute(
                "SELECT count(*) FROM raw._dlt_loads WHERE status = 0"  # noqa: S608
            ).fetchone()[0],  # type: ignore[index]
        }



def test_pipeline_idempotent_second_run(idempotent_settings: Settings) -> None:
    """Second ingest run with no source changes must not alter raw.* row counts.

    Flow:
    1.  Bootstrap — create CDC slots, load initial snapshots.
    2.  Ingest run 1 — captures any WAL events since bootstrap (expected: none).
    3.  Record raw.* row counts and _dlt_loads count.
    4.  Ingest run 2 — no source changes; must not produce new rows in any table.
    5.  Assert row counts identical to step 3.
    6.  Assert no duplicate PKs in providers (merge) or patient_consents (scd2).
    7.  Bootstrap run 2 — idempotency guard must fire; 0 new _dlt_loads rows.
    """
    db_path = str(idempotent_settings.duckdb_path)
    pipeline = make_pipeline(idempotent_settings)

    for slot_name, table_name, primary_key, pub_name in CDC_TABLES:
        snapshot = bootstrap_table(slot_name, table_name, primary_key, pub_name, idempotent_settings)
        assert snapshot is not None, (
            f"bootstrap_table returned None for {table_name!r} on first call — "
            "slot already exists; run 'make down && make up && make seed' to reset."
        )
        pipeline.run(snapshot)

    _run_ingest(idempotent_settings)

    counts_after_run1 = _row_counts(db_path)

    _run_ingest(idempotent_settings)

    counts_after_run2 = _row_counts(db_path)

    # merge tables — strict row-count idempotency (delete-insert by PK).
    assert counts_after_run2["providers"] == counts_after_run1["providers"], (
        f"raw.providers grew on second run: "
        f"{counts_after_run1['providers']} → {counts_after_run2['providers']}"
    )
    assert counts_after_run2["appointments"] == counts_after_run1["appointments"], (
        f"raw.appointments grew on second run: "
        f"{counts_after_run1['appointments']} → {counts_after_run2['appointments']}"
    )

    # scd2 table — current-row count must not change (no new _dlt_valid_to mutations).
    assert (
        counts_after_run2["patient_consents_current"]
        == counts_after_run1["patient_consents_current"]
    ), (
        f"raw.patient_consents current rows changed on second run: "
        f"{counts_after_run1['patient_consents_current']} → "
        f"{counts_after_run2['patient_consents_current']}"
    )

    # append + incremental table — at-least-once semantics with lag=5min.
    # The lag window may re-read rows whose updated_at falls within 5 minutes of the
    # watermark; the count may grow slightly but must NOT approach a full re-read
    # (which would indicate a watermark reset bug).
    patients_run1 = counts_after_run1["patients"]
    patients_run2 = counts_after_run2["patients"]
    assert patients_run2 >= patients_run1, (
        f"raw.patients shrank on second run: {patients_run1} → {patients_run2}"
    )
    assert patients_run2 < patients_run1 * 2, (
        f"raw.patients nearly doubled on second run ({patients_run1} → {patients_run2}), "
        "indicating a watermark reset; incremental state was not persisted."
    )

    with duckdb.connect(db_path, read_only=True) as conn:
        providers_dups = conn.execute(
            "SELECT count(*) FROM ("
            "  SELECT provider_id FROM raw.providers"
            "  GROUP BY provider_id HAVING count(*) > 1"
            ")"
        ).fetchone()
        assert providers_dups is not None
        assert providers_dups[0] == 0, (
            f"Duplicate provider_id rows found after second run: {providers_dups[0]}"
        )

        # SCD2: only current rows (valid_to IS NULL) should be unique per patient_id.
        consent_dups = conn.execute(
            "SELECT count(*) FROM ("
            "  SELECT patient_id FROM raw.patient_consents"
            "  WHERE _dlt_valid_to IS NULL"
            "  GROUP BY patient_id HAVING count(*) > 1"
            ")"
        ).fetchone()
        assert consent_dups is not None
        assert consent_dups[0] == 0, (
            f"Duplicate current patient_consents rows found after second run: "
            f"{consent_dups[0]}"
        )

    loads_before_second_bootstrap = counts_after_run2["dlt_loads"]

    for slot_name, table_name, primary_key, pub_name in CDC_TABLES:
        result = bootstrap_table(slot_name, table_name, primary_key, pub_name, idempotent_settings)
        assert result is None, (
            f"bootstrap_table returned a resource for {table_name!r} on second call — "
            f"idempotency guard did not fire for slot {slot_name!r}."
        )

    with duckdb.connect(db_path, read_only=True) as conn:
        loads_after = conn.execute(
            "SELECT count(*) FROM raw._dlt_loads WHERE status = 0"  # noqa: S608
        ).fetchone()
    assert loads_after is not None
    assert loads_after[0] == loads_before_second_bootstrap, (
        f"Second bootstrap created new _dlt_loads rows: "
        f"before={loads_before_second_bootstrap}, after={loads_after[0]}"
    )
