"""Integration tests — pipeline crash-resume correctness after SIGKILL.

Prerequisite: ``make up && make seed`` must have run before executing these tests.

The test kills a pipeline subprocess at an arbitrary point (during startup, mid-load, or
post-completion) and verifies that a subsequent ``run_pipeline`` call produces a correct
final state: row counts match the seed, no duplicate PKs, and exactly one new committed
``_dlt_loads`` entry from the resume run.

Note on timing: the subprocess may be killed at any phase of the run (including after
completion, in which case the kill is a no-op). The invariant under test is
crash-SAFETY not crash-DETECTION: regardless of when the kill occurs, the database must
not be left in a corrupt or duplicated state.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import duckdb
import pytest

from aidn.config import Settings
from aidn.ingest.bootstrap import CDC_TABLES, bootstrap_table
from aidn.ingest.pipeline import aidn_source, make_pipeline, run_pipeline
from aidn.logging_setup import bind_run_id, configure_logging, get_logger

# Time to wait before sending SIGKILL — long enough for the subprocess to start
# Python and begin loading, short enough to catch some runs mid-flight.
KILL_DELAY_SECONDS: float = 0.8

_PROJECT_ROOT = Path(__file__).parents[2]



@pytest.fixture
def crash_settings(
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
    run_log = bind_run_id(get_logger(__name__), "test-resume")
    return run_pipeline(aidn_source(settings), settings=settings, run_logger=run_log)


def _count_dlt_loads(db_path: str) -> int:
    """Return the number of committed (status=0) _dlt_loads packages."""
    with duckdb.connect(db_path, read_only=True) as conn:
        result = conn.execute(
            "SELECT count(*) FROM raw._dlt_loads WHERE status = 0"  # noqa: S608
        ).fetchone()
    assert result is not None
    return int(result[0])



def test_pipeline_resumes_after_kill(crash_settings: Settings) -> None:
    """Database state must be correct after SIGKILL + resume, regardless of kill timing.

    Flow:
    1.  Bootstrap — create CDC slots, load initial snapshots.
    2.  Record post-bootstrap baseline counts.
    3.  Start ingest subprocess via ``sys.executable``; send SIGKILL after
        ``KILL_DELAY_SECONDS`` (may occur during startup, mid-load, or post-completion).
    4.  Run resume ingest in-process.
    5.  Assert providers + appointments counts equal baseline (merge idempotency).
    6.  Assert patient_consents current-row count equals baseline (scd2 correctness).
    7.  Assert patients count did not double (watermark persisted; not a full re-read).
    8.  Assert no duplicate PKs in providers or patient_consents current rows.
    9.  Assert exactly one new committed _dlt_loads entry from the resume run.
    """
    db_path = str(crash_settings.duckdb_path)
    dlt_dir = str(crash_settings.dlt_data_dir)
    pipeline = make_pipeline(crash_settings)

    for slot_name, table_name, primary_key, pub_name in CDC_TABLES:
        snapshot = bootstrap_table(slot_name, table_name, primary_key, pub_name, crash_settings)
        assert snapshot is not None, (
            f"bootstrap_table returned None for {table_name!r} — slot already exists; "
            "run 'make down && make up && make seed' to reset."
        )
        pipeline.run(snapshot)

    _run_ingest(crash_settings)

    with duckdb.connect(db_path, read_only=True) as conn:
        baseline = {
            "providers": conn.execute(
                "SELECT count(*) FROM raw.providers"  # noqa: S608
            ).fetchone()[0],  # type: ignore[index]
            "appointments": conn.execute(
                "SELECT count(*) FROM raw.appointments"  # noqa: S608
            ).fetchone()[0],  # type: ignore[index]
            "patients": conn.execute(
                "SELECT count(*) FROM raw.patients"  # noqa: S608
            ).fetchone()[0],  # type: ignore[index]
            "patient_consents_current": conn.execute(
                "SELECT count(*) FROM raw.patient_consents WHERE _dlt_valid_to IS NULL"  # noqa: S608
            ).fetchone()[0],  # type: ignore[index]
        }
    load_count_before_kill = _count_dlt_loads(db_path)

    # Pass test-specific env vars so the subprocess writes to the isolated tmp_path.
    subprocess_env = {
        **os.environ,
        "DUCKDB_PATH": db_path,
        "DLT_DATA_DIR": dlt_dir,
        "POSTGRES_URL": str(crash_settings.postgres_url),
        "POSTGRES_REPL_URL": str(crash_settings.postgres_repl_url),
    }
    proc = subprocess.Popen(
        [sys.executable, "-c", "from aidn.cli import main; main(['ingest'])"],
        env=subprocess_env,
        cwd=str(_PROJECT_ROOT),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(KILL_DELAY_SECONDS)
    proc.kill()
    proc.wait()

    # The kill may have landed pre-commit, mid-commit, or post-completion; all are valid.
    load_count_after_kill = _count_dlt_loads(db_path)

    _run_ingest(crash_settings)
    load_count_after_resume = _count_dlt_loads(db_path)

    with duckdb.connect(db_path, read_only=True) as conn:
        providers_count = conn.execute(
            "SELECT count(*) FROM raw.providers"  # noqa: S608
        ).fetchone()[0]  # type: ignore[index]
        appointments_count = conn.execute(
            "SELECT count(*) FROM raw.appointments"  # noqa: S608
        ).fetchone()[0]  # type: ignore[index]
        patients_count = conn.execute(
            "SELECT count(*) FROM raw.patients"  # noqa: S608
        ).fetchone()[0]  # type: ignore[index]
        consents_current = conn.execute(
            "SELECT count(*) FROM raw.patient_consents WHERE _dlt_valid_to IS NULL"  # noqa: S608
        ).fetchone()[0]  # type: ignore[index]

        # merge tables — strictly idempotent
        assert providers_count == baseline["providers"], (
            f"raw.providers changed after crash+resume: "
            f"{baseline['providers']} → {providers_count}"
        )
        assert appointments_count == baseline["appointments"], (
            f"raw.appointments changed after crash+resume: "
            f"{baseline['appointments']} → {appointments_count}"
        )

        # scd2 — current-row count stable
        assert consents_current == baseline["patient_consents_current"], (
            f"raw.patient_consents current rows changed: "
            f"{baseline['patient_consents_current']} → {consents_current}"
        )

        # append+lag — may grow slightly; must not double (watermark reset would double it)
        assert patients_count >= baseline["patients"], (
            f"raw.patients shrank after crash+resume: "
            f"{baseline['patients']} → {patients_count}"
        )
        assert patients_count < baseline["patients"] * 2, (
            f"raw.patients nearly doubled ({baseline['patients']} → {patients_count}), "
            "indicating the watermark was not persisted across the crash."
        )

        providers_dups = conn.execute(
            "SELECT count(*) FROM ("
            "  SELECT provider_id FROM raw.providers"
            "  GROUP BY provider_id HAVING count(*) > 1"
            ")"
        ).fetchone()
        assert providers_dups is not None
        assert providers_dups[0] == 0, (
            f"Duplicate provider_id rows after crash+resume: {providers_dups[0]}"
        )

        consent_dups = conn.execute(
            "SELECT count(*) FROM ("
            "  SELECT patient_id FROM raw.patient_consents"
            "  WHERE _dlt_valid_to IS NULL"
            "  GROUP BY patient_id HAVING count(*) > 1"
            ")"
        ).fetchone()
        assert consent_dups is not None
        assert consent_dups[0] == 0, (
            f"Duplicate current patient_consents rows after crash+resume: "
            f"{consent_dups[0]}"
        )

    # The resume run always commits one package (dlt commits even for empty loads).
    # This confirms the resume ran to completion without multi-package anomaly.
    resume_packages = load_count_after_resume - load_count_after_kill
    assert resume_packages == 1, (
        f"Expected exactly 1 new _dlt_loads entry from resume, got {resume_packages} "
        f"(kill count: {load_count_after_kill}, resume count: {load_count_after_resume})"
    )
