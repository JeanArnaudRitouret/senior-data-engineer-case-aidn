"""Integration tests — patient_consents full-snapshot SCD2 (no merge_key) invariants.

Tests 1–2 verify SCD2 correctness against a live Postgres container.
Test 3 covers the snapshot-truncation guard (deferred; marked xfail).

Prerequisite: ``make up && make seed`` must have run before executing these tests.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import psycopg2
import pytest

from aidn.config import Settings
from aidn.ingest.bootstrap import CDC_TABLES, bootstrap_table
from aidn.ingest.pipeline import aidn_source, make_pipeline, run_pipeline
from aidn.logging_setup import bind_run_id, configure_logging, get_logger

logger = get_logger(__name__)



@pytest.fixture
def scd2_settings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Settings:
    """Settings pointing at a temp DuckDB and DLT state directory.

    Postgres credentials are read from the environment and must point at a
    running container with seeded data.
    """
    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "test.duckdb"))
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
    return Settings()  # type: ignore[call-arg]



def _bootstrap_and_ingest(settings: Settings) -> None:
    """Bootstrap CDC slots, load snapshots, then run a full ingest."""
    pipeline = make_pipeline(settings)
    for slot_name, table_name, primary_key, pub_name in CDC_TABLES:
        snapshot = bootstrap_table(slot_name, table_name, primary_key, pub_name, settings)
        assert snapshot is not None, (
            f"bootstrap_table returned None for {table_name!r} — "
            "slot already exists; conftest drop_cdc_slots should have cleaned it."
        )
        pipeline.run(snapshot)
    configure_logging(settings.log_level)
    run_log = bind_run_id(get_logger(__name__), "test-scd2-setup")
    run_pipeline(aidn_source(settings), settings=settings, run_logger=run_log)


def _run_ingest(settings: Settings) -> None:
    """Run a full ingest without re-bootstrapping."""
    configure_logging(settings.log_level)
    run_log = bind_run_id(get_logger(__name__), "test-scd2-run")
    run_pipeline(aidn_source(settings), settings=settings, run_logger=run_log)



def test_raw_patient_consents_scd2_retires_deleted_row(
    scd2_settings: Settings,
) -> None:
    """Deleting a consent row in Postgres must close exactly one raw row.

    Full-snapshot SCD2 (no merge_key): when ``patient_id`` is absent from the next
    full snapshot, dlt sets ``_dlt_valid_to`` on the existing raw row; all other
    rows remain current (``_dlt_valid_to IS NULL``).

    Args:
        scd2_settings: Isolated settings with temp DuckDB and DLT state dir.
    """
    _bootstrap_and_ingest(scd2_settings)

    db_path = str(scd2_settings.duckdb_path)
    dsn = str(scd2_settings.postgres_url)

    with psycopg2.connect(dsn) as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT patient_id FROM patient_consents ORDER BY patient_id LIMIT 1"
            )
            row = cur.fetchone()
    assert row is not None, "seed data missing — run 'make up && make seed' first"
    target_pid: str = row[0]

    with duckdb.connect(db_path, read_only=True) as conn:
        total_current_before: int = conn.execute(
            "SELECT count(*) FROM raw.patient_consents WHERE _dlt_valid_to IS NULL"  # noqa: S608
        ).fetchone()[0]  # type: ignore[index]
        target_current_before: int = conn.execute(
            "SELECT count(*) FROM raw.patient_consents "  # noqa: S608
            "WHERE patient_id = ? AND _dlt_valid_to IS NULL",
            [target_pid],
        ).fetchone()[0]  # type: ignore[index]

    assert target_current_before == 1, (
        f"Expected 1 current row for {target_pid!r} before delete; "
        f"got {target_current_before}"
    )

    with psycopg2.connect(dsn) as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM patient_consents WHERE patient_id = %s", (target_pid,)
            )

    _run_ingest(scd2_settings)

    with duckdb.connect(db_path, read_only=True) as conn:
        retired: int = conn.execute(
            "SELECT count(*) FROM raw.patient_consents "  # noqa: S608
            "WHERE patient_id = ? AND _dlt_valid_to IS NOT NULL",
            [target_pid],
        ).fetchone()[0]  # type: ignore[index]
        still_current: int = conn.execute(
            "SELECT count(*) FROM raw.patient_consents "  # noqa: S608
            "WHERE patient_id = ? AND _dlt_valid_to IS NULL",
            [target_pid],
        ).fetchone()[0]  # type: ignore[index]
        total_current_after: int = conn.execute(
            "SELECT count(*) FROM raw.patient_consents WHERE _dlt_valid_to IS NULL"  # noqa: S608
        ).fetchone()[0]  # type: ignore[index]

    assert retired == 1, (
        f"Expected 1 retired row for deleted patient_id {target_pid!r}; got {retired}"
    )
    assert still_current == 0, (
        f"Deleted patient_id {target_pid!r} still has {still_current} current row(s) "
        "after delete + ingest"
    )
    assert total_current_after == total_current_before - 1, (
        f"Total current rows should have decreased by 1 after one delete; "
        f"before={total_current_before}, after={total_current_after}"
    )


def test_raw_patient_consents_scd2_history_preserved_on_consent_flip(
    scd2_settings: Settings,
) -> None:
    """Flipping a consent flag must close the old row and open a new current row.

    Full-snapshot SCD2 (no merge_key): content change on an existing ``patient_id``
    → dlt sets ``_dlt_valid_to`` on the prior row and inserts a new row
    reflecting the updated consent value.

    Args:
        scd2_settings: Isolated settings with temp DuckDB and DLT state dir.
    """
    _bootstrap_and_ingest(scd2_settings)

    db_path = str(scd2_settings.duckdb_path)
    dsn = str(scd2_settings.postgres_url)

    with psycopg2.connect(dsn) as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT patient_id, consent_research FROM patient_consents "
                "ORDER BY patient_id LIMIT 1"
            )
            row = cur.fetchone()
    assert row is not None, "seed data missing"
    target_pid: str = row[0]
    original_value: bool = bool(row[1])

    with psycopg2.connect(dsn) as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(
                "UPDATE patient_consents "
                "SET consent_research = NOT consent_research "
                "WHERE patient_id = %s",
                (target_pid,),
            )

    _run_ingest(scd2_settings)

    with duckdb.connect(db_path, read_only=True) as conn:
        closed: int = conn.execute(
            "SELECT count(*) FROM raw.patient_consents "  # noqa: S608
            "WHERE patient_id = ? AND _dlt_valid_to IS NOT NULL",
            [target_pid],
        ).fetchone()[0]  # type: ignore[index]
        current_rows = conn.execute(
            "SELECT consent_research FROM raw.patient_consents "  # noqa: S608
            "WHERE patient_id = ? AND _dlt_valid_to IS NULL",
            [target_pid],
        ).fetchall()

    assert closed == 1, (
        f"Expected 1 closed row for {target_pid!r} after consent flip; got {closed}"
    )
    assert len(current_rows) == 1, (
        f"Expected exactly 1 current row for {target_pid!r}; got {len(current_rows)}"
    )
    new_value: bool = bool(current_rows[0][0])
    assert new_value != original_value, (
        f"consent_research for {target_pid!r} is still {new_value!r}; "
        f"flag flip (was {original_value!r}) was not captured in raw"
    )


@pytest.mark.xfail(
    reason="Snapshot-truncation guard not yet implemented; remove this mark when the guard is added.",
    strict=False,
)
def test_raw_patient_consents_full_snapshot_guard_aborts_on_truncation(
    scd2_settings: Settings,
) -> None:
    """Snapshot guard must raise RuntimeError before any _dlt_valid_to mutation.

    Mocks ``SELECT count(*)`` to return 0 when ``last_count=50``, simulating a
    partial read that would otherwise cause phantom ``_dlt_valid_to`` entries
    if not aborted.

    Remove the ``xfail`` mark and implement the body when the snapshot-truncation guard is implemented.

    Args:
        scd2_settings: Isolated settings with temp DuckDB and DLT state dir.
    """
    raise NotImplementedError("1.19 not yet implemented")
