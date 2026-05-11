"""Integration tests — patient_consents CDC event log invariants.

Tests verify that pg_replication CDC produces an append-only WAL event log:
DELETE events land as rows with ``deleted_ts IS NOT NULL``; UPDATE events add
a new row with a non-NULL ``lsn``; re-running without source changes is idempotent.

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
def cdc_settings(
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
    run_log = bind_run_id(get_logger(__name__), "test-cdc-setup")
    run_pipeline(aidn_source(settings), settings=settings, run_logger=run_log)


def _run_ingest(settings: Settings) -> None:
    """Run a full ingest without re-bootstrapping."""
    configure_logging(settings.log_level)
    run_log = bind_run_id(get_logger(__name__), "test-cdc-run")
    run_pipeline(aidn_source(settings), settings=settings, run_logger=run_log)


def test_raw_patient_consents_cdc_captures_delete_event(
    cdc_settings: Settings,
) -> None:
    """Deleting a consent row in Postgres must produce a WAL event row with deleted_ts set.

    CDC event log shape: the deleted patient_id retains prior rows with
    ``deleted_ts IS NULL`` (history preserved) and gains a new row with
    ``deleted_ts IS NOT NULL`` (DELETE event).

    Args:
        cdc_settings: Isolated settings with temp DuckDB and DLT state dir.
    """
    _bootstrap_and_ingest(cdc_settings)

    db_path = str(cdc_settings.duckdb_path)
    dsn = str(cdc_settings.postgres_url)

    with psycopg2.connect(dsn) as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT patient_id FROM patient_consents ORDER BY patient_id LIMIT 1"
            )
            row = cur.fetchone()
    assert row is not None, "seed data missing — run 'make up && make seed' first"
    target_pid: str = row[0]

    with psycopg2.connect(dsn) as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM patient_consents WHERE patient_id = %s", (target_pid,)
            )

    _run_ingest(cdc_settings)

    with duckdb.connect(db_path, read_only=True) as conn:
        deleted_event_count: int = conn.execute(
            "SELECT count(*) FROM raw.patient_consents "  # noqa: S608
            "WHERE patient_id = ? AND deleted_ts IS NOT NULL",
            [target_pid],
        ).fetchone()[0]  # type: ignore[index]
        prior_rows_count: int = conn.execute(
            "SELECT count(*) FROM raw.patient_consents "  # noqa: S608
            "WHERE patient_id = ? AND deleted_ts IS NULL",
            [target_pid],
        ).fetchone()[0]  # type: ignore[index]

    assert deleted_event_count >= 1, (
        f"Expected ≥1 DELETE event row for {target_pid!r} with deleted_ts IS NOT NULL; "
        f"got {deleted_event_count}"
    )
    assert prior_rows_count >= 1, (
        f"Expected prior rows (deleted_ts IS NULL) to be preserved for {target_pid!r}; "
        f"got {prior_rows_count} — event log must be append-only"
    )


def test_raw_patient_consents_cdc_captures_update_event(
    cdc_settings: Settings,
) -> None:
    """Updating a consent flag must add a new WAL event row with higher lsn.

    CDC event log shape: the original row (from bootstrap snapshot; lsn=NULL)
    is preserved and a new row with a non-NULL lsn and the updated consent value
    is appended.

    Args:
        cdc_settings: Isolated settings with temp DuckDB and DLT state dir.
    """
    _bootstrap_and_ingest(cdc_settings)

    db_path = str(cdc_settings.duckdb_path)
    dsn = str(cdc_settings.postgres_url)

    with psycopg2.connect(dsn) as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT patient_id, consent_research FROM patient_consents "
                "WHERE consent_research IS NOT NULL ORDER BY patient_id LIMIT 1"
            )
            row = cur.fetchone()
    assert row is not None, "seed data missing"
    target_pid: str = row[0]
    original_value: bool = bool(row[1])

    with duckdb.connect(db_path, read_only=True) as conn:
        rows_before: int = conn.execute(
            "SELECT count(*) FROM raw.patient_consents WHERE patient_id = ?",  # noqa: S608
            [target_pid],
        ).fetchone()[0]  # type: ignore[index]

    with psycopg2.connect(dsn) as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(
                "UPDATE patient_consents "
                "SET consent_research = NOT consent_research "
                "WHERE patient_id = %s",
                (target_pid,),
            )

    _run_ingest(cdc_settings)

    with duckdb.connect(db_path, read_only=True) as conn:
        rows_after: int = conn.execute(
            "SELECT count(*) FROM raw.patient_consents WHERE patient_id = ?",  # noqa: S608
            [target_pid],
        ).fetchone()[0]  # type: ignore[index]
        new_event_rows = conn.execute(
            "SELECT consent_research FROM raw.patient_consents "  # noqa: S608
            "WHERE patient_id = ? AND lsn IS NOT NULL AND deleted_ts IS NULL",
            [target_pid],
        ).fetchall()

    assert rows_after == rows_before + 1, (
        f"Expected one new row appended for {target_pid!r} after UPDATE; "
        f"before={rows_before}, after={rows_after}"
    )
    assert len(new_event_rows) == 1, (
        f"Expected exactly 1 non-null lsn UPDATE-event row for {target_pid!r}; "
        f"got {len(new_event_rows)}"
    )
    new_value: bool = bool(new_event_rows[0][0])
    assert new_value != original_value, (
        f"consent_research for {target_pid!r} is still {new_value!r} on the new event row; "
        f"flag flip (was {original_value!r}) was not captured"
    )


def test_raw_patient_consents_cdc_event_log_append_only(
    cdc_settings: Settings,
) -> None:
    """Two consecutive ingests with no source changes must not grow the event log.

    Merge on ``lsn`` is idempotent — replaying the same WAL events produces no
    phantom rows.

    Args:
        cdc_settings: Isolated settings with temp DuckDB and DLT state dir.
    """
    _bootstrap_and_ingest(cdc_settings)

    db_path = str(cdc_settings.duckdb_path)

    with duckdb.connect(db_path, read_only=True) as conn:
        count_after_first: int = conn.execute(
            "SELECT count(*) FROM raw.patient_consents"  # noqa: S608
        ).fetchone()[0]  # type: ignore[index]

    _run_ingest(cdc_settings)

    with duckdb.connect(db_path, read_only=True) as conn:
        count_after_second: int = conn.execute(
            "SELECT count(*) FROM raw.patient_consents"  # noqa: S608
        ).fetchone()[0]  # type: ignore[index]

    assert count_after_second == count_after_first, (
        f"Row count grew from {count_after_first} to {count_after_second} with no source "
        "changes — merge on lsn is not idempotent"
    )
