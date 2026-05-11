"""Integration test — Postgres hard DELETE surfaces as soft-delete row in raw.appointments.

REPLICA IDENTITY FULL on the appointments table ensures the WAL DELETE event
carries the full old row (not just the PK).  The appointments CDC resource is
configured with ``hard_delete=False`` on ``deleted_ts``, so dlt keeps the raw
row and sets ``deleted_ts`` (the deletion timestamp) rather than physically
removing the row from the destination.

Invariant pinned:
  A hard DELETE in the source must appear in ``raw.appointments`` with
  ``deleted_ts IS NOT NULL`` and must not change the total row count.

Prerequisite: ``make up && make seed`` must have run.
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

_logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def replica_identity_settings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Settings:
    """Settings pointing at a temp DuckDB and DLT state directory.

    Postgres credentials are read from the environment and must point at a
    running container with seeded appointments data.
    """
    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "test.duckdb"))
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
    return Settings()  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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
    run_log = bind_run_id(_logger, "test-replica-id-setup")
    run_pipeline(aidn_source(settings), settings=settings, run_logger=run_log)


def _run_ingest(settings: Settings) -> None:
    """Run a full ingest without re-bootstrapping."""
    configure_logging(settings.log_level)
    run_log = bind_run_id(_logger, "test-replica-id-run")
    run_pipeline(aidn_source(settings), settings=settings, run_logger=run_log)


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


def test_appointments_hard_delete_surfaces_as_is_deleted(
    replica_identity_settings: Settings,
) -> None:
    """A Postgres DELETE on appointments must produce a soft-delete row in raw.

    Flow:
    1.  Bootstrap CDC slots and load initial snapshots.
    2.  Run ingest to capture any WAL events since bootstrap.
    3.  Record raw.appointments row count as baseline.
    4.  DELETE the target appointment from Postgres.
    5.  Run ingest again — WAL DELETE event (with full row from REPLICA IDENTITY FULL)
        is captured; dlt merges with ``hard_delete=False``, setting ``deleted_ts``.
    6.  Assert: the target event_id is present with ``deleted_ts IS NOT NULL``.
    7.  Assert: total raw.appointments row count is unchanged (row is updated,
        not removed — merge keeps the row in place).

    Args:
        replica_identity_settings: Isolated settings with temp DuckDB and DLT state dir.
    """
    _bootstrap_and_ingest(replica_identity_settings)

    db_path = str(replica_identity_settings.duckdb_path)
    dsn = str(replica_identity_settings.postgres_url)

    # -- pick a target event_id from the seeded appointments table ------------
    with psycopg2.connect(dsn) as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT event_id FROM appointments ORDER BY event_id LIMIT 1"
            )
            row = cur.fetchone()
    assert row is not None, "seed data missing — run 'make up && make seed' first"
    target_event_id: str = row[0]

    # -- baseline: total row count before delete ------------------------------
    with duckdb.connect(db_path, read_only=True) as conn:
        total_before: int = conn.execute(
            "SELECT count(*) FROM raw.appointments"  # noqa: S608
        ).fetchone()[0]  # type: ignore[index]

        live_before: int = conn.execute(
            "SELECT count(*) FROM raw.appointments "  # noqa: S608
            "WHERE event_id = ? AND deleted_ts IS NULL",
            [target_event_id],
        ).fetchone()[0]  # type: ignore[index]

    assert live_before == 1, (
        f"Expected exactly 1 live row for {target_event_id!r} before delete; "
        f"got {live_before}"
    )

    # -- hard DELETE at source ------------------------------------------------
    with psycopg2.connect(dsn) as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM appointments WHERE event_id = %s", (target_event_id,)
            )

    _run_ingest(replica_identity_settings)

    # -- assert soft-delete in raw --------------------------------------------
    with duckdb.connect(db_path, read_only=True) as conn:
        deleted_row_count: int = conn.execute(
            "SELECT count(*) FROM raw.appointments "  # noqa: S608
            "WHERE event_id = ? AND deleted_ts IS NOT NULL",
            [target_event_id],
        ).fetchone()[0]  # type: ignore[index]

        still_live: int = conn.execute(
            "SELECT count(*) FROM raw.appointments "  # noqa: S608
            "WHERE event_id = ? AND deleted_ts IS NULL",
            [target_event_id],
        ).fetchone()[0]  # type: ignore[index]

        total_after: int = conn.execute(
            "SELECT count(*) FROM raw.appointments"  # noqa: S608
        ).fetchone()[0]  # type: ignore[index]

    assert deleted_row_count == 1, (
        f"Expected 1 soft-deleted row (deleted_ts IS NOT NULL) for "
        f"event_id={target_event_id!r} after source DELETE; got {deleted_row_count}"
    )
    assert still_live == 0, (
        f"Deleted event_id {target_event_id!r} still has {still_live} live rows "
        "(deleted_ts IS NULL) — soft-delete not applied"
    )
    # Row count must be unchanged: the merge updates the existing row in place.
    assert total_after == total_before, (
        f"raw.appointments row count changed after DELETE+ingest: "
        f"before={total_before}, after={total_after}; "
        "expected merge-in-place to leave count unchanged"
    )
