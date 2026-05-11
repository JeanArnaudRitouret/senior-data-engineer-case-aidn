"""Integration tests — schema_contract: freeze blocks unexpected source columns (item 1.24).

Each parametrize case adds a novel column to one Postgres table, runs the pipeline,
and asserts the correct rejection behaviour for that table's ingestion mechanism.

Rejection mechanism by table type:

pg_replication (providers, appointments)
  After ALTER TABLE, a WAL INSERT generates a RELATION message that carries the new
  schema; dlt detects the column during normalization and raises PipelineStepFailed
  wrapping SchemaEvolutionException. The exception chain names the unexpected column.

sql_table full-SELECT (patient_consents)
  dlt reflects the full schema from Postgres via SQLAlchemy; the new column appears
  before any rows are fetched; SchemaEvolutionException is raised during normalization.

sql_table incremental (patients)
  dlt's normalization never sees the extra column because the Pydantic two-tier
  validator (extra="forbid") drops each row as Tier-1 ValidationError BEFORE the
  row reaches the normalizer.  The pipeline completes without raising.  The effective
  schema guard here is Pydantic, not dlt's schema_contract.  The invariant still holds:
  no row with the unexpected column lands in raw.patients.

  This is a documented behaviour gap: schema_contract={"columns": "freeze"} does NOT
  raise PipelineStepFailed for incremental sql_table when add_map drops all rows first.
  The assertion for patients is therefore different: assert ≥ 1 row_dropped WARNING
  is emitted for the patients table.

Prerequisite: ``make up && make seed`` must have run.
"""

from __future__ import annotations

import logging
from pathlib import Path

import psycopg2
import pytest
from dlt.pipeline.exceptions import PipelineStepFailed

from aidn.config import Settings
from aidn.ingest.bootstrap import CDC_TABLES, bootstrap_table
from aidn.ingest.pipeline import aidn_source, make_pipeline, run_pipeline
from aidn.logging_setup import bind_run_id, configure_logging, get_logger

_logger = get_logger(__name__)

# Deliberately generic name to avoid collision with any future source column.
_EXTRA_COL = "unexpected_schema_freeze_col"

# Tables that use pg_replication CDC — need a WAL event after ALTER TABLE to
# surface the new column in a RELATION message dlt can observe.
_CDC_TABLES: frozenset[str] = frozenset({"providers", "appointments"})

# Tables whose ingestion mechanism raises PipelineStepFailed on schema evolution.
# patients is excluded: its incremental+add_map path drops rows before normalization,
# so PipelineStepFailed is never raised (see module docstring for details).
_RAISES_TABLES: frozenset[str] = frozenset({"providers", "appointments", "patient_consents"})


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def freeze_settings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Settings:
    """Isolated settings pointing at a temp DuckDB and DLT state directory.

    Provides isolated destination storage so tests do not touch ``aidn.duckdb``
    or the project-level ``.dlt/`` state.  Postgres credentials are read from
    the environment.
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
    run_log = bind_run_id(_logger, "test-freeze-setup")
    run_pipeline(aidn_source(settings), settings=settings, run_logger=run_log)


def _inject_walevent_for_cdc_table(table: str, dsn: str) -> None:
    """UPDATE an existing row to generate a WAL RELATION + UPDATE event.

    An UPDATE after ALTER TABLE causes Postgres to emit a RELATION message that
    carries the updated schema (including the new column), allowing dlt to detect
    the schema change on the next ingest run.  UPDATE is preferred over INSERT to
    avoid UniqueViolation errors when the container is reused across test runs.

    Args:
        table: Name of the Postgres source table (``providers`` or ``appointments``).
        dsn: Postgres connection string.
    """
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            if table == "providers":
                # Touch one existing row — SET name = name is a no-op value-wise
                # but always emits a WAL UPDATE + RELATION record.
                cur.execute(
                    "UPDATE providers SET name = name "  # noqa: S608
                    "WHERE provider_id = ("
                    "  SELECT provider_id FROM providers ORDER BY provider_id LIMIT 1"
                    ")"
                )
            else:
                # appointments has no PK constraint; touch one existing event.
                cur.execute(
                    "UPDATE appointments SET status = status "  # noqa: S608
                    "WHERE event_id = ("
                    "  SELECT event_id FROM appointments ORDER BY event_id LIMIT 1"
                    ")"
                )


def _exc_chain_str(exc: BaseException) -> str:
    """Concatenate string representations of the full exception chain.

    Args:
        exc: Root exception to traverse.

    Returns:
        Single string with each exception's str() joined by a space.
    """
    parts: list[str] = []
    current: BaseException | None = exc
    while current is not None:
        parts.append(str(current))
        cause = current.__cause__
        ctx = current.__context__
        current = cause if cause is not None else ctx
    return " ".join(parts)


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "table",
    ["providers", "appointments", "patients", "patient_consents"],
)
def test_schema_contract_freeze_rejects_unexpected_column(
    table: str,
    freeze_settings: Settings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Injecting an unexpected column must be caught before landing in raw.*.

    For three of four tables the rejection is PipelineStepFailed (hard block).
    For ``patients`` (incremental sql_table) the Pydantic two-tier validator is
    the effective guard: rows are dropped with a WARNING before normalization.
    See module docstring for the full explanation.

    Args:
        table: Source table name to modify with an extra column.
        freeze_settings: Isolated settings with temp DuckDB and DLT state dir.
        caplog: pytest fixture; used to capture row_dropped warnings for patients.
    """
    _bootstrap_and_ingest(freeze_settings)

    dsn = str(freeze_settings.postgres_url)

    # Add the unexpected column to the Postgres source table.
    with psycopg2.connect(dsn) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                f"ALTER TABLE {table} ADD COLUMN {_EXTRA_COL} TEXT DEFAULT 'extra'"  # noqa: S608
            )

    try:
        # pg_replication discovers schema from WAL RELATION messages, which are sent
        # before each DML event on the table.  Emit one INSERT so the RELATION
        # message (containing the new column) appears in the WAL stream.
        if table in _CDC_TABLES:
            _inject_walevent_for_cdc_table(table, dsn)

        configure_logging(freeze_settings.log_level)
        run_log = bind_run_id(_logger, "test-freeze-run")

        if table in _RAISES_TABLES:
            # Hard block: PipelineStepFailed wrapping SchemaEvolutionException.
            with pytest.raises(PipelineStepFailed) as exc_info:
                run_pipeline(
                    aidn_source(freeze_settings),
                    settings=freeze_settings,
                    run_logger=run_log,
                )
            chain = _exc_chain_str(exc_info.value)
            assert _EXTRA_COL in chain, (
                f"Expected column name {_EXTRA_COL!r} in exception chain for {table!r}; "
                f"got (truncated): {chain[:800]!r}"
            )
        else:
            # patients (incremental sql_table): Pydantic Tier-1 acts as guard.
            # Pipeline completes; rows with the extra column are dropped with WARNINGs.
            with caplog.at_level(logging.WARNING):
                run_pipeline(
                    aidn_source(freeze_settings),
                    settings=freeze_settings,
                    run_logger=run_log,
                )
            dropped = [
                r for r in caplog.records
                if "row_dropped" in r.getMessage() and "table=patients" in r.getMessage()
            ]
            assert len(dropped) > 0, (
                f"Expected ≥ 1 row_dropped WARNING for {table!r} with extra column; "
                "got none — the extra column may have silently slipped through"
            )

    finally:
        # Always drop the extra column so the shared Postgres container is clean
        # for the next parametrize case or the next test run.
        with psycopg2.connect(dsn) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    f"ALTER TABLE {table} DROP COLUMN IF EXISTS {_EXTRA_COL}"  # noqa: S608
                )
