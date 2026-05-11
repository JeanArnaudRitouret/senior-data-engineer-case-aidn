"""Integration tests — initial-snapshot bootstrap visibility and idempotency.

Prerequisite: ``make up && make seed`` must have run before executing these tests.
The tests create real replication slots against the Postgres container and write
snapshot data to a temporary DuckDB file; they do NOT touch ``aidn.duckdb``.
"""

from __future__ import annotations

import csv
from pathlib import Path

import duckdb
import pytest

from aidn.config import Settings
from aidn.ingest.bootstrap import CDC_TABLES, bootstrap_table
from aidn.ingest.pipeline import make_pipeline

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SEED_DIR = Path(__file__).parents[2] / "seed"


def _csv_row_count(path: Path) -> int:
    """Return the number of data rows in a CSV file (excluding the header).

    Args:
        path: Absolute path to the CSV file.

    Returns:
        Row count excluding the header line.
    """
    with path.open(newline="") as fh:
        return sum(1 for _ in csv.reader(fh)) - 1


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def bootstrap_settings(
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


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_seeded_rows_visible_after_bootstrap(
    bootstrap_settings: Settings,
) -> None:
    """Seed CSV rows must be present in raw.* after bootstrap runs."""
    expected: dict[str, int] = {
        "providers": _csv_row_count(_SEED_DIR / "providers.csv"),
        "appointments": _csv_row_count(_SEED_DIR / "appointments.csv"),
    }

    pipeline = make_pipeline(bootstrap_settings)
    load_count = 0

    for slot_name, table_name in CDC_TABLES:
        snapshot = bootstrap_table(slot_name, table_name, bootstrap_settings)
        assert snapshot is not None, (
            f"bootstrap_table returned None for {table_name!r} — slot already exists; "
            "run 'make down && make up && make seed' to reset the container."
        )
        info = pipeline.run(snapshot)
        ids = info.loads_ids
        assert len(ids) == 1, (
            f"Expected 1 load package for {table_name!r}, got {len(ids)}: {ids}"
        )
        load_count += 1

    assert load_count == 2

    db_path = str(bootstrap_settings.duckdb_path)
    with duckdb.connect(db_path, read_only=True) as conn:
        for table_name, expected_count in expected.items():
            actual = conn.execute(
                f"SELECT count(*) FROM raw.{table_name}"  # noqa: S608
            ).fetchone()
            assert actual is not None
            assert actual[0] == expected_count, (
                f"raw.{table_name}: expected {expected_count} rows, got {actual[0]}"
            )

        load_rows = conn.execute(
            "SELECT count(*) FROM raw._dlt_loads WHERE status='loaded'"
        ).fetchone()
        assert load_rows is not None
        assert load_rows[0] == 2, (
            f"Expected 2 committed _dlt_loads packages, got {load_rows[0]}"
        )


def test_bootstrap_idempotent(bootstrap_settings: Settings) -> None:
    """Second bootstrap call must no-op: no new _dlt_loads rows created."""
    pipeline = make_pipeline(bootstrap_settings)

    # First pass: create slots and load snapshots.
    for slot_name, table_name in CDC_TABLES:
        snapshot = bootstrap_table(slot_name, table_name, bootstrap_settings)
        if snapshot is not None:
            pipeline.run(snapshot)

    db_path = str(bootstrap_settings.duckdb_path)
    with duckdb.connect(db_path, read_only=True) as conn:
        before = conn.execute(
            "SELECT count(*) FROM raw._dlt_loads"
        ).fetchone()
    assert before is not None
    count_before: int = before[0]

    # Second pass: all slots exist; bootstrap_table must return None for every table.
    for slot_name, table_name in CDC_TABLES:
        result = bootstrap_table(slot_name, table_name, bootstrap_settings)
        assert result is None, (
            f"Expected idempotent no-op for {table_name!r}, but bootstrap_table "
            f"returned a resource — slot {slot_name!r} was not detected as existing."
        )

    with duckdb.connect(db_path, read_only=True) as conn:
        after = conn.execute(
            "SELECT count(*) FROM raw._dlt_loads"
        ).fetchone()
    assert after is not None
    assert after[0] == count_before, (
        f"Second bootstrap created new _dlt_loads rows: "
        f"before={count_before}, after={after[0]}"
    )
