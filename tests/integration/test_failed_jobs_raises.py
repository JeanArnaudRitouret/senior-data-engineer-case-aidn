"""Integration test — PipelineStepFailed propagates out of run_pipeline.

Prerequisite: ``make up`` must have run (the conftest autouse fixture connects to
Postgres to clean up CDC slots).  The test itself does NOT use Postgres as a data
source; it uses an inline dlt resource so that only the destination failure path is
exercised.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import dlt
import pytest
from dlt.pipeline.exceptions import PipelineStepFailed

from aidn.config import Settings
from aidn.ingest.pipeline import run_pipeline
from aidn.logging_setup import bind_run_id, configure_logging, get_logger

logger = get_logger(__name__)


@dlt.resource(write_disposition="append")
def _one_row_resource() -> Iterator[dict[str, Any]]:
    """Yield a single row to guarantee dlt attempts a load step."""
    yield {"id": "1", "value": "test"}


def test_failed_jobs_propagates_pipeline_step_failed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """PipelineStepFailed must propagate out of run_pipeline with an ERROR log.

    The DuckDB destination path is replaced by a directory so DuckDB cannot open
    it as a database file.  dlt wraps the resulting write error as
    ``PipelineStepFailed``; ``run_pipeline`` must re-raise it after emitting an
    ERROR record that contains ``step=`` and carries ``exc_info``.

    Args:
        tmp_path: pytest-provided temporary directory.
        monkeypatch: pytest fixture for environment patching.
        caplog: pytest fixture for log-record capture.
    """
    # Replace DuckDB path with a directory — DuckDB cannot open a directory as a db.
    bad_duckdb = tmp_path / "test.duckdb"
    bad_duckdb.mkdir()

    monkeypatch.setenv("DUCKDB_PATH", str(bad_duckdb))
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
    settings = Settings()  # type: ignore[call-arg]

    configure_logging(settings.log_level)
    run_log = bind_run_id(get_logger(__name__), "test-failure-run")

    with caplog.at_level(logging.ERROR):
        with pytest.raises(PipelineStepFailed):
            run_pipeline(_one_row_resource(), settings=settings, run_logger=run_log)

    error_records = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert error_records, "Expected at least one ERROR log record after pipeline failure"

    dlt_load_failed = [r for r in error_records if "dlt_load_failed" in r.getMessage()]
    assert dlt_load_failed, (
        f"Expected an ERROR record containing 'dlt_load_failed'; "
        f"got: {[r.getMessage() for r in error_records]}"
    )

    record = dlt_load_failed[0]
    assert "step=" in record.getMessage(), (
        f"ERROR record missing 'step=': {record.getMessage()!r}"
    )
    assert record.exc_info is not None, (
        "ERROR record must carry exc_info (exc_info=True was passed to logger.error)"
    )
