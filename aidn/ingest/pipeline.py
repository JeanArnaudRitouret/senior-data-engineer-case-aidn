"""dlt pipeline factory and run entrypoint for the Aidn ingest layer."""

from __future__ import annotations

import logging
import time
from typing import Any

import dlt
from dlt.destinations import duckdb as dlt_duckdb
from dlt.extract import DltResource
from dlt.pipeline.exceptions import PipelineStepFailed

from aidn.config import Settings
from aidn.ingest.resources.appointments import appointments_resource
from aidn.ingest.resources.patient_consents import patient_consents_resource
from aidn.ingest.resources.patients import patients_resource
from aidn.ingest.resources.providers import providers_resource
from aidn.logging_setup import get_logger

logger = get_logger(__name__)

# Retry parameters — importable by tests to assert retry behaviour.
MAX_RETRIES: int = 3
BASE_DELAY_SECONDS: float = 1.0


def make_pipeline(settings: Settings) -> dlt.Pipeline:
    """Create the dlt pipeline targeting the DuckDB destination.

    Args:
        settings: Runtime settings supplying DuckDB path and dlt state directory.

    Returns:
        Configured dlt Pipeline instance.
    """
    return dlt.pipeline(
        pipeline_name="aidn_ingest",
        destination=dlt_duckdb(credentials=str(settings.duckdb_path)),
        dataset_name="raw",
        pipelines_dir=str(settings.dlt_data_dir),
    )


@dlt.source(name="aidn_ingest")
def aidn_source(settings: Settings) -> list[DltResource]:
    """Return all four ingest resources as the aidn_ingest dlt source.

    Decorated with ``@dlt.source`` — calling ``aidn_source(settings)`` returns
    a ``DltSource`` whose resources can be narrowed with
    ``source.with_resources("<name>")``.

    Args:
        settings: Runtime settings supplying connection URLs for each resource.

    Returns:
        List of DltResource objects; dlt wraps these into a DltSource on return.
    """
    return [
        providers_resource(settings),
        appointments_resource(settings),
        patients_resource(settings),
        patient_consents_resource(settings),
    ]


def run_pipeline(
    source: Any,
    *,
    settings: Settings,
    run_logger: logging.LoggerAdapter[logging.Logger],
) -> str:
    """Run a dlt source through the pipeline and return the committed load_id.

    Primary failure contract: ``PipelineStepFailed`` is dlt's default raise on
    terminal job failure (dlt-standards Rule 7). ``raise_on_failed_jobs`` is not
    overridden, so the ``has_failed_jobs`` path is not the primary contract.

    ``LoadInfo.loads_ids`` is handled with explicit 0 / 1 / N branching
    (dlt-standards Rule 9) — a multi-package run fails loudly rather than
    silently adopting an arbitrary package id.

    Args:
        source: dlt source or resource to load into ``raw.*``.
        settings: Runtime settings used to construct the pipeline.
        run_logger: ``LoggerAdapter`` with ``run_id`` pre-bound; every log
            record in this function carries the caller-supplied run identifier.

    Returns:
        The committed ``load_id`` string, or ``""`` on a no-op run (zero new rows).

    Raises:
        PipelineStepFailed: Terminal dlt job failure — re-raised after logging.
        RuntimeError: dlt committed more than one load package in a single run.
    """
    pipeline = make_pipeline(settings)
    started_at = time.monotonic()

    try:
        info = pipeline.run(source)
    except PipelineStepFailed as exc:
        run_logger.error(
            "dlt_load_failed step=%s",
            exc.step,
            exc_info=True,
        )
        raise

    ids = info.loads_ids
    duration_ms = int((time.monotonic() - started_at) * 1000)

    if len(ids) == 0:
        run_logger.info("pipeline_noop reason=no_new_rows")
        return ""

    if len(ids) == 1:
        run_id = ids[0]
        # Per-table row counts are emitted by each transformer's transform_complete
        # log line; this summary carries run-level context only.
        run_logger.info(
            "pipeline_complete run_id=%s layer=raw rows_in=0 rows_out=0 "
            "rows_dropped=0 duration_ms=%d status=success",
            run_id,
            duration_ms,
        )
        return run_id

    # N > 1 packages in one sequential run — unexpected; fail loudly.
    run_logger.error("pipeline_multi_package run_ids=%s", ids)
    raise RuntimeError(f"Unexpected multi-package run: {ids}")
