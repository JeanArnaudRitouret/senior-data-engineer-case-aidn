from __future__ import annotations

"""Unit tests for aidn.logging_setup — no live DB required."""

import io
import json
import logging

import pytest
from pythonjsonlogger import jsonlogger

from aidn.logging_setup import PiiSafeFilter, bind_run_id, get_logger


@pytest.fixture()
def json_handler() -> logging.StreamHandler[io.StringIO]:
    """StreamHandler writing JSON to an in-memory buffer."""
    stream: io.StringIO = io.StringIO()
    handler: logging.StreamHandler[io.StringIO] = logging.StreamHandler(stream)
    handler.setFormatter(jsonlogger.JsonFormatter())
    return handler


def test_json_output_contains_run_id(
    json_handler: logging.StreamHandler[io.StringIO],
) -> None:
    logger = get_logger("aidn.test.run_id")
    logger.addHandler(json_handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    adapter = bind_run_id(logger, "run-test-001")
    adapter.info("pipeline_start layer=raw")

    output = json.loads(json_handler.stream.getvalue().strip())
    assert output["run_id"] == "run-test-001"

    logger.removeHandler(json_handler)


def test_pii_safe_filter_redacts_name() -> None:
    record = logging.LogRecord(
        name="aidn.test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="patient_ingested",
        args=(),
        exc_info=None,
    )
    # Simulate PII leaking into a record attribute (e.g. via direct assignment
    # in a third-party emitter that bypasses extra= collision checks).
    record.__dict__["name"] = "Alice"

    f = PiiSafeFilter()
    result = f.filter(record)

    assert result is True
    assert record.__dict__["name"] == "<redacted>"


def test_pii_safe_filter_redacts_postcode(
    json_handler: logging.StreamHandler[io.StringIO],
) -> None:
    logger = get_logger("aidn.test.postcode")
    logger.addHandler(json_handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.addFilter(PiiSafeFilter())

    logger.info("patient_ingested", extra={"postcode": "SW1A 1AA"})

    output = json.loads(json_handler.stream.getvalue().strip())
    assert output["postcode"] == "<redacted>"

    logger.removeHandler(json_handler)
    logger.removeFilter(logger.filters[0])
