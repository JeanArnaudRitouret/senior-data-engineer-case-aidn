from __future__ import annotations

"""Unit tests for aidn.ingest.transformers — no live DB required."""

import logging
from collections.abc import Iterator

import pydantic
import pytest

from aidn.ingest.transformers import validate


class _Row(pydantic.BaseModel):
    """Minimal model used only in these tests."""

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")

    id: str
    value: int


def test_validate_drops_invalid_row_increments_rows_dropped(
    caplog: pytest.LogCaptureFixture,
) -> None:
    transformer = validate(_Row, table="rows", pk="id")
    rows = [
        {"id": "r1", "value": "not-an-int"},  # ValidationError — dropped
        {"id": "r2", "value": 42},            # valid — passed through
    ]

    with caplog.at_level(logging.INFO, logger="aidn.ingest.transformers"):
        results = list(transformer(iter(rows)))

    assert len(results) == 1
    assert results[0].id == "r2"

    warning_messages = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("row_dropped" in m and "table=rows" in m for m in warning_messages)

    info_messages = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
    assert any("rows_dropped=1" in m and "reason=validation_error" in m for m in info_messages)


def test_validate_unexpected_exception_propagates() -> None:
    class _Simple(pydantic.BaseModel):
        """Single-field model for propagation test."""

        id: str

    def _bad_rows() -> Iterator[dict[str, str]]:
        yield {"id": "x"}
        raise RuntimeError("unexpected failure")

    transformer = validate(_Simple, table="simple", pk="id")

    with pytest.raises(RuntimeError, match="unexpected failure"):
        list(transformer(_bad_rows()))


def test_validate_no_pii_field_values_in_log_output(
    caplog: pytest.LogCaptureFixture,
) -> None:
    transformer = validate(_Row, table="rows", pk="id")
    # Passing a value that would cause a ValidationError; the raw value must
    # not appear in any log record (privacy-consent Rule 5).
    rows = [{"id": "r1", "value": "SECRET_PATIENT_DATA"}]

    with caplog.at_level(logging.INFO, logger="aidn.ingest.transformers"):
        list(transformer(iter(rows)))

    for record in caplog.records:
        assert "SECRET_PATIENT_DATA" not in record.getMessage()
