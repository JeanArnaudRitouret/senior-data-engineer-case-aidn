"""Unit tests for aidn/ingest/resources/patients.py."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from unittest.mock import MagicMock, patch

from aidn.ingest.resources.patients import (
    _SCHEMA_CONTRACT,
    _WRITE_DISPOSITION,
)


def _make_mock_settings() -> MagicMock:
    """Return a Settings mock with a dummy postgres_url."""
    settings = MagicMock()
    settings.postgres_url = "postgresql://postgres:dev@localhost:5432/aidn?sslmode=require"
    return settings


def test_patients_write_disposition_is_append() -> None:
    """Module constant confirms write_disposition=append (Q22: no merge destroys SCD2 history)."""
    assert _WRITE_DISPOSITION == "append"


def test_patients_schema_contract_is_freeze() -> None:
    """schema_contract freeze prevents silent new columns landing in raw."""
    assert _SCHEMA_CONTRACT == {"columns": "freeze"}


def test_patients_no_hard_delete_in_schema_contract() -> None:
    """hard_delete is absent — patients has no row-level delete path (Q22)."""
    assert "hard_delete" not in str(_SCHEMA_CONTRACT)


def test_patients_resource_apply_hints_write_disposition_and_schema_contract() -> None:
    """patients_resource calls apply_hints with write_disposition=append and freeze."""
    with patch("aidn.ingest.resources.patients.sql_table") as mock_sql_table:
        mock_resource = MagicMock()
        mock_sql_table.return_value = mock_resource

        from aidn.ingest.resources.patients import patients_resource

        patients_resource(_make_mock_settings())

        mock_resource.apply_hints.assert_called_once_with(
            write_disposition="append",
            schema_contract={"columns": "freeze"},
        )


def test_patients_resource_no_hard_delete_in_apply_hints() -> None:
    """apply_hints call carries no hard_delete key — no soft-delete for patients."""
    with patch("aidn.ingest.resources.patients.sql_table") as mock_sql_table:
        mock_resource = MagicMock()
        mock_sql_table.return_value = mock_resource

        from aidn.ingest.resources.patients import patients_resource

        patients_resource(_make_mock_settings())

        assert "hard_delete" not in str(mock_resource.apply_hints.call_args)


def test_patients_model_has_no_is_deleted_or_deleted_ts_field() -> None:
    """Patient model carries no delete-signal fields — raw layer has no delete path (Q22)."""
    from aidn.models.ingest import Patient

    assert "is_deleted" not in Patient.model_fields
    assert "deleted_ts" not in Patient.model_fields


def test_patients_model_validates_standard_row() -> None:
    """Patient validates from a standard sql_table row with no delete-related keys."""
    from aidn.models.ingest import Patient

    row: dict[str, Any] = {
        "patient_id": "pat-001",
        "name": "Test Patient A",
        "primary_provider_id": None,
        "postcode": "0000",
        "updated_at": datetime(2024, 1, 1, 0, 0, 0),
    }
    p = Patient.model_validate(row)
    assert p.patient_id == "pat-001"
    assert p.updated_at == datetime(2024, 1, 1)


def test_validate_patient_logs_only_patient_id_not_pii(
    caplog: Any,
) -> None:
    """_validate_patient warning log contains entity_id but never name or postcode (PII)."""
    import logging

    from aidn.ingest.validators import _validate_patient

    # Missing required field updated_at triggers Tier-1 drop
    bad_row: dict[str, Any] = {
        "patient_id": "pat-bad",
        "name": "Sensitive Name",
        "postcode": "1234",
        # updated_at intentionally omitted to trigger ValidationError
    }
    with caplog.at_level(logging.WARNING, logger="aidn.ingest.validators"):
        result = _validate_patient(bad_row)

    assert result is None
    assert len(caplog.records) == 1
    log_text = caplog.records[0].message
    assert "pat-bad" in log_text
    assert "Sensitive Name" not in log_text
    assert "1234" not in log_text
