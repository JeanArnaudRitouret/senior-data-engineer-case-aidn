"""Unit tests for aidn/ingest/resources/patients.py."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from dlt.sources.credentials import ConnectionStringCredentials

from pg_replication import replication_resource as _pg_replication_resource

from aidn.ingest.resources.patients import (
    _COLUMN_HINTS,
    _PUB_NAME,
    _SLOT_NAME,
    _strip_name,
)


def _make_resource() -> Any:
    """Return a patients-scoped replication resource with column hints applied."""
    creds = ConnectionStringCredentials(
        "postgresql://postgres:dev@localhost:5432/test"
    )
    resource = _pg_replication_resource(
        slot_name=_SLOT_NAME,
        pub_name=_PUB_NAME,
        credentials=creds,
    )
    resource.apply_hints(columns=_COLUMN_HINTS)
    return resource


# --- Column-hint invariants (mirrors test_appointments_resource.py) ---


def test_patients_apply_hints_hard_delete_false() -> None:
    """apply_hints overrides pg_replication default hard_delete=True on deleted_ts."""
    resource = _make_resource()
    cols: dict[str, Any] = resource.columns
    assert "deleted_ts" in cols
    assert cols["deleted_ts"]["hard_delete"] is False


def test_patients_apply_hints_lsn_declared() -> None:
    """apply_hints explicitly declares lsn so schema_contract freeze accepts it."""
    resource = _make_resource()
    cols: dict[str, Any] = resource.columns
    assert "lsn" in cols
    assert cols["lsn"]["data_type"] == "bigint"


def test_patients_no_static_dedup_sort_column() -> None:
    """Column hints must declare zero dedup_sort columns.

    pg_replication adds dedup_sort to lsn dynamically during schema evolution.
    A second static dedup_sort causes SchemaCorruptedException on the second run
    (per 1.27a).
    """
    resource = _make_resource()
    cols: dict[str, Any] = resource.columns
    for col_name, col_hints in cols.items():
        assert col_hints.get("dedup_sort") is None, (
            f"Column {col_name!r} has dedup_sort set in static hints — "
            "pg_replication already sets dedup_sort on lsn dynamically."
        )


# --- _strip_name invariants ---


def test_patients_strip_name_removes_name_key() -> None:
    """_strip_name drops the name key from a WAL event dict (Q40, P.2)."""
    row: dict[str, Any] = {"patient_id": "pat-1", "name": "Alice", "postcode": "0000"}
    result = _strip_name(row)
    assert "name" not in result
    assert result["patient_id"] == "pat-1"
    assert result["postcode"] == "0000"


def test_patients_strip_name_noop_when_absent() -> None:
    """_strip_name is a no-op on DELETE events that carry no name column."""
    row: dict[str, Any] = {"patient_id": "pat-1", "deleted_ts": "2024-01-01T00:00:00"}
    result = _strip_name(row)
    assert result == {"patient_id": "pat-1", "deleted_ts": "2024-01-01T00:00:00"}


# --- Model invariants ---


def test_patients_model_has_lsn_and_deleted_ts_fields() -> None:
    """Patient carries lsn and deleted_ts (Phase Patient CDC); is_deleted never materialized at raw (Q36)."""
    from aidn.models.ingest import Patient

    assert "lsn" in Patient.model_fields
    assert "deleted_ts" in Patient.model_fields
    assert "is_deleted" not in Patient.model_fields


def test_patients_model_validates_standard_row() -> None:
    """Patient validates from a WAL event row with no delete-related keys."""
    from aidn.models.ingest import Patient

    row: dict[str, Any] = {
        "patient_id": "pat-001",
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
    """_validate_patient warning log contains entity_id but never postcode (PII)."""
    import logging

    from aidn.ingest.validators import _validate_patient

    # updated_at is now optional; type mismatch triggers Tier-1 ValidationError
    bad_row: dict[str, Any] = {
        "patient_id": "pat-bad",
        "postcode": "1234",
        "updated_at": "not-a-datetime",
    }
    with caplog.at_level(logging.WARNING, logger="aidn.ingest.validators"):
        result = _validate_patient(bad_row)

    assert result is None
    assert len(caplog.records) == 1
    log_text = caplog.records[0].message
    assert "pat-bad" in log_text
    assert "1234" not in log_text
