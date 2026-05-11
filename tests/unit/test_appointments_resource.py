"""Unit tests for aidn/ingest/resources/appointments.py."""

from typing import Any

from dlt.sources.credentials import ConnectionStringCredentials

from pg_replication import replication_resource as _pg_replication_resource

from aidn.ingest.resources.appointments import (
    _COLUMN_HINTS,
    _PUB_NAME,
    _SLOT_NAME,
)


def _make_resource() -> Any:
    """Return an appointments-scoped replication resource with hints applied."""
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


def test_appointments_apply_hints_hard_delete_false() -> None:
    """apply_hints overrides pg_replication default hard_delete=True on deleted_ts."""
    resource = _make_resource()
    cols: dict[str, Any] = resource.columns
    assert "deleted_ts" in cols
    assert cols["deleted_ts"]["hard_delete"] is False


def test_appointments_apply_hints_lsn_declared() -> None:
    """apply_hints explicitly declares the lsn column so schema_contract freeze accepts it."""
    resource = _make_resource()
    cols: dict[str, Any] = resource.columns
    assert "lsn" in cols
    assert cols["lsn"]["data_type"] == "bigint"


def test_appointments_no_static_dedup_sort_column() -> None:
    """Our column hints must declare zero dedup_sort columns.

    pg_replication adds dedup_sort to lsn dynamically during schema evolution.
    Declaring a second dedup_sort (e.g. on ingested_at) in apply_hints causes
    SchemaCorruptedException on the second pipeline run.
    """
    resource = _make_resource()
    cols: dict[str, Any] = resource.columns
    for col_name, col_hints in cols.items():
        assert col_hints.get("dedup_sort") is None, (
            f"Column {col_name!r} has dedup_sort set in static hints — "
            "pg_replication already sets dedup_sort on lsn dynamically; "
            "a second dedup_sort column causes SchemaCorruptedException."
        )


def test_appointments_deleted_ts_absent_on_insert_validates_as_live() -> None:
    """A row without deleted_ts key validates with deleted_ts=None (live appointment)."""
    from datetime import datetime

    from aidn.models.ingest import Appointment

    row: dict[str, Any] = {
        "event_id": "evt-1",
        "appointment_id": "appt-1",
        "patient_id": "pat-1",
        "provider_id": "prov-1",
        "scheduled_at": datetime(2024, 3, 1, 9, 0),
        "status": "scheduled",
        "event_timestamp": datetime(2024, 3, 1, 8, 0),
        "ingested_at": datetime(2024, 3, 1, 8, 1),
    }
    a = Appointment.model_validate(row)
    assert a.deleted_ts is None


def test_appointments_deleted_ts_present_on_delete_validates_as_deleted() -> None:
    """A row with deleted_ts populated validates correctly (WAL DELETE event)."""
    from datetime import datetime

    from aidn.models.ingest import Appointment

    row: dict[str, Any] = {
        "event_id": "evt-2",
        "appointment_id": "appt-2",
        "patient_id": "pat-2",
        "provider_id": "prov-2",
        "scheduled_at": datetime(2024, 3, 2, 10, 0),
        "status": "completed",
        "event_timestamp": datetime(2024, 3, 2, 11, 0),
        "ingested_at": datetime(2024, 3, 2, 11, 1),
        "deleted_ts": datetime(2024, 3, 2, 12, 0),
    }
    a = Appointment.model_validate(row)
    assert a.deleted_ts is not None
