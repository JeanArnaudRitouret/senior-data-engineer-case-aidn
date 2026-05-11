"""Unit tests for aidn/ingest/resources/providers.py."""

from typing import Any

import pytest
from dlt.sources.credentials import ConnectionStringCredentials

from pg_replication import replication_resource as _pg_replication_resource

from aidn.ingest.resources.providers import (
    _CDC_COLUMNS,
    _PUB_NAME,
    _SLOT_NAME,
)


def _make_resource() -> Any:
    """Return a providers-scoped replication resource with hints applied."""
    creds = ConnectionStringCredentials(
        "postgresql://postgres:dev@localhost:5432/test"
    )
    resource = _pg_replication_resource(
        slot_name=_SLOT_NAME,
        pub_name=_PUB_NAME,
        credentials=creds,
    )
    resource.apply_hints(columns=_CDC_COLUMNS)
    return resource


def test_providers_apply_hints_hard_delete_false() -> None:
    """apply_hints overrides pg_replication default hard_delete=True on deleted_ts."""
    resource = _make_resource()
    cols: dict[str, Any] = resource.columns
    assert "deleted_ts" in cols
    assert cols["deleted_ts"]["hard_delete"] is False


def test_providers_apply_hints_lsn_declared() -> None:
    """apply_hints explicitly declares the lsn column so schema_contract freeze accepts it."""
    resource = _make_resource()
    cols: dict[str, Any] = resource.columns
    assert "lsn" in cols
    assert cols["lsn"]["data_type"] == "bigint"


def test_providers_deleted_ts_absent_on_insert_validates_as_live() -> None:
    """A row without deleted_ts key validates with deleted_ts=None (live provider)."""
    from aidn.models.ingest import Provider

    row: dict[str, Any] = {
        "provider_id": "prov-1",
        "name": "Dr. Smith",
        "specialty": None,
        "lsn": 12345,
    }
    p = Provider.model_validate(row)
    assert p.deleted_ts is None


def test_providers_deleted_ts_present_on_delete_validates_as_deleted() -> None:
    """A row with deleted_ts populated validates correctly (WAL DELETE event)."""
    from datetime import datetime

    from aidn.models.ingest import Provider

    row: dict[str, Any] = {
        "provider_id": "prov-2",
        "name": "Dr. Jones",
        "specialty": "Cardiology",
        "lsn": 67890,
        "deleted_ts": datetime(2024, 6, 1, 12, 0, 0),
    }
    p = Provider.model_validate(row)
    assert p.deleted_ts is not None
