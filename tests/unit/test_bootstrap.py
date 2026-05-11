"""Unit tests for aidn/ingest/bootstrap.py — no live DB required."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

from aidn.ingest.bootstrap import _SNAPSHOT_CDC_COLUMNS, bootstrap_table
from aidn.ingest.validators import TABLE_VALIDATORS


def _mock_settings() -> Any:
    """Return a MagicMock stand-in for Settings with required bootstrap fields."""
    s = MagicMock()
    s.postgres_repl_url = (
        "postgresql://postgres:dev@localhost:5432/test?sslmode=require"
    )
    s.postgres_source_schema = "public"
    return s


def test_bootstrap_snapshot_declares_cdc_columns() -> None:
    """apply_hints is called with lsn and deleted_ts column declarations on the snapshot resource."""
    mock_resource = MagicMock()

    with (
        patch("aidn.ingest.bootstrap._slot_exists", return_value=False),
        patch(
            "aidn.ingest.bootstrap.init_replication", return_value=[mock_resource]
        ),
    ):
        result = bootstrap_table(
            "aidn_providers_slot", "providers", "provider_id", "aidn_providers_pub", _mock_settings()
        )

    assert result is mock_resource
    mock_resource.apply_hints.assert_called_once_with(
        primary_key="provider_id", columns=_SNAPSHOT_CDC_COLUMNS
    )
    assert "lsn" in _SNAPSHOT_CDC_COLUMNS
    assert "deleted_ts" in _SNAPSHOT_CDC_COLUMNS


def test_bootstrap_snapshot_applies_validator() -> None:
    """add_map is called with the correct TABLE_VALIDATORS entry for the table."""
    mock_resource = MagicMock()

    with (
        patch("aidn.ingest.bootstrap._slot_exists", return_value=False),
        patch(
            "aidn.ingest.bootstrap.init_replication", return_value=[mock_resource]
        ),
    ):
        bootstrap_table(
            "aidn_providers_slot", "providers", "provider_id", "aidn_providers_pub", _mock_settings()
        )

    mock_resource.add_map.assert_called_once_with(TABLE_VALIDATORS["providers"])


def test_bootstrap_snapshot_applies_validator_appointments() -> None:
    """add_map for appointments uses the appointments validator, not the providers one."""
    mock_resource = MagicMock()

    with (
        patch("aidn.ingest.bootstrap._slot_exists", return_value=False),
        patch(
            "aidn.ingest.bootstrap.init_replication", return_value=[mock_resource]
        ),
    ):
        bootstrap_table(
            "aidn_appointments_slot",
            "appointments",
            "event_id",
            "aidn_appointments_pub",
            _mock_settings(),
        )

    mock_resource.add_map.assert_called_once_with(TABLE_VALIDATORS["appointments"])
    assert TABLE_VALIDATORS["appointments"] is not TABLE_VALIDATORS["providers"]


def test_bootstrap_skip_when_slot_exists_does_not_apply_hints() -> None:
    """Idempotency path: returns None; init_replication, apply_hints, add_map not called."""
    with (
        patch("aidn.ingest.bootstrap._slot_exists", return_value=True) as mock_slot,
        patch("aidn.ingest.bootstrap.init_replication") as mock_init,
    ):
        result = bootstrap_table(
            "aidn_providers_slot", "providers", "provider_id", "aidn_providers_pub", _mock_settings()
        )

    assert result is None
    mock_slot.assert_called_once()
    mock_init.assert_not_called()
