"""Per-table Pydantic v2 two-tier validators for the dlt/raw boundary (dlt-standards Rule 12)."""

from __future__ import annotations

from typing import Any, Callable

import pydantic

from aidn.logging_setup import get_logger
from aidn.models.ingest import Appointment, Provider

_logger = get_logger(__name__)


def _validate_provider(row: dict[str, Any]) -> Provider | None:
    """Validate a single row against the Provider model.

    Tier 1 (ValidationError | KeyError): log warning with pseudonymous entity_id;
    return None to drop the row (dlt add_map drops None items).
    Tier 2 (any other Exception): log error with exc_info; re-raise.

    Args:
        row: Raw dict from the pg_replication CDC stream (deleted_ts is None
            for live rows, non-null for WAL DELETE events).

    Returns:
        Validated Provider instance, or None if Tier-1 validation fails.
    """
    try:
        return Provider.model_validate(row)
    except (pydantic.ValidationError, KeyError) as e:
        _logger.warning(
            "row_dropped table=providers reason=%s entity_id=%s",
            type(e).__name__,
            row.get("provider_id"),
        )
        return None
    except Exception:
        _logger.error("row_failed table=providers", exc_info=True)
        raise


def _validate_appointment(row: dict[str, Any]) -> Appointment | None:
    """Validate a single row against the Appointment model.

    Tier 1 (ValidationError | KeyError): log warning with pseudonymous event_id;
    return None to drop the row (dlt add_map drops None items).
    Tier 2 (any other Exception): log error with exc_info; re-raise.

    Args:
        row: Raw dict from the pg_replication CDC stream (deleted_ts is None
            for live rows, non-null for WAL DELETE events).

    Returns:
        Validated Appointment instance, or None if Tier-1 validation fails.
    """
    try:
        return Appointment.model_validate(row)
    except (pydantic.ValidationError, KeyError) as e:
        _logger.warning(
            "row_dropped table=appointments reason=%s entity_id=%s",
            type(e).__name__,
            row.get("event_id"),
        )
        return None
    except Exception:
        _logger.error("row_failed table=appointments", exc_info=True)
        raise


TABLE_VALIDATORS: dict[str, Callable[[dict[str, Any]], Any]] = {
    "providers": _validate_provider,
    "appointments": _validate_appointment,
}
