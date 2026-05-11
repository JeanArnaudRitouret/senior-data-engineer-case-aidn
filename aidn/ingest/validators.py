"""Pydantic v2 row-level validators wired into dlt resources via add_map.

Two-tier handling: ValidationError → drop row + WARN with rows_dropped count;
any other exception → ERROR + re-raise (never swallowed)."""

from __future__ import annotations

from typing import Any, Callable

import pydantic
from pydantic import BaseModel

from aidn.logging_setup import get_logger
from aidn.models.ingest import Appointment, Patient, PatientConsent, Provider

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


def _validate_patient(row: dict[str, Any]) -> Patient | None:
    """Validate a single row against the Patient model.

    Tier 1 (ValidationError | KeyError): log warning with pseudonymous patient_id;
    return None to drop the row (dlt add_map drops None items).
    Tier 2 (any other Exception): log error with exc_info; re-raise.

    PII: ``postcode`` is a quasi-identifier and must never appear in any log record.
    ``name`` is dropped at the resource boundary and never reaches this validator.
    Only the pseudonymous ``patient_id`` key is logged.

    Args:
        row: Raw dict from the sql_table incremental source.

    Returns:
        Validated Patient instance, or None if Tier-1 validation fails.
    """
    try:
        return Patient.model_validate(row)
    except (pydantic.ValidationError, KeyError) as e:
        _logger.warning(
            "row_dropped table=patients reason=%s entity_id=%s",
            type(e).__name__,
            row.get("patient_id"),
        )
        return None
    except Exception:
        _logger.error("row_failed table=patients", exc_info=True)
        raise


def _validate_patient_consent(row: dict[str, Any]) -> PatientConsent | None:
    """Validate a single row against the PatientConsent model.

    Tier 1 (ValidationError | KeyError): log warning with pseudonymous patient_id;
    return None to drop the row (dlt add_map drops None items).
    Tier 2 (any other Exception): log error with exc_info; re-raise.

    Note: the three consent boolean columns are NULLABLE in the source schema.
    A NULL value will raise ValidationError (model types them as bool, not
    bool | None). This is the safe failure mode — the row is dropped with a
    warning. If intentional NULLs appear, the model should be updated to
    bool | None and the downstream consent-filter logic reviewed.

    Args:
        row: Raw dict from the sql_table full-SELECT snapshot.

    Returns:
        Validated PatientConsent instance, or None if Tier-1 validation fails.
    """
    try:
        return PatientConsent.model_validate(row)
    except (pydantic.ValidationError, KeyError) as e:
        _logger.warning(
            "row_dropped table=patient_consents reason=%s entity_id=%s",
            type(e).__name__,
            row.get("patient_id"),
        )
        return None
    except Exception:
        _logger.error("row_failed table=patient_consents", exc_info=True)
        raise


TABLE_VALIDATORS: dict[str, Callable[[dict[str, Any]], BaseModel | None]] = {
    "providers": _validate_provider,
    "appointments": _validate_appointment,
    "patients": _validate_patient,
    "patient_consents": _validate_patient_consent,
}
