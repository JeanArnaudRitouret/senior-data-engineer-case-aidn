"""Pydantic v2 models for the four source entities at the dlt/raw boundary.

All models are frozen and reject unexpected columns (extra="forbid") so that
schema_contract freeze violations surface as rich ValidationError messages before
dlt's own schema_contract check can fire.

PII fields (name, postcode) are intentionally present at this layer — raw is the
archive tier. Direct identifiers are stripped at the raw → staging boundary.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class Provider(BaseModel):
    """A provider record emitted by the pg_replication CDC resource.

    Attributes:
        provider_id: Stable primary key from the source table.
        name: Provider display name. PII — do not log.
        specialty: Clinical specialty; None when not recorded.
        lsn: WAL log sequence number (pg_lsn converted to int by dlt).
        deleted_ts: Timestamp of the WAL delete event; None when row is live.
        is_deleted: Derived flag: True when deleted_ts is not None.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    provider_id: str
    name: str
    specialty: str | None
    lsn: int
    deleted_ts: datetime | None
    is_deleted: bool


class Patient(BaseModel):
    """A patient snapshot row emitted by the sql_database incremental resource.

    Attributes:
        patient_id: Pseudonymous stable key linking records across tables.
        name: Patient display name. Direct identifier — PII, do not log.
        primary_provider_id: FK to providers; None when unassigned.
        postcode: Geographic quasi-identifier — PII, do not log.
        updated_at: Source-side watermark column driving incremental cursor.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    patient_id: str
    name: str
    primary_provider_id: str | None
    postcode: str | None
    updated_at: datetime


class Appointment(BaseModel):
    """An appointment event emitted by the pg_replication CDC resource.

    event_id is the dedup primary key (at-least-once at-least-once events share
    appointment_id but differ by event_id). Status-history collapse on
    appointment_id by latest event_timestamp lives in intm, not raw.

    Attributes:
        event_id: Unique event identifier; primary key for raw-layer dedup.
        appointment_id: Business identifier grouping status-history events.
        patient_id: FK to patients.
        provider_id: FK to providers.
        scheduled_at: Scheduled appointment timestamp.
        status: Appointment lifecycle status string.
        event_timestamp: Source-side event time; dedup tie-break in intm.
        ingested_at: Pipeline ingestion timestamp; dedup_sort column in raw.
        lsn: WAL log sequence number (pg_lsn converted to int by dlt).
        deleted_ts: Timestamp of the WAL delete event; None when row is live.
        is_deleted: Derived flag: True when deleted_ts is not None.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    event_id: str
    appointment_id: str
    patient_id: str
    provider_id: str
    scheduled_at: datetime
    status: str
    event_timestamp: datetime
    ingested_at: datetime
    lsn: int
    deleted_ts: datetime | None
    is_deleted: bool


class PatientConsent(BaseModel):
    """A patient consent snapshot row for the Regime A SCD2 resource.

    Full-SELECT snapshot yielded on every run; dlt auto-closes absent rows via
    _dlt_valid_to. No source updated_at — _dlt_loaded_at serves as the boundary
    timestamp.

    Attributes:
        patient_id: FK to patients; SCD2 key.
        consent_research: Patient has consented to use of data for research.
        consent_marketing: Patient has consented to marketing communications.
        consent_partner_share: Patient has consented to sharing with partners.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    patient_id: str
    consent_research: bool
    consent_marketing: bool
    consent_partner_share: bool
