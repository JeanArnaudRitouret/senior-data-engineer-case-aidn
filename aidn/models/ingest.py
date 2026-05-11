"""Pydantic v2 models for the four source entities at the dlt/raw boundary.

All models are frozen and reject unexpected columns (extra="forbid") so that
schema_contract freeze violations surface as rich ValidationError messages before
dlt's own schema_contract check can fire.

patient.name (direct identifier) is dropped at the dlt extraction boundary and
never enters raw. postcode (quasi-identifier) is retained in raw and removed by
the GDPR erasure sweep. provider.name is a non-patient identifier and is retained as-is.
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
            ``deleted_ts IS NOT NULL`` is the sole delete signal at raw.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    provider_id: str
    name: str
    specialty: str | None
    lsn: int | None = None
    deleted_ts: datetime | None = None


class Patient(BaseModel):
    """A patient WAL event emitted by the pg_replication CDC resource.

    Attributes:
        patient_id: Pseudonymous stable key linking records across tables.
        lsn: WAL log sequence number (pg_lsn converted to int by dlt); None on
            snapshot rows which are loaded before CDC begins.
        primary_provider_id: FK to providers; None when unassigned.
        postcode: Geographic quasi-identifier — PII, do not log.
        updated_at: Source-side last-modified timestamp; None on DELETE WAL events
            which may not carry non-key columns.
        deleted_ts: Timestamp of the WAL delete event; None when row is live.
            ``deleted_ts IS NOT NULL`` is the sole delete signal at raw.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    patient_id: str
    lsn: int | None = None
    primary_provider_id: str | None = None
    postcode: str | None = None
    updated_at: datetime | None = None
    deleted_ts: datetime | None = None


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
            ``deleted_ts IS NOT NULL`` is the sole delete signal at raw.
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
    lsn: int | None = None
    deleted_ts: datetime | None = None


class PatientConsent(BaseModel):
    """A patient consent CDC event row emitted by the pg_replication resource.

    WAL DELETE events carry only patient_id, lsn, and deleted_ts — consent flag
    columns are absent. Flags are optional to prevent Tier-1 drops on valid
    DELETE events.

    Attributes:
        patient_id: FK to patients; pseudonymous key.
        consent_research: Patient consented to research use; None on DELETE events.
        consent_marketing: Patient consented to marketing; None on DELETE events.
        consent_partner_share: Patient consented to partner sharing; None on DELETE events.
        lsn: WAL log sequence number; merge key and event-ordering column.
        deleted_ts: Populated on WAL DELETE events; NULL on INSERT/UPDATE.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    patient_id: str
    consent_research: bool | None = None
    consent_marketing: bool | None = None
    consent_partner_share: bool | None = None
    lsn: int | None = None
    deleted_ts: datetime | None = None
