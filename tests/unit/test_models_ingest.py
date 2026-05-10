"""Unit tests for aidn/models/ingest.py Pydantic v2 models."""

import pytest
from pydantic import ValidationError

from aidn.models.ingest import Appointment, Patient, PatientConsent, Provider

_TS = "2024-01-15T10:00:00Z"


class TestProvider:
    def test_happy_path(self) -> None:
        p = Provider(
            provider_id="prov-1",
            name="Test Provider A",
            specialty="Cardiology",
            lsn=12345,
            deleted_ts=None,
            is_deleted=False,
        )
        assert p.provider_id == "prov-1"
        assert p.is_deleted is False

    def test_specialty_nullable(self) -> None:
        p = Provider(
            provider_id="prov-2",
            name="Test Provider B",
            specialty=None,
            lsn=1,
            deleted_ts=None,
            is_deleted=False,
        )
        assert p.specialty is None

    def test_extra_key_raises(self) -> None:
        with pytest.raises(ValidationError):
            Provider(
                provider_id="prov-1",
                name="Test Provider A",
                specialty=None,
                lsn=1,
                deleted_ts=None,
                is_deleted=False,
                unexpected_column="boom",
            )

    def test_lsn_must_be_int(self) -> None:
        with pytest.raises(ValidationError):
            Provider(
                provider_id="prov-1",
                name="Test Provider A",
                specialty=None,
                lsn="not-an-int",
                deleted_ts=None,
                is_deleted=False,
            )

    def test_frozen(self) -> None:
        p = Provider(
            provider_id="prov-1",
            name="Test Provider A",
            specialty=None,
            lsn=1,
            deleted_ts=None,
            is_deleted=False,
        )
        with pytest.raises(ValidationError):
            p.provider_id = "mutated"  # type: ignore[misc]


class TestPatient:
    def test_happy_path(self) -> None:
        p = Patient(
            patient_id="pat-1",
            name="Test Patient A",
            primary_provider_id="prov-1",
            postcode="0000",
            updated_at=_TS,
        )
        assert p.patient_id == "pat-1"

    def test_nullable_fields(self) -> None:
        p = Patient(
            patient_id="pat-2",
            name="Test Patient B",
            primary_provider_id=None,
            postcode=None,
            updated_at=_TS,
        )
        assert p.primary_provider_id is None
        assert p.postcode is None

    def test_extra_key_raises(self) -> None:
        with pytest.raises(ValidationError):
            Patient(
                patient_id="pat-1",
                name="Test Patient A",
                primary_provider_id=None,
                postcode=None,
                updated_at=_TS,
                unexpected_column="boom",
            )

    def test_frozen(self) -> None:
        p = Patient(
            patient_id="pat-1",
            name="Test Patient A",
            primary_provider_id=None,
            postcode=None,
            updated_at=_TS,
        )
        with pytest.raises(ValidationError):
            p.patient_id = "mutated"  # type: ignore[misc]


class TestAppointment:
    def test_happy_path(self) -> None:
        a = Appointment(
            event_id="evt-1",
            appointment_id="appt-1",
            patient_id="pat-1",
            provider_id="prov-1",
            scheduled_at=_TS,
            status="confirmed",
            event_timestamp=_TS,
            ingested_at=_TS,
            lsn=99,
            deleted_ts=None,
            is_deleted=False,
        )
        assert a.event_id == "evt-1"
        assert a.is_deleted is False

    def test_deleted_row(self) -> None:
        a = Appointment(
            event_id="evt-2",
            appointment_id="appt-2",
            patient_id="pat-1",
            provider_id="prov-1",
            scheduled_at=_TS,
            status="cancelled",
            event_timestamp=_TS,
            ingested_at=_TS,
            lsn=100,
            deleted_ts=_TS,
            is_deleted=True,
        )
        assert a.is_deleted is True
        assert a.deleted_ts is not None

    def test_extra_key_raises(self) -> None:
        with pytest.raises(ValidationError):
            Appointment(
                event_id="evt-1",
                appointment_id="appt-1",
                patient_id="pat-1",
                provider_id="prov-1",
                scheduled_at=_TS,
                status="confirmed",
                event_timestamp=_TS,
                ingested_at=_TS,
                lsn=1,
                deleted_ts=None,
                is_deleted=False,
                unexpected_column="boom",
            )

    def test_lsn_must_be_int(self) -> None:
        with pytest.raises(ValidationError):
            Appointment(
                event_id="evt-1",
                appointment_id="appt-1",
                patient_id="pat-1",
                provider_id="prov-1",
                scheduled_at=_TS,
                status="confirmed",
                event_timestamp=_TS,
                ingested_at=_TS,
                lsn="not-an-int",
                deleted_ts=None,
                is_deleted=False,
            )

    def test_frozen(self) -> None:
        a = Appointment(
            event_id="evt-1",
            appointment_id="appt-1",
            patient_id="pat-1",
            provider_id="prov-1",
            scheduled_at=_TS,
            status="confirmed",
            event_timestamp=_TS,
            ingested_at=_TS,
            lsn=1,
            deleted_ts=None,
            is_deleted=False,
        )
        with pytest.raises(ValidationError):
            a.event_id = "mutated"  # type: ignore[misc]


class TestPatientConsent:
    def test_happy_path(self) -> None:
        c = PatientConsent(
            patient_id="pat-1",
            consent_research=True,
            consent_marketing=False,
            consent_partner_share=False,
        )
        assert c.patient_id == "pat-1"
        assert c.consent_research is True

    def test_extra_key_raises(self) -> None:
        with pytest.raises(ValidationError):
            PatientConsent(
                patient_id="pat-1",
                consent_research=True,
                consent_marketing=False,
                consent_partner_share=False,
                unexpected_column="boom",
            )

    def test_frozen(self) -> None:
        c = PatientConsent(
            patient_id="pat-1",
            consent_research=True,
            consent_marketing=False,
            consent_partner_share=False,
        )
        with pytest.raises(ValidationError):
            c.patient_id = "mutated"  # type: ignore[misc]
