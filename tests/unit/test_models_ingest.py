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
        )
        assert p.provider_id == "prov-1"
        assert p.deleted_ts is None

    def test_specialty_nullable(self) -> None:
        p = Provider(
            provider_id="prov-2",
            name="Test Provider B",
            specialty=None,
            lsn=1,
            deleted_ts=None,
        )
        assert p.specialty is None

    def test_extra_key_raises(self) -> None:
        with pytest.raises(ValidationError):
            Provider(
                provider_id="prov-1",
                name="Test Provider A",
                specialty=None,
                lsn=1,
                unexpected_column="boom",  # type: ignore[call-arg]
            )

    def test_lsn_must_be_int(self) -> None:
        with pytest.raises(ValidationError):
            Provider(
                provider_id="prov-1",
                name="Test Provider A",
                specialty=None,
                lsn="not-an-int",  # type: ignore[arg-type]
            )

    def test_frozen(self) -> None:
        p = Provider(
            provider_id="prov-1",
            name="Test Provider A",
            specialty=None,
            lsn=1,
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
            updated_at=_TS,  # type: ignore[arg-type]
        )
        assert p.patient_id == "pat-1"

    def test_nullable_fields(self) -> None:
        p = Patient(
            patient_id="pat-2",
            name="Test Patient B",
            primary_provider_id=None,
            postcode=None,
            updated_at=_TS,  # type: ignore[arg-type]
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
                updated_at=_TS,  # type: ignore[arg-type]
                unexpected_column="boom",  # type: ignore[call-arg]
            )

    def test_frozen(self) -> None:
        p = Patient(
            patient_id="pat-1",
            name="Test Patient A",
            primary_provider_id=None,
            postcode=None,
            updated_at=_TS,  # type: ignore[arg-type]
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
            scheduled_at=_TS,  # type: ignore[arg-type]
            status="confirmed",
            event_timestamp=_TS,  # type: ignore[arg-type]
            ingested_at=_TS,  # type: ignore[arg-type]
            lsn=99,
            deleted_ts=None,
        )
        assert a.event_id == "evt-1"
        assert a.deleted_ts is None

    def test_deleted_row(self) -> None:
        a = Appointment(
            event_id="evt-2",
            appointment_id="appt-2",
            patient_id="pat-1",
            provider_id="prov-1",
            scheduled_at=_TS,  # type: ignore[arg-type]
            status="cancelled",
            event_timestamp=_TS,  # type: ignore[arg-type]
            ingested_at=_TS,  # type: ignore[arg-type]
            lsn=100,
            deleted_ts=_TS,  # type: ignore[arg-type]
        )
        assert a.deleted_ts is not None

    def test_extra_key_raises(self) -> None:
        with pytest.raises(ValidationError):
            Appointment(
                event_id="evt-1",
                appointment_id="appt-1",
                patient_id="pat-1",
                provider_id="prov-1",
                scheduled_at=_TS,  # type: ignore[arg-type]
                status="confirmed",
                event_timestamp=_TS,  # type: ignore[arg-type]
                ingested_at=_TS,  # type: ignore[arg-type]
                lsn=1,
                deleted_ts=None,
                unexpected_column="boom",  # type: ignore[call-arg]
            )

    def test_lsn_must_be_int(self) -> None:
        with pytest.raises(ValidationError):
            Appointment(
                event_id="evt-1",
                appointment_id="appt-1",
                patient_id="pat-1",
                provider_id="prov-1",
                scheduled_at=_TS,  # type: ignore[arg-type]
                status="confirmed",
                event_timestamp=_TS,  # type: ignore[arg-type]
                ingested_at=_TS,  # type: ignore[arg-type]
                lsn="not-an-int",  # type: ignore[arg-type]
            )

    def test_frozen(self) -> None:
        a = Appointment(
            event_id="evt-1",
            appointment_id="appt-1",
            patient_id="pat-1",
            provider_id="prov-1",
            scheduled_at=_TS,  # type: ignore[arg-type]
            status="confirmed",
            event_timestamp=_TS,  # type: ignore[arg-type]
            ingested_at=_TS,  # type: ignore[arg-type]
            lsn=1,
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
                unexpected_column="boom",  # type: ignore[call-arg]
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
