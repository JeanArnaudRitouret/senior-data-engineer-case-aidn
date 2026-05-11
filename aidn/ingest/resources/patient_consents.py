"""CDC resource for the patient_consents table — pg_replication merge on lsn; hard_delete overridden to False.

Slot ``aidn_patient_consents_slot`` is owned by ``aidn/ingest/bootstrap.py``.
This module consumes a pre-existing slot and must not call ``init_replication()``.
raw.patient_consents is an append-only WAL event log; one row per WAL event.
SCD2 reconstruction is deferred to dbt (int_patient_consents_scd2 — see TO_DO Q.deferred).
"""

from __future__ import annotations

from typing import Any

from dlt.extract import DltResource
from dlt.sources.credentials import ConnectionStringCredentials

from pg_replication import replication_resource

from aidn.config import Settings
from aidn.ingest.validators import _validate_patient_consent

_SLOT_NAME: str = "aidn_patient_consents_slot"
_PUB_NAME: str = "aidn_patient_consents_pub"

_CDC_COLUMNS: dict[str, Any] = {
    "lsn": {"data_type": "bigint", "nullable": True},
    "deleted_ts": {
        # Override pg_replication's default hard_delete=True so DELETE events
        # produce a row with deleted_ts IS NOT NULL rather than removing the row.
        "hard_delete": False,
        "data_type": "timestamp",
        "nullable": True,
    },
}


def patient_consents_resource(settings: Settings) -> DltResource:
    """Return a configured CDC dlt resource for the patient_consents table.

    primary_key="lsn" preserves full WAL event history in raw — every INSERT,
    UPDATE, and DELETE lands as a separate row. SCD2 reconstruction is deferred
    to a dbt intm model (int_patient_consents_scd2).

    Args:
        settings: Runtime settings supplying the replication connection URL.

    Returns:
        DltResource ready to be included in an ``aidn_source()`` factory.
    """
    creds = ConnectionStringCredentials(str(settings.postgres_repl_url))
    resource: DltResource = replication_resource(
        slot_name=_SLOT_NAME,
        pub_name=_PUB_NAME,
        credentials=creds,
    )
    resource.apply_hints(
        table_name="patient_consents",
        write_disposition="merge",
        primary_key="lsn",
        schema_contract={"columns": "freeze"},
        columns=_CDC_COLUMNS,
    )
    resource.add_map(_validate_patient_consent)
    return resource
