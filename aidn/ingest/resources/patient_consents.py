"""Regime A SCD2 resource for the patient_consents table — full SELECT on every run (Q23 + Q24).

No merge_key: dlt SCD2 uses primary_key=patient_id to compare the full snapshot against the
destination; rows absent from the snapshot have _dlt_valid_to set (absent-row retirement);
rows whose content changes are closed and a new row is inserted (content-change close-out).
No hard_delete, no is_deleted: consent withdrawal is a flag flip (state change),
not a delete; GDPR Art. 17 erasure is a separate Phase 5.5 delete-insert resource
(Q24, dlt-standards Rule 5). boundary_timestamp uses _dlt_loaded_at (default) because
the source table has no updated_at column (Q23).
"""

from __future__ import annotations

from dlt.common.schema.typing import TSchemaContractDict, TScd2StrategyDict
from dlt.extract import DltResource
from dlt.sources.sql_database import sql_table

from aidn.config import Settings
from aidn.ingest.validators import _validate_patient_consent

# Regime A SCD2: full snapshot on every run; no merge_key so dlt compares the
# entire snapshot against the destination by primary_key.  Both absent-row
# retirement AND content-change close-out are handled by dlt SCD2.
# boundary_timestamp defaults to _dlt_loaded_at — source has no updated_at.
_WRITE_DISPOSITION: TScd2StrategyDict = {"disposition": "merge", "strategy": "scd2"}
_SCHEMA_CONTRACT: TSchemaContractDict = {"columns": "freeze"}


def patient_consents_resource(settings: Settings) -> DltResource:
    """Return a configured Regime A SCD2 dlt resource for the patient_consents table.

    Full SELECT on every run (no incremental cursor) so dlt sees the complete
    snapshot and can apply Regime A SCD2 close-out logic.

    No ``merge_key``: dlt uses ``primary_key="patient_id"`` to match rows between
    the snapshot and destination.  This handles both:
    - Absent-row retirement: ``_dlt_valid_to`` set when ``patient_id`` is missing
      from the snapshot (e.g. source DELETE).
    - Content-change close-out: prior row closed, new row inserted when consent
      flags change for an existing ``patient_id``.

    Regime B (``merge_key="patient_id"``) was tested and found NOT to retire
    absent rows for ``sql_table`` full-scan sources (Q.16 investigation); Regime A
    was confirmed to handle both close-out paths correctly.

    Args:
        settings: Runtime settings supplying the Postgres connection URL.

    Returns:
        DltResource ready to be included in an ``aidn_source()`` factory.
    """
    # No incremental= argument: sql_table performs a full SELECT on every run,
    # giving dlt the complete snapshot it needs to close retired consent rows.
    resource: DltResource = sql_table(
        credentials=str(settings.postgres_url),
        table="patient_consents",
        schema=settings.postgres_source_schema,
    )
    resource.apply_hints(
        primary_key="patient_id",
        write_disposition=_WRITE_DISPOSITION,
        schema_contract=_SCHEMA_CONTRACT,
    )
    resource.add_map(_validate_patient_consent)
    return resource
