"""Full-snapshot SCD2 (no merge_key) for patient_consents.

Every run fetches the complete source table. dlt SCD2 handles two cases:
- Row disappears → _dlt_valid_to set (absent-row retirement).
- Flag changes → prior row closed, new row opened.
boundary_timestamp defaults to _dlt_loaded_at because the source has no
updated_at column. GDPR erasure runs as a separate hard-delete
operation; hard_delete and scd2 are intentionally not combined on the same
resource because their semantics are not jointly documented by dlt.
"""

from __future__ import annotations

from dlt.common.schema.typing import TSchemaContractDict, TScd2StrategyDict
from dlt.extract import DltResource
from dlt.sources.sql_database import sql_table

from aidn.config import Settings
from aidn.ingest.validators import _validate_patient_consent

# Full-snapshot SCD2 (no merge_key): dlt compares the entire snapshot against
# the destination by primary_key. Both absent-row retirement (row gone from
# source) and content-change close-out (flag flipped) are handled by dlt SCD2.
# boundary_timestamp defaults to _dlt_loaded_at — source has no updated_at column.
_WRITE_DISPOSITION: TScd2StrategyDict = {"disposition": "merge", "strategy": "scd2"}
_SCHEMA_CONTRACT: TSchemaContractDict = {"columns": "freeze"}


def patient_consents_resource(settings: Settings) -> DltResource:
    """Return a configured full-snapshot SCD2 dlt resource for the patient_consents table.

    Full SELECT on every run (no incremental cursor) so dlt sees the complete
    snapshot and can apply SCD2 close-out logic.

    No ``merge_key``: dlt uses ``primary_key="patient_id"`` to match rows between
    the snapshot and destination.  This handles both:
    - Absent-row retirement: ``_dlt_valid_to`` set when ``patient_id`` is missing
      from the snapshot (e.g. source DELETE).
    - Content-change close-out: prior row closed, new row inserted when consent
      flags change for an existing ``patient_id``.

    An alternative configuration using ``merge_key="patient_id"`` was tested and found
    NOT to retire absent rows when the source is a full ``sql_table`` scan; the no-merge_key
    configuration handles both absent-row retirement and content-change close-out correctly.

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
