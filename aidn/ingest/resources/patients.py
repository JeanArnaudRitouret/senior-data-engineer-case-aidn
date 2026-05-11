"""Incremental append resource for the patients table — SQL polling via sql_table (Q21 + Q22).

No hard_delete, no is_deleted, no pk_snapshots: patients accepts no row-level
deletes in normal pipeline operation. The sole removal path is the Phase 5.5
GDPR Art. 17 erasure sweep (anonymize PII; retain patient_id). See Q21 + Q22.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Literal

import dlt
from dlt.common.schema.typing import TSchemaContractDict
from dlt.extract import DltResource
from dlt.sources.sql_database import sql_table

from aidn.config import Settings
from aidn.ingest.validators import _validate_patient

# Five-minute lag tolerates clock skew and late-arriving rows under at-least-once
# delivery; keeps the cursor below the leading edge of the source write window.
# dlt Incremental.lag expects float (seconds), not timedelta.
_INCREMENTAL_LAG: timedelta = timedelta(minutes=5)
_WRITE_DISPOSITION: Literal["append"] = "append"
_SCHEMA_CONTRACT: TSchemaContractDict = {"columns": "freeze"}


def patients_resource(settings: Settings) -> DltResource:
    """Return a configured incremental append dlt resource for the patients table.

    Uses ``sql_table`` (SQLAlchemy SQL polling) with an incremental cursor on
    ``updated_at``. ``write_disposition`` is ``append`` — the source emits
    SCD2 history natively (multiple rows per ``patient_id``, ordered by
    ``updated_at``). Merging would destroy that history; append retains it
    per Q21 + dlt-standards Rule 1.

    No ``hard_delete``, no ``is_deleted``, no ``pk_snapshots``: patients has
    no row-level delete path in normal pipeline operation (Q22). The Phase 5.5
    GDPR erasure sweep is the sole removal path.

    Args:
        settings: Runtime settings supplying the Postgres connection URL.

    Returns:
        DltResource ready to be included in an ``aidn_source()`` factory.
    """
    resource: DltResource = sql_table(
        credentials=str(settings.postgres_url),
        table="patients",
        schema="public",
        incremental=dlt.sources.incremental("updated_at", lag=_INCREMENTAL_LAG.total_seconds()),
    )
    resource.apply_hints(
        write_disposition=_WRITE_DISPOSITION,
        schema_contract=_SCHEMA_CONTRACT,
    )
    resource.add_map(_validate_patient)
    return resource
