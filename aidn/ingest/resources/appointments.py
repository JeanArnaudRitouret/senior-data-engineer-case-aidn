"""CDC resource for the appointments table — pg_replication merge; hard_delete on deleted_ts overridden to False.

Slot ``aidn_appointments_slot`` is owned by ``aidn/ingest/bootstrap.py``.
This module consumes a pre-existing slot and must not call ``init_replication()``.
"""

from __future__ import annotations

from typing import Any

from dlt.extract import DltResource
from dlt.sources.credentials import ConnectionStringCredentials

from pg_replication import replication_resource

from aidn.config import Settings
from aidn.ingest.validators import _validate_appointment

# Replication slot dedicated to the appointments table; separate from the
# providers slot so each resource consumes WAL independently.
_SLOT_NAME: str = "aidn_appointments_slot"
_PUB_NAME: str = "aidn_appointments_pub"

# Column hints applied to the appointments resource:
# - lsn, deleted_ts: CDC columns added by pg_replication absent from the source schema;
#   declared explicitly so schema_contract freeze accepts them on first run.
# - lsn carries dedup_sort="asc" set by pg_replication internally; it is the WAL
#   ordering key and the correct at-least-once dedup tie-break for event_id (WAL-level
#   duplicates always differ by lsn, not by ingested_at). Do not add a second
#   dedup_sort column — dlt allows exactly one per table.
_COLUMN_HINTS: dict[str, Any] = {
    "lsn": {"data_type": "bigint", "nullable": True},
    "deleted_ts": {
        # Override pg_replication's default hard_delete=True (Q36): preserve the
        # raw row even when the source row is physically deleted; deleted_ts IS
        # NOT NULL is the sole delete signal at the raw boundary.
        "hard_delete": False,
        "data_type": "timestamp",
        "nullable": True,
    },
}


def appointments_resource(settings: Settings) -> DltResource:
    """Return a configured CDC dlt resource for the appointments table.

    Configures the pg_replication resource with:
    - ``write_disposition="merge"`` on ``event_id`` (at-least-once dedup)
    - ``dedup_sort`` on ``lsn`` (set by pg_replication internally; WAL ordering key)
    - ``schema_contract={"columns": "freeze"}`` (unexpected source columns raise)
    - ``hard_delete=False`` on ``deleted_ts`` (raw row preserved on WAL delete; Q36)

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
        table_name="appointments",
        write_disposition="merge",
        primary_key="event_id",
        schema_contract={"columns": "freeze"},
        columns=_COLUMN_HINTS,
    )
    resource.add_map(_validate_appointment)
    return resource
