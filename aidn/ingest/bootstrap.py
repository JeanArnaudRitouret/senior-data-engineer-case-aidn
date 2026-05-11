"""Initial-snapshot bootstrap — creates per-table replication slots and loads pre-existing rows.

The snapshot resource returned by ``init_replication(persist_snapshots=True)`` must be
consumed in the same process invocation via ``pipeline.run(snapshot)`` — do not store
and replay across process boundaries (the exported-snapshot handle is ephemeral).

Each snapshot resource is augmented before use:
- ``apply_hints(columns=_SNAPSHOT_CDC_COLUMNS)`` pre-declares ``lsn`` and ``deleted_ts``
  so the destination schema is stable post-bootstrap (columns appear even before the first
  WAL event).
- ``add_map(TABLE_VALIDATORS[table_name])`` applies the same Pydantic two-tier validator
  the steady-state CDC resource uses, closing the validity-contract divergence.
"""

from __future__ import annotations

import logging
from typing import Any

import psycopg2
from dlt.extract import DltResource
from dlt.sources.credentials import ConnectionStringCredentials

from aidn.config import Settings
from aidn.ingest.preprocess import TABLE_PREPROCESSORS
from aidn.ingest.validators import TABLE_VALIDATORS
from pg_replication.helpers import init_replication

logger = logging.getLogger(__name__)

# CDC columns pre-declared on the snapshot resource so the destination schema is
# stable before any WAL event flows. hard_delete is omitted here — snapshot rows
# carry no delete signal; the steady-state CDC resource applies hard_delete=False
# so WAL DELETE events preserve the row rather than removing it from raw.
_SNAPSHOT_CDC_COLUMNS: dict[str, Any] = {
    "lsn": {"data_type": "bigint", "nullable": True},
    "deleted_ts": {"data_type": "timestamp", "nullable": True},
}

# One slot per CDC table — independent confirmed_flush_lsn, independent cadence.
# Each tuple is (slot_name, table_name, snapshot_primary_key, pub_name).
# snapshot_primary_key=None signals write_disposition="append" for the bootstrap snapshot;
# use this when the CDC primary key (e.g. lsn) is NULL on all snapshot rows, which would
# collapse every row to one under a merge disposition.
CDC_TABLES: tuple[tuple[str, str, str | None, str], ...] = (
    ("aidn_providers_slot", "providers", "provider_id", "aidn_providers_pub"),
    ("aidn_appointments_slot", "appointments", "event_id", "aidn_appointments_pub"),
    # patients: lsn is the CDC merge key (WAL-unique), but snapshot rows have lsn=NULL.
    # None → bootstrap uses write_disposition="append" so all 68 seed rows land intact.
    ("aidn_patients_slot", "patients", None, "aidn_patients_pub"),
    # patient_consents: lsn is NULL on snapshot rows → append disposition for bootstrap.
    ("aidn_patient_consents_slot", "patient_consents", None, "aidn_patient_consents_pub"),
)


def _slot_exists(slot_name: str, dsn: str) -> bool:
    """Return True if a replication slot with the given name already exists.

    Args:
        slot_name: Name of the replication slot to check.
        dsn: Postgres connection string; replication-role credentials are sufficient
            because ``pg_replication_slots`` is readable by roles with REPLICATION.

    Returns:
        True if the slot is present in ``pg_replication_slots``, False otherwise.
    """
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
                (slot_name,),
            )
            return cur.fetchone() is not None


def bootstrap_table(
    slot_name: str,
    table_name: str,
    primary_key: str | None,
    pub_name: str,
    settings: Settings,
) -> DltResource | None:
    """Create a replication slot and return the initial-snapshot resource.

    Idempotent: if the slot already exists the function logs a skip and returns
    ``None``. The pre-check is mandatory because ``init_replication`` raises
    ``RuntimeError`` when called with ``persist_snapshots=True`` on an existing slot.

    The caller is responsible for running the returned resource through
    ``pipeline.run(snapshot)`` in the same process invocation.

    Args:
        slot_name: Logical-replication slot name (unique per table).
        table_name: Postgres table to bootstrap; must be included in ``pub_name``.
        primary_key: Column to use as the snapshot merge key, or ``None`` when the
            CDC primary key is NULL on all snapshot rows (e.g. ``lsn`` for patients).
            ``None`` → snapshot uses ``write_disposition="append"`` so every source
            row lands without NULL-key dedup collapse.
        pub_name: Per-table publication name (e.g. ``aidn_providers_pub``).
        settings: Runtime config supplying the replication connection URL.

    Returns:
        A ``DltResource`` representing the exported snapshot, or ``None`` when the
        slot pre-existed (idempotent no-op path).
    """
    dsn = str(settings.postgres_repl_url)

    if _slot_exists(slot_name, dsn):
        logger.info(
            "bootstrap_skip reason=slot_exists slot_name=%s table=%s",
            slot_name,
            table_name,
        )
        return None

    creds = ConnectionStringCredentials(dsn)
    result = init_replication(
        slot_name=slot_name,
        pub_name=pub_name,
        schema_name=settings.postgres_source_schema,
        table_names=[table_name],
        credentials=creds,
        publish="insert,update,delete",
        persist_snapshots=True,
    )

    if result is None:
        # Unreachable: slot-exists guard above ensures this is a first call.
        raise RuntimeError(
            f"init_replication returned None for newly created slot {slot_name!r}"
        )

    resource: DltResource = result[0] if isinstance(result, list) else result

    if primary_key is None:
        resource.apply_hints(write_disposition="append", columns=_SNAPSHOT_CDC_COLUMNS)
    else:
        resource.apply_hints(primary_key=primary_key, columns=_SNAPSHOT_CDC_COLUMNS)
    if table_name in TABLE_PREPROCESSORS:
        resource.add_map(TABLE_PREPROCESSORS[table_name])
    resource.add_map(TABLE_VALIDATORS[table_name])

    logger.info(
        "bootstrap_snapshot_ready slot_name=%s table=%s",
        slot_name,
        table_name,
    )
    return resource
