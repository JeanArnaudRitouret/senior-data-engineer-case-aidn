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

import psycopg2  # type: ignore[import-untyped]
from dlt.extract import DltResource
from dlt.sources.credentials import ConnectionStringCredentials

from aidn.config import Settings
from aidn.ingest.validators import TABLE_VALIDATORS
from pg_replication.helpers import init_replication

logger = logging.getLogger(__name__)

# Shared publication; covers all CDC tables via a single pub created in init.sql.
_PUB_NAME: str = "aidn_cdc_pub"

# CDC columns pre-declared on the snapshot resource so the destination schema is
# stable post-bootstrap. Omits hard_delete — snapshot rows always have NULL here;
# the steady-state CDC resource (_CDC_COLUMNS in providers.py) carries hard_delete=False
# per Q36, which is meaningful only at the merge step.
_SNAPSHOT_CDC_COLUMNS: dict[str, Any] = {
    "lsn": {"data_type": "bigint", "nullable": True},
    "deleted_ts": {"data_type": "timestamp", "nullable": True},
}

# One slot per CDC table — independent confirmed_flush_lsn, independent cadence.
# Each tuple is (slot_name, table_name, primary_key). Import this constant in cli.py
# to drive the bootstrap loop without duplicating the table list.
CDC_TABLES: tuple[tuple[str, str, str], ...] = (
    ("aidn_providers_slot", "providers", "provider_id"),
    ("aidn_appointments_slot", "appointments", "event_id"),
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
    primary_key: str,
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
        table_name: Postgres table to bootstrap; must be included in ``aidn_cdc_pub``.
        primary_key: Primary key column name; passed to ``apply_hints`` so the merge
            disposition has a key to upsert on during the initial snapshot load.
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
        pub_name=_PUB_NAME,
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

    resource.apply_hints(primary_key=primary_key, columns=_SNAPSHOT_CDC_COLUMNS)
    resource.add_map(TABLE_VALIDATORS[table_name])

    logger.info(
        "bootstrap_snapshot_ready slot_name=%s table=%s",
        slot_name,
        table_name,
    )
    return resource
