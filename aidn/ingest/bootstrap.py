"""Initial-snapshot bootstrap — creates per-table replication slots and loads pre-existing rows.

The snapshot resource returned by ``init_replication(persist_snapshots=True)`` must be
consumed in the same process invocation via ``pipeline.run(snapshot)`` — do not store
and replay across process boundaries (the exported-snapshot handle is ephemeral).
"""

from __future__ import annotations

import logging

import psycopg2  # type: ignore[import-untyped]
from dlt.extract import DltResource
from dlt.sources.credentials import ConnectionStringCredentials

from aidn.config import Settings
from pg_replication.helpers import init_replication

logger = logging.getLogger(__name__)

# Shared publication; covers all CDC tables via a single pub created in init.sql.
_PUB_NAME: str = "aidn_cdc_pub"

# One slot per CDC table — independent confirmed_flush_lsn, independent cadence.
# Each tuple is (slot_name, table_name). Import this constant in cli.py to drive
# the bootstrap loop without duplicating the table list.
CDC_TABLES: tuple[tuple[str, str], ...] = (
    ("aidn_providers_slot", "providers"),
    ("aidn_appointments_slot", "appointments"),
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
        schema_name="public",
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

    logger.info(
        "bootstrap_snapshot_ready slot_name=%s table=%s",
        slot_name,
        table_name,
    )
    return resource
