"""CDC resource for the providers table — pg_replication merge; hard_delete on deleted_ts overridden to False.

Slot ``aidn_providers_slot`` is owned by ``aidn/ingest/bootstrap.py``.
This module consumes a pre-existing slot and must not call ``init_replication()``.
"""

from __future__ import annotations

from typing import Any

import pydantic
from dlt.extract import DltResource
from dlt.sources.credentials import ConnectionStringCredentials

from pg_replication import replication_resource

from aidn.config import Settings
from aidn.logging_setup import get_logger

_logger = get_logger(__name__)

# Replication slot dedicated to the providers table; separate from the
# appointments slot so each resource consumes WAL independently.
_SLOT_NAME: str = "aidn_providers_slot"
_PUB_NAME: str = "aidn_cdc_pub"

# CDC columns added by pg_replication that are absent from the source schema;
# declared explicitly so schema_contract freeze accepts them on first run.
_CDC_COLUMNS: dict[str, Any] = {
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


def _validate_provider(row: dict[str, Any]) -> Any:
    """Validate a single row against the Provider model (two-tier exception handler).

    Tier 1 (ValidationError | KeyError): log warning with pseudonymous entity_id;
    return None to drop the row (dlt add_map drops None items).
    Tier 2 (any other Exception): log error with exc_info; re-raise.

    Args:
        row: Raw dict from the pg_replication CDC stream (deleted_ts is None
            for live rows, non-null for WAL DELETE events).

    Returns:
        Validated Provider model instance, or None if validation fails.
    """
    try:
        from aidn.models.ingest import Provider  # deferred to avoid circular scan

        return Provider.model_validate(row)
    except (pydantic.ValidationError, KeyError) as e:
        _logger.warning(
            "row_dropped table=providers reason=%s entity_id=%s",
            type(e).__name__,
            row.get("provider_id"),
        )
        return None
    except Exception:
        _logger.error("row_failed table=providers", exc_info=True)
        raise


def providers_resource(settings: Settings) -> DltResource:
    """Return a configured CDC dlt resource for the providers table.

    Configures the pg_replication resource with:
    - ``write_disposition="merge"`` on ``provider_id`` (idempotent upsert)
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
        table_name="providers",
        write_disposition="merge",
        primary_key="provider_id",
        schema_contract={"columns": "freeze"},
        columns=_CDC_COLUMNS,
    )
    resource.add_map(_validate_provider)
    return resource
