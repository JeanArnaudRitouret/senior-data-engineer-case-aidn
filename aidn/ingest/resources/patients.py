"""CDC resource for the patients table — pg_replication merge; hard_delete on deleted_ts overridden to False.

Slot ``aidn_patients_slot`` is owned by ``aidn/ingest/bootstrap.py``.
This module consumes a pre-existing slot and must not call ``init_replication()``.

``name`` (direct identifier) is stripped from WAL events before Pydantic validation,
continuing the data-minimisation decision from the prior sql_table resource (Q40, P.2).
"""

from __future__ import annotations

from typing import Any

from dlt.extract import DltResource
from dlt.sources.credentials import ConnectionStringCredentials

from pg_replication import replication_resource

from aidn.config import Settings
from aidn.ingest.preprocess import _strip_name
from aidn.ingest.validators import _validate_patient

_SLOT_NAME: str = "aidn_patients_slot"
_PUB_NAME: str = "aidn_patients_pub"

# Column hints applied to the patients resource:
# - lsn, deleted_ts: CDC columns added by pg_replication absent from the source schema;
#   declared explicitly so schema_contract freeze accepts them on first run.
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



def patients_resource(settings: Settings) -> DltResource:
    """Return a configured CDC dlt resource for the patients table.

    Configures the pg_replication resource with:
    - ``write_disposition="merge"`` on ``lsn`` (WAL-unique per event; at-least-once dedup)
    - ``schema_contract={"columns": "freeze"}`` (unexpected source columns raise)
    - ``hard_delete=False`` on ``deleted_ts`` (raw row preserved on WAL delete; Q36)

    ``name`` (direct identifier) is stripped before Pydantic validation so it never
    enters raw — mirroring the ``excluded_columns=["name"]`` behaviour of the prior
    sql_table resource (Q40, P.2).

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
        table_name="patients",
        write_disposition="merge",
        primary_key="lsn",
        schema_contract={"columns": "freeze"},
        columns=_COLUMN_HINTS,
    )
    resource.add_map(_strip_name)
    resource.add_map(_validate_patient)
    return resource
