"""Per-table row preprocessors applied at the dlt extraction boundary, before Pydantic validation.

Preprocessors strip or transform columns that are present in the source but absent from (or
forbidden by) the destination Pydantic model.  Each preprocessor is a plain ``dict → dict``
function so it can be chained via ``dlt.Resource.add_map`` on both the CDC resource and the
bootstrap snapshot resource without duplication.

Registry:
    TABLE_PREPROCESSORS — maps table name → preprocessor function.
    Only tables that require preprocessing have entries; tables with no entry are a no-op.
"""

from __future__ import annotations

from typing import Any, Callable


def _strip_name(row: dict[str, Any]) -> dict[str, Any]:
    """Remove ``name`` from a patient WAL event or snapshot dict.

    ``patients.name`` is a direct identifier excluded at all pipeline boundaries (Q40, P.2).
    ``pg_replication`` includes every source column in WAL events and in the exported snapshot
    rows; stripping here prevents the Patient model's ``extra="forbid"`` from rejecting them.

    Args:
        row: Raw event or snapshot dict from pg_replication.

    Returns:
        The same dict with ``name`` removed; no-op when ``name`` is absent.
    """
    row.pop("name", None)
    return row


TABLE_PREPROCESSORS: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
    "patients": _strip_name,
}
