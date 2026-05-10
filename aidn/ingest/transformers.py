from __future__ import annotations

"""dlt transformer factory — Pydantic v2 validation at the dlt/raw boundary."""

from collections.abc import Callable, Iterable, Iterator
from typing import Any, TypeVar

import pydantic

from aidn.logging_setup import get_logger

logger = get_logger(__name__)

T = TypeVar("T", bound=pydantic.BaseModel)


def validate(
    model: type[T],
    *,
    table: str,
    pk: str,
) -> Callable[[Iterable[dict[str, Any]]], Iterator[T]]:
    """Return a transformer that validates each dict row against model.

    Two-tier exception handler (dlt-standards Rule 12):

    - **Tier 1** ``ValidationError | KeyError`` — WARNING + skip row; increments
      ``rows_dropped``.  The pseudonymous ``entity_id`` (pk value) is logged;
      field values are never logged (privacy-consent Rule 5).
    - **Tier 2** any other ``Exception`` — ERROR with ``exc_info=True`` + re-raise.

    Emits one ``INFO`` summary per batch: ``rows_in``, ``rows_dropped``, ``reason``.

    Args:
        model: Pydantic v2 model class to validate each row against.
        table: Destination table name; used in log context.
        pk: Primary key column name; its value is logged as pseudonymous
            ``entity_id`` — never field values.

    Returns:
        Generator function that accepts an iterable of raw dicts and yields
        validated model instances; drops and warns on validation failures.
    """

    def _transform(rows: Iterable[dict[str, Any]]) -> Iterator[T]:
        rows_in: int = 0
        rows_dropped: int = 0

        for row in rows:
            rows_in += 1
            try:
                yield model.model_validate(row)
            except (pydantic.ValidationError, KeyError) as e:
                rows_dropped += 1
                logger.warning(
                    "row_dropped table=%s reason=%s entity_id=%s",
                    table,
                    type(e).__name__,
                    row.get(pk),
                )
            except Exception:
                logger.error("row_failed table=%s", table, exc_info=True)
                raise

        drop_reason = "validation_error" if rows_dropped else "none"
        logger.info(
            "transform_complete table=%s rows_in=%d rows_dropped=%d reason=%s",
            table,
            rows_in,
            rows_dropped,
            drop_reason,
        )

    return _transform
