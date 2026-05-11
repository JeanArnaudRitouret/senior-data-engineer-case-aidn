from __future__ import annotations

"""Logging configuration — JSON formatter, run_id adapter, PII-safe filter."""

import logging
import sys

from pythonjsonlogger import jsonlogger

# Keys that carry patient PII and must never appear in plaintext in log output.
_PII_KEYS: frozenset[str] = frozenset({"name", "postcode"})

# Sentinel: prevents double-configuration if configure_logging() is called twice.
_CONFIGURED: bool = False


class PiiSafeFilter(logging.Filter):
    """Logging filter that redacts PII field values before emission.

    Attached to the root handler so every log record in the process is
    scrubbed, regardless of which logger emitted it.

    Attributes:
        (none beyond inherited Filter state)
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """Redact PII keys in the log record dict; always returns True.

        Args:
            record: The log record to inspect and potentially mutate.

        Returns:
            True — this filter never suppresses records; it only redacts values.
        """
        for key in _PII_KEYS:
            if key in record.__dict__:
                record.__dict__[key] = "<redacted>"
        return True


def configure_logging(level: str) -> None:
    """Configure root logger with JSON formatter and PII-safe filter.

    Idempotent — safe to call multiple times; subsequent calls are no-ops.

    Args:
        level: Python logging level name (e.g. ``"INFO"``, ``"DEBUG"``).
    """
    global _CONFIGURED
    if _CONFIGURED:
        return

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        jsonlogger.JsonFormatter(  # type: ignore[no-untyped-call]
            "%(asctime)s %(name)s %(levelname)s %(message)s"
        )
    )
    handler.addFilter(PiiSafeFilter())

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    root.addHandler(handler)
    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """Return a module-level logger for name.

    Args:
        name: Logger name — callers pass ``__name__``.

    Returns:
        Standard Logger instance.
    """
    return logging.getLogger(name)


def bind_run_id(
    logger: logging.Logger, run_id: str
) -> logging.LoggerAdapter[logging.Logger]:
    """Return a LoggerAdapter that injects run_id into every log record.

    Args:
        logger: Base logger to wrap.
        run_id: Pipeline run identifier; emitted as ``run_id=`` on every record.

    Returns:
        LoggerAdapter pre-loaded with ``{"run_id": run_id}``.
    """
    return logging.LoggerAdapter(logger, extra={"run_id": run_id})
