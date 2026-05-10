"""CLI entrypoint — argument parsing and dispatch only; no business logic."""

from __future__ import annotations

import argparse
import uuid
from collections.abc import Sequence
from dataclasses import dataclass

from aidn.ingest.pipeline import run_pipeline
from aidn.logging_setup import bind_run_id, configure_logging, get_logger

# Canonical resource names — kept here until aidn_source() is wired in item 1.20.
_RESOURCE_NAMES: tuple[str, ...] = (
    "providers",
    "appointments",
    "patients",
    "patient_consents",
)

_logger = get_logger(__name__)


@dataclass
class _Args:
    """Parsed CLI arguments.

    Attributes:
        subcommand: Top-level subcommand name (e.g. ``"ingest"``).
        dry_run: When True, log targets without writing to the destination.
        table: Restrict the run to this resource name; None means all resources.
    """

    subcommand: str
    dry_run: bool
    table: str | None


def parse_args(argv: Sequence[str] | None = None) -> _Args:
    """Parse CLI arguments into a typed namespace.

    Args:
        argv: Argument list; defaults to ``sys.argv[1:]`` when None.

    Returns:
        Typed argument object for the selected subcommand.
    """
    parser = argparse.ArgumentParser(
        prog="aidn",
        description="Aidn data pipeline runner.",
    )
    sub = parser.add_subparsers(dest="subcommand", required=True)

    ingest_p = sub.add_parser("ingest", help="Run the ingest pipeline.")
    ingest_p.add_argument(
        "--dry-run",
        action="store_true",
        help="Log target resources without writing to the destination.",
    )
    ingest_p.add_argument(
        "--table",
        metavar="<name>",
        help="Ingest a single resource by name.",
    )

    ns = parser.parse_args(argv)
    return _Args(
        subcommand=ns.subcommand,
        dry_run=ns.dry_run,
        table=ns.table,
    )


def main(argv: Sequence[str] | None = None) -> None:
    """Dispatch CLI subcommands to pipeline functions.

    Args:
        argv: Argument list; defaults to ``sys.argv[1:]`` when None.
    """
    args = parse_args(argv)

    if args.subcommand == "ingest":
        _run_ingest(dry_run=args.dry_run, table=args.table)


def _run_ingest(*, dry_run: bool, table: str | None) -> None:
    """Dispatch the ingest subcommand to run_pipeline or the dry-run branch.

    Args:
        dry_run: When True, print target resource names and return without loading.
        table: Restrict the run to this resource; None means all resources.
    """
    targets: tuple[str, ...] = (table,) if table is not None else _RESOURCE_NAMES

    if dry_run:
        for name in targets:
            print(name)  # noqa: T201 — intentional user-facing dry-run output
        return

    from aidn.config import Settings  # deferred: avoids env-var load on --dry-run

    settings = Settings()  # type: ignore[call-arg]  # fields loaded from env
    configure_logging(settings.log_level)
    run_logger = bind_run_id(_logger, str(uuid.uuid4()))

    # aidn_source() is wired in item 1.20; this import raises AttributeError
    # until that item is implemented — acceptable since the live path is not
    # tested until 1.21.
    from aidn.ingest import pipeline as _pipeline  # noqa: PLC0415

    source = _pipeline.aidn_source()  # type: ignore[attr-defined]
    if table is not None:
        source = source.with_resources(table)

    run_pipeline(source, settings=settings, run_logger=run_logger)
