"""CLI entrypoint — argument parsing and dispatch only; no business logic."""

from __future__ import annotations

import argparse
import uuid
from collections.abc import Sequence
from dataclasses import dataclass

from aidn.ingest.pipeline import run_pipeline
from aidn.logging_setup import bind_run_id, configure_logging, get_logger

# Canonical resource names used for dry-run output and --table validation.
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
        subcommand: Top-level subcommand name (``"ingest"`` or ``"bootstrap"``).
        dry_run: When True, log targets without writing to the destination.
        table: Restrict the ingest run to this resource name; None means all resources.
            Always None for the ``bootstrap`` subcommand.
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

    bootstrap_p = sub.add_parser(
        "bootstrap",
        help="Create per-table replication slots and load the initial snapshot.",
    )
    bootstrap_p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print CDC table names without creating slots or writing to the destination.",
    )
    bootstrap_p.set_defaults(table=None)

    ns = parser.parse_args(argv)
    return _Args(
        subcommand=ns.subcommand,
        dry_run=ns.dry_run,
        table=getattr(ns, "table", None),
    )


def main(argv: Sequence[str] | None = None) -> None:
    """Dispatch CLI subcommands to pipeline functions.

    Args:
        argv: Argument list; defaults to ``sys.argv[1:]`` when None.
    """
    args = parse_args(argv)

    if args.subcommand == "ingest":
        _run_ingest(dry_run=args.dry_run, table=args.table)
    elif args.subcommand == "bootstrap":
        _run_bootstrap(dry_run=args.dry_run)


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

    from aidn.ingest.pipeline import aidn_source  # noqa: PLC0415

    source = aidn_source(settings)
    if table is not None:
        source = source.with_resources(table)

    run_pipeline(source, settings=settings, run_logger=run_logger)


def _run_bootstrap(*, dry_run: bool) -> None:
    """Dispatch the bootstrap subcommand: create slots and load initial snapshots.

    Args:
        dry_run: When True, print CDC table names and return without touching
            Postgres or the DuckDB destination.
    """
    from aidn.ingest.bootstrap import CDC_TABLES, bootstrap_table  # deferred

    if dry_run:
        for _, table_name, _pk, _pub in CDC_TABLES:
            print(table_name)  # noqa: T201 — intentional user-facing dry-run output
        return

    from aidn.config import Settings  # deferred: avoids env-var load on --dry-run
    from dlt.pipeline.exceptions import PipelineStepFailed

    from aidn.ingest.pipeline import make_pipeline

    settings = Settings()  # type: ignore[call-arg]
    configure_logging(settings.log_level)
    boot_logger = bind_run_id(_logger, str(uuid.uuid4()))

    pipeline = make_pipeline(settings)

    for slot_name, table_name, primary_key, pub_name in CDC_TABLES:
        snapshot = bootstrap_table(slot_name, table_name, primary_key, pub_name, settings)
        if snapshot is None:
            boot_logger.info(
                "bootstrap_noop table=%s reason=slot_exists",
                table_name,
            )
            continue

        try:
            info = pipeline.run(snapshot)
        except PipelineStepFailed as exc:
            boot_logger.error(
                "bootstrap_load_failed table=%s step=%s",
                table_name,
                exc.step,
                exc_info=True,
            )
            raise

        ids = info.loads_ids
        if len(ids) == 0:
            boot_logger.info(
                "bootstrap_noop table=%s reason=no_new_rows",
                table_name,
            )
        elif len(ids) == 1:
            boot_logger.info(
                "bootstrap_complete table=%s load_id=%s",
                table_name,
                ids[0],
            )
        else:
            boot_logger.error(
                "bootstrap_multi_package table=%s run_ids=%s",
                table_name,
                ids,
            )
            raise RuntimeError(
                f"Bootstrap produced unexpected multi-package run for {table_name!r}: {ids}"
            )
