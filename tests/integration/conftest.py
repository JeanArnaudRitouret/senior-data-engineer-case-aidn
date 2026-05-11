"""Shared fixtures for integration tests that talk to the live Postgres container."""

from __future__ import annotations

import psycopg2
import pytest

from aidn.config import Settings
from aidn.ingest.bootstrap import CDC_TABLES


@pytest.fixture(autouse=True)
def drop_cdc_slots() -> None:
    """Drop any pre-existing CDC replication slots before each integration test.

    Ensures a clean baseline when the Postgres container has leftover slots from
    a prior ``make bootstrap`` run or a previous test execution.
    """
    settings = Settings()  # type: ignore[call-arg]
    dsn = str(settings.postgres_repl_url)
    slot_names = [slot_name for slot_name, *_ in CDC_TABLES]
    with psycopg2.connect(dsn) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            for slot_name in slot_names:
                cur.execute(
                    "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
                    (slot_name,),
                )
                if cur.fetchone() is not None:
                    cur.execute(
                        "SELECT pg_drop_replication_slot(%s)", (slot_name,)
                    )
