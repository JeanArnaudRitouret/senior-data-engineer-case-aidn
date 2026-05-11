"""Integration test — Postgres logical replication prerequisites (item 1.27c).

Verifies that the Postgres container is configured with all prerequisites for
CDC-based ingest before any dlt resource is run.  Failures here indicate a
docker-compose or init.sql misconfiguration, not a pipeline bug.

Prerequisite: ``make up`` must have run (container running + init.sql applied).
Seed data is not required.
"""

from __future__ import annotations

from collections.abc import Generator

import psycopg2
import pytest

from aidn.config import Settings


@pytest.fixture
def pg_conn() -> Generator["psycopg2.extensions.connection", None, None]:
    """Open a regular (non-replication) psycopg2 connection from Settings.

    Yields:
        Open psycopg2 connection; auto-closed after the test.
    """
    settings = Settings()  # type: ignore[call-arg]
    conn = psycopg2.connect(str(settings.postgres_url))
    conn.autocommit = True
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


def test_postgres_logical_replication_prerequisites_exist(
    pg_conn: "psycopg2.extensions.connection",
) -> None:
    """Postgres must be configured for logical replication before pipeline runs.

    Assertions:
    1.  ``wal_level = logical`` — required for pg_replication CDC.
    2.  ``aidn_providers_pub`` publication exists and covers the ``providers`` table.
    3.  ``aidn_appointments_pub`` publication exists and covers the ``appointments`` table.
    4.  ``appointments`` has ``REPLICA IDENTITY FULL`` (``relreplident = 'f'``).
    5.  ``providers`` has ``REPLICA IDENTITY FULL`` (``relreplident = 'f'``).
    6.  The connected role has the REPLICATION privilege.

    Args:
        pg_conn: Open psycopg2 connection to the Postgres container.
    """
    with pg_conn.cursor() as cur:

        # 1. wal_level must be "logical"
        cur.execute("SHOW wal_level")
        wal_level: str = cur.fetchone()[0]  # type: ignore[index]
        assert wal_level == "logical", (
            f"wal_level is {wal_level!r}; expected 'logical'. "
            "Set '-c wal_level=logical' in docker-compose.yaml."
        )

        # 2. aidn_providers_pub covers providers
        cur.execute(
            "SELECT count(*) FROM pg_publication p "
            "JOIN pg_publication_tables pt ON pt.pubname = p.pubname "
            "WHERE p.pubname = %s AND pt.tablename = %s",
            ("aidn_providers_pub", "providers"),
        )
        providers_pub_count: int = cur.fetchone()[0]  # type: ignore[index]
        assert providers_pub_count == 1, (
            "Publication 'aidn_providers_pub' covering 'providers' not found. "
            "Run 'make seed' to apply init.sql."
        )

        # 3. aidn_appointments_pub covers appointments
        cur.execute(
            "SELECT count(*) FROM pg_publication p "
            "JOIN pg_publication_tables pt ON pt.pubname = p.pubname "
            "WHERE p.pubname = %s AND pt.tablename = %s",
            ("aidn_appointments_pub", "appointments"),
        )
        appointments_pub_count: int = cur.fetchone()[0]  # type: ignore[index]
        assert appointments_pub_count == 1, (
            "Publication 'aidn_appointments_pub' covering 'appointments' not found. "
            "Run 'make seed' to apply init.sql."
        )

        # 4. appointments has REPLICA IDENTITY FULL (relreplident = 'f')
        cur.execute(
            "SELECT relreplident FROM pg_class WHERE relname = %s AND relkind = 'r'",
            ("appointments",),
        )
        appt_row = cur.fetchone()
        assert appt_row is not None, "Table 'appointments' not found in pg_class"
        assert appt_row[0] == "f", (
            f"appointments.relreplident = {appt_row[0]!r}; expected 'f' (FULL). "
            "Run 'make seed' which applies 'ALTER TABLE appointments REPLICA IDENTITY FULL'."
        )

        # 5. providers has REPLICA IDENTITY FULL
        cur.execute(
            "SELECT relreplident FROM pg_class WHERE relname = %s AND relkind = 'r'",
            ("providers",),
        )
        prov_row = cur.fetchone()
        assert prov_row is not None, "Table 'providers' not found in pg_class"
        assert prov_row[0] == "f", (
            f"providers.relreplident = {prov_row[0]!r}; expected 'f' (FULL). "
            "Run 'make seed' which applies 'ALTER TABLE providers REPLICA IDENTITY FULL'."
        )

        # 6. Connected role has REPLICATION privilege
        cur.execute("SELECT rolreplication FROM pg_roles WHERE rolname = current_user")
        role_row = cur.fetchone()
        assert role_row is not None, "current_user not found in pg_roles"
        assert role_row[0] is True, (
            f"current_user does not have REPLICATION privilege (rolreplication=False). "
            "The pipeline requires a role with LOGIN and REPLICATION to create slots."
        )
