from __future__ import annotations

"""Unit tests for aidn.config — no live DB required."""

import pytest

from aidn.config import Settings


@pytest.mark.parametrize("insecure_mode", ["disable", "allow", "prefer"])
def test_ingest_refuses_insecure_sslmode(
    monkeypatch: pytest.MonkeyPatch, insecure_mode: str
) -> None:
    monkeypatch.setenv(
        "POSTGRES_URL",
        f"postgresql://postgres:dev@localhost:5432/aidn?sslmode={insecure_mode}",
    )
    monkeypatch.setenv(
        "POSTGRES_REPL_URL",
        "postgresql://postgres:dev@localhost:5432/aidn?sslmode=require",
    )
    monkeypatch.setenv("DUCKDB_PATH", "./aidn.duckdb")
    monkeypatch.setenv("DLT_DATA_DIR", "./.dlt")

    with pytest.raises(ValueError, match="sslmode"):
        Settings()
