"""End-to-end: the CLI with a mocked API, for both stores."""

import os

import httpx
import pytest
import respx

from pipeline.run import main
from pipeline.sources.openbesluitvorming import BASE_URL

DB_URL = os.environ.get("OPEN_HAGUE_TEST_DATABASE_URL")


def mock_api(obv_fixture):
    respx.get(f"{BASE_URL}/api/sources").mock(
        return_value=httpx.Response(200, content=obv_fixture("sources.json"))
    )
    respx.get(f"{BASE_URL}/api/export/snapshot").mock(
        return_value=httpx.Response(
            200,
            content=obv_fixture("snapshot_page1.ndjson"),
            headers={"X-Has-More": "false", "X-Changes-Cursor": "chg-0"},
        )
    )
    respx.get(f"{BASE_URL}/api/export/changes").mock(
        return_value=httpx.Response(200, content=b"", headers={"X-Has-More": "false"})
    )


@pytest.fixture(autouse=True)
def fast_and_contactable(monkeypatch):
    monkeypatch.setattr("pipeline.run.MIN_INTERVAL_FLOOR", 0.0)
    monkeypatch.setenv("OPEN_HAGUE_MIN_INTERVAL", "0")
    monkeypatch.setenv("OPEN_HAGUE_CONTACT", "https://example.org/contact")


def test_cli_requires_contact(monkeypatch):
    monkeypatch.delenv("OPEN_HAGUE_CONTACT")
    with pytest.raises(SystemExit):
        main(["--source", "openbesluitvorming", "--store", "jsonl"])


@respx.mock
def test_cli_jsonl(tmp_path, monkeypatch, obv_fixture):
    mock_api(obv_fixture)
    monkeypatch.setenv("OPEN_HAGUE_DATA_DIR", str(tmp_path))
    monkeypatch.delenv("DATABASE_URL", raising=False)
    assert main(["--source", "openbesluitvorming"]) == 0
    out = tmp_path / "entities" / "openbesluitvorming--example-den-haag.jsonl"
    assert len(out.read_text().splitlines()) == 2
    state = (tmp_path / "state" / "openbesluitvorming--example-den-haag.json").read_text()
    assert '"chg-0"' in state


@pytest.mark.db
@pytest.mark.skipif(not DB_URL, reason="OPEN_HAGUE_TEST_DATABASE_URL not set")
@respx.mock
def test_cli_postgres(tmp_path, monkeypatch, obv_fixture):
    import uuid

    import psycopg

    schema = f"t_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(DB_URL, autocommit=True) as admin:
        admin.execute(f"CREATE SCHEMA {schema}")
    sep = "&" if "?" in DB_URL else "?"
    url = f"{DB_URL}{sep}options=-csearch_path%3D{schema}"
    try:
        mock_api(obv_fixture)
        monkeypatch.setenv("DATABASE_URL", url)
        monkeypatch.setenv("OPEN_HAGUE_DATA_DIR", str(tmp_path))
        assert main(["--source", "openbesluitvorming"]) == 0
        assert main(["--source", "openbesluitvorming"]) == 0  # second run resumes, no errors
        with psycopg.connect(url) as conn:
            assert conn.execute("SELECT count(*) FROM entity").fetchone()[0] == 2
            phase = conn.execute("SELECT phase, cursor FROM sync_state").fetchone()
            assert phase == ("changes", "chg-0")
    finally:
        with psycopg.connect(DB_URL, autocommit=True) as admin:
            admin.execute(f"DROP SCHEMA {schema} CASCADE")
