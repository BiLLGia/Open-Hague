"""PostgreSQL integration tests. Skipped unless OPEN_HAGUE_TEST_DATABASE_URL is set.
Each test runs in its own throwaway schema."""

import os
import uuid
from datetime import UTC, datetime

import pytest

from pipeline.raw_store import RawStore
from pipeline.sources.openbesluitvorming import SyncState, parse_ndjson
from pipeline.store import PageMeta, PostgresStore, connect

psycopg = pytest.importorskip("psycopg")
URL = os.environ.get("OPEN_HAGUE_TEST_DATABASE_URL")
pytestmark = [
    pytest.mark.db,
    pytest.mark.skipif(not URL, reason="OPEN_HAGUE_TEST_DATABASE_URL not set"),
]
KEY = "example-den-haag"


@pytest.fixture
def schema_url():
    """A URL whose search_path points at a fresh, migrated schema."""
    from pipeline.db.migrate import migrate

    schema = f"t_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(URL, autocommit=True) as admin:
        admin.execute(f"CREATE SCHEMA {schema}")
    sep = "&" if "?" in URL else "?"
    url = f"{URL}{sep}options=-csearch_path%3D{schema}"
    try:
        with connect(url) as connection:
            assert migrate(connection) == ["001_init.sql"]
            assert migrate(connection) == []  # idempotent
        yield url
    finally:
        with psycopg.connect(URL, autocommit=True) as admin:
            admin.execute(f"DROP SCHEMA {schema} CASCADE")


@pytest.fixture
def conn(schema_url):
    with connect(schema_url) as connection:
        yield connection


def commit(store, tmp_path, records, state, body=b"page"):
    raw = RawStore(tmp_path).put("openbesluitvorming", body)
    meta = PageMeta("openbesluitvorming", KEY, "https://x/page", datetime.now(UTC), raw)
    store.commit_page(meta, records, state)


def rows(conn):
    return {
        r[0].split(":")[-1]: r[1:]
        for r in conn.execute(
            "SELECT entity_id, title, content_hash, deleted, seq FROM entity ORDER BY entity_id"
        )
    }


def test_upsert_update_and_delete(conn, tmp_path, obv_fixture):
    store = PostgresStore(conn)
    page1, _ = parse_ndjson(obv_fixture("snapshot_page1.ndjson"))
    page2, _ = parse_ndjson(obv_fixture("snapshot_page2.ndjson"))
    changes, _ = parse_ndjson(obv_fixture("changes_page1.ndjson"))

    commit(store, tmp_path, page1, SyncState("snapshot", "cur-2", "chg-0"), b"p1")
    commit(store, tmp_path, page2, SyncState("changes", "chg-0"), b"p2")
    assert len(rows(conn)) == 4
    assert store.load_state("openbesluitvorming", KEY) == SyncState("changes", "chg-0")

    commit(store, tmp_path, changes, SyncState("changes", "chg-1"), b"c1")
    result = rows(conn)
    assert result["5001"][0].endswith("(gewijzigd)")
    assert result["5001"][1] == "sha256:aaa1-v2"
    assert result["8001"][2] is True  # deleted
    assert store.load_state("openbesluitvorming", KEY) == SyncState("changes", "chg-1")


def test_replaying_older_records_does_not_overwrite_newer(conn, tmp_path, obv_fixture):
    store = PostgresStore(conn)
    page1, _ = parse_ndjson(obv_fixture("snapshot_page1.ndjson"))
    changes, _ = parse_ndjson(obv_fixture("changes_page1.ndjson"))
    commit(store, tmp_path, changes[:1], SyncState("changes", "chg-1"), b"c1")  # seq 105
    commit(store, tmp_path, page1, SyncState("changes", "chg-1"), b"p1")  # seq 101, older
    assert rows(conn)["5001"][1] == "sha256:aaa1-v2"


def test_raw_file_row_and_provenance(conn, tmp_path, obv_fixture):
    store = PostgresStore(conn)
    page1, _ = parse_ndjson(obv_fixture("snapshot_page1.ndjson"))
    commit(store, tmp_path, page1, SyncState("changes", "c"), b"p1")
    missing = conn.execute(
        "SELECT count(*) FROM entity e LEFT JOIN raw_file r ON r.sha256 = e.raw_sha256"
        " WHERE r.sha256 IS NULL OR e.retrieved_at IS NULL OR e.content_hash IS NULL"
    ).fetchone()[0]
    assert missing == 0


def test_failed_page_does_not_advance_state(schema_url, conn, tmp_path, obv_fixture):
    store = PostgresStore(conn)
    page1, _ = parse_ndjson(obv_fixture("snapshot_page1.ndjson"))
    commit(store, tmp_path, page1, SyncState("snapshot", "cur-2", "chg-0"), b"p1")
    bad = page1[0].__class__(**{**page1[0].__dict__, "entity_type": None})  # violates NOT NULL
    with pytest.raises(psycopg.errors.NotNullViolation):
        commit(store, tmp_path, [bad], SyncState("changes", "chg-0"), b"p2")
    assert store.load_state("openbesluitvorming", KEY) == SyncState("snapshot", "cur-2", "chg-0")
    # Checked from a second connection: the first page is durably committed on its own.
    with connect(schema_url) as other:
        assert PostgresStore(other).load_state("openbesluitvorming", KEY) == SyncState(
            "snapshot", "cur-2", "chg-0"
        )
        assert other.execute("SELECT count(*) FROM entity").fetchone()[0] == 2


def test_each_page_is_visible_to_other_connections_immediately(
    schema_url, conn, tmp_path, obv_fixture
):
    page1, _ = parse_ndjson(obv_fixture("snapshot_page1.ndjson"))
    commit(PostgresStore(conn), tmp_path, page1, SyncState("snapshot", "cur-2", "chg-0"), b"p1")
    with connect(schema_url) as other:
        assert other.execute("SELECT count(*) FROM entity").fetchone()[0] == 2


def test_rejects_non_autocommit_connection(schema_url):
    with psycopg.connect(schema_url) as plain, pytest.raises(ValueError):
        PostgresStore(plain)


def test_delete_of_unknown_entity_leaves_tombstone(conn, tmp_path, obv_fixture):
    store = PostgresStore(conn)
    changes, _ = parse_ndjson(obv_fixture("changes_page1.ndjson"))
    page2, _ = parse_ndjson(obv_fixture("snapshot_page2.ndjson"))
    commit(store, tmp_path, changes[1:], SyncState("changes", "c1"), b"c1")  # delete, seq 106
    commit(store, tmp_path, page2, SyncState("changes", "c1"), b"p2")  # older upsert, seq 104
    assert rows(conn)["8001"][2] is True  # stays deleted


def test_same_page_delete_then_recreate_applies_in_order(conn, tmp_path):
    store = PostgresStore(conn)
    lines = (
        b'{"seq": 1, "op": "upsert", "entity_id": "d:1", "entity_type": "Document", '
        b'"content_hash": "h1", "payload": {"name": "v1"}}\n'
        b'{"seq": 2, "op": "delete", "entity_id": "d:1", "entity_type": "Document"}\n'
        b'{"seq": 3, "op": "upsert", "entity_id": "d:1", "entity_type": "Document", '
        b'"content_hash": "h3", "payload": {"name": "v3"}}\n'
    )
    records, skipped = parse_ndjson(lines)
    assert skipped == 0
    commit(store, tmp_path, records, SyncState("changes", "c"), b"page")
    assert rows(conn)["1"] == ("v3", "h3", False, 3)


def test_record_without_seq_does_not_overwrite_newer(conn, tmp_path, obv_fixture):
    store = PostgresStore(conn)
    page1, _ = parse_ndjson(obv_fixture("snapshot_page1.ndjson"))
    commit(store, tmp_path, page1, SyncState("changes", "c"), b"p1")
    no_seq = page1[0].__class__(**{**page1[0].__dict__, "seq": None, "content_hash": "other"})
    commit(store, tmp_path, [no_seq], SyncState("changes", "c"), b"p2")
    assert rows(conn)["5001"][1] == "sha256:aaa1"


def test_page_headers_and_skipped_lines_are_recorded(conn, tmp_path):
    store = PostgresStore(conn)
    raw = RawStore(tmp_path).put("openbesluitvorming", b"x")
    meta = PageMeta(
        "openbesluitvorming",
        KEY,
        "https://x",
        datetime.now(UTC),
        raw,
        {"x-next-cursor": "n1"},
        2,
    )
    store.commit_page(meta, [], SyncState("changes", "n1"))
    headers, skipped = conn.execute(
        "SELECT response_headers, skipped_lines FROM raw_file"
    ).fetchone()
    assert headers == {"x-next-cursor": "n1"} and skipped == 2
