import json
from datetime import UTC, datetime

import httpx
import respx

from pipeline.raw_store import RawStore
from pipeline.run import _run
from pipeline.sources.openbesluitvorming import (
    BASE_URL,
    OpenBesluitvorming,
    SyncState,
    parse_ndjson,
)
from pipeline.store import JsonlStore

SNAPSHOT = f"{BASE_URL}/api/export/snapshot"
CHANGES = f"{BASE_URL}/api/export/changes"
KEY = "example-den-haag"


def ndjson(body: bytes, **headers: str) -> httpx.Response:
    return httpx.Response(200, content=body, headers=headers)


def mock_feed(obv_fixture):
    respx.get(SNAPSHOT).mock(
        side_effect=[
            ndjson(
                obv_fixture("snapshot_page1.ndjson"),
                **{"X-Next-Cursor": "cur-2", "X-Has-More": "true", "X-Changes-Cursor": "chg-0"},
            ),
            ndjson(obv_fixture("snapshot_page2.ndjson"), **{"X-Has-More": "false"}),
        ]
    )
    return respx.get(CHANGES).mock(
        side_effect=[
            ndjson(
                obv_fixture("changes_page1.ndjson"),
                **{"X-Next-Cursor": "chg-1", "X-Has-More": "false"},
            ),
        ]
    )


@respx.mock
def test_jsonl_run_writes_records_raw_files_and_state(tmp_path, fast_client, obv_fixture):
    mock_feed(obv_fixture)
    store = JsonlStore(tmp_path)
    stats = _run(OpenBesluitvorming(fast_client, KEY), store, RawStore(tmp_path), KEY)

    assert stats.pages == 3
    assert stats.skipped_lines == 1
    assert stats.deletes == 1

    rows = [
        json.loads(line)
        for line in (tmp_path / "entities" / f"openbesluitvorming--{KEY}.jsonl").open()
    ]
    assert {r["entity_id"].split(":")[-1] for r in rows} == {"5001", "4001", "7001", "8001"}
    assert all(r["raw_sha256"] and r["retrieved_at"] and r["content_hash"] for r in rows)
    assert all(r["source_url"] for r in rows)

    pages = [
        json.loads(x) for x in (tmp_path / "pages" / f"openbesluitvorming--{KEY}.jsonl").open()
    ]
    assert [p["skipped_lines"] for p in pages] == [0, 1, 0]
    assert pages[0]["headers"]["x-next-cursor"] == "cur-2"

    raw_files = list((tmp_path / "raw" / "openbesluitvorming").rglob("*.ndjson"))
    assert len(raw_files) == 3
    assert store.load_state("openbesluitvorming", KEY) == SyncState(phase="changes", cursor="chg-1")


@respx.mock
def test_rerun_resumes_from_stored_cursor(tmp_path, fast_client, obv_fixture):
    store = JsonlStore(tmp_path)
    mock_feed(obv_fixture)
    _run(OpenBesluitvorming(fast_client, KEY), store, RawStore(tmp_path), KEY)

    respx.reset()
    snapshot = respx.get(SNAPSHOT).mock(return_value=ndjson(b""))
    changes = respx.get(CHANGES).mock(return_value=ndjson(b"", **{"X-Has-More": "false"}))
    stats = _run(OpenBesluitvorming(fast_client, KEY), store, RawStore(tmp_path), KEY)

    assert snapshot.call_count == 0
    assert changes.calls[0].request.url.params["cursor"] == "chg-1"
    assert stats.upserts == 0


def test_ingest_uses_injected_clock(tmp_path, obv_fixture):
    from pipeline.run import ingest
    from pipeline.sources.openbesluitvorming import Page

    records, _ = parse_ndjson(obv_fixture("snapshot_page1.ndjson"))
    page = Page(records, b"raw", "https://x", SyncState("changes", "c"), 0, False, {})
    fixed = datetime(2026, 11, 6, 12, tzinfo=UTC)
    ingest([page], JsonlStore(tmp_path), RawStore(tmp_path), KEY, now=lambda: fixed)
    line = (tmp_path / "entities" / f"openbesluitvorming--{KEY}.jsonl").read_text().splitlines()[0]
    assert json.loads(line)["retrieved_at"] == fixed.isoformat()
