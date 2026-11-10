from datetime import date

import httpx
import pytest
import respx

from pipeline.sources.openbesluitvorming import (
    BASE_URL,
    FeedError,
    OpenBesluitvorming,
    SyncState,
    find_source_key,
    parse_ndjson,
)

SNAPSHOT = f"{BASE_URL}/api/export/snapshot"
CHANGES = f"{BASE_URL}/api/export/changes"


def ndjson_response(body: bytes, **headers: str) -> httpx.Response:
    return httpx.Response(
        200, content=body, headers={"Content-Type": "application/x-ndjson", **headers}
    )


# --- parsing -------------------------------------------------------------------------------


def test_parse_upsert_record(obv_fixture):
    records, skipped = parse_ndjson(obv_fixture("snapshot_page1.ndjson"))
    assert skipped == 0
    first = records[0]
    assert first.entity_id == "document:notubiz:gemeente:example-den-haag:5001"
    assert first.entity_type == "Document"
    assert first.op == "upsert"
    assert first.seq == 101
    assert first.content_hash == "sha256:aaa1"
    assert first.title == "RIS900001 Raadsvoorstel voorbeeld woningbouw"
    assert first.doc_date == date(2026, 7, 15)
    assert first.source_url == "https://example.invalid/document/5001"


def test_parse_reads_datetime_dates_and_skips_bad_lines(obv_fixture):
    records, skipped = parse_ndjson(obv_fixture("snapshot_page2.ndjson"))
    assert skipped == 1
    meeting = records[0]
    assert meeting.entity_type == "Meeting"
    assert meeting.doc_date == date(2026, 9, 24)


def test_parse_delete_has_no_payload(obv_fixture):
    records, _ = parse_ndjson(obv_fixture("changes_page1.ndjson"))
    tombstone = records[1]
    assert tombstone.op == "delete"
    assert tombstone.payload == {}
    assert tombstone.title is None


def test_parse_ignores_blank_lines():
    records, skipped = parse_ndjson(b"\n\n")
    assert records == [] and skipped == 0


# --- source discovery ----------------------------------------------------------------------


@respx.mock
def test_find_source_key_by_label(fast_client, obv_fixture):
    respx.get(f"{BASE_URL}/api/sources").mock(
        return_value=httpx.Response(200, content=obv_fixture("sources.json"))
    )
    assert find_source_key(fast_client) == "example-den-haag"


@respx.mock
def test_find_source_key_accepts_official_name(fast_client):
    body = {
        "sources": [
            {"key": "sgravenhage", "label": "'s-Gravenhage", "organizationType": "gemeente"}
        ]
    }
    respx.get(f"{BASE_URL}/api/sources").mock(return_value=httpx.Response(200, json=body))
    assert find_source_key(fast_client) == "sgravenhage"


@respx.mock
def test_find_source_key_raises_when_missing(fast_client):
    respx.get(f"{BASE_URL}/api/sources").mock(
        return_value=httpx.Response(200, json={"sources": []})
    )
    with pytest.raises(LookupError):
        find_source_key(fast_client)


# --- syncing -------------------------------------------------------------------------------


@respx.mock
def test_full_snapshot_then_changes(fast_client, obv_fixture):
    snapshot = respx.get(SNAPSHOT).mock(
        side_effect=[
            ndjson_response(
                obv_fixture("snapshot_page1.ndjson"),
                **{"X-Next-Cursor": "cur-2", "X-Has-More": "true", "X-Changes-Cursor": "chg-0"},
            ),
            ndjson_response(
                obv_fixture("snapshot_page2.ndjson"),
                **{"X-Next-Cursor": "cur-3", "X-Has-More": "false"},
            ),
        ]
    )
    changes = respx.get(CHANGES).mock(
        side_effect=[
            ndjson_response(
                obv_fixture("changes_page1.ndjson"),
                **{"X-Next-Cursor": "chg-1", "X-Has-More": "true"},
            ),
            ndjson_response(b"", **{"X-Next-Cursor": "chg-1", "X-Has-More": "false"}),
        ]
    )

    adapter = OpenBesluitvorming(fast_client, source_key="example-den-haag", page_size=500)
    pages = list(adapter.pages(SyncState.initial()))

    assert [len(p.records) for p in pages] == [2, 2, 2, 0]
    assert pages[1].skipped_lines == 1

    first_params = snapshot.calls[0].request.url.params
    assert first_params["source"] == "example-den-haag"
    assert first_params["limit"] == "500"
    assert "cursor" not in first_params
    assert snapshot.calls[1].request.url.params["cursor"] == "cur-2"

    # After the last snapshot page we switch to the change feed at the cursor from page 1.
    assert pages[1].next_state == SyncState(phase="changes", cursor="chg-0")
    assert changes.calls[0].request.url.params["cursor"] == "chg-0"
    assert pages[-1].next_state == SyncState(phase="changes", cursor="chg-1")


@respx.mock
def test_resumes_snapshot_from_saved_cursor(fast_client, obv_fixture):
    snapshot = respx.get(SNAPSHOT).mock(
        return_value=ndjson_response(
            obv_fixture("snapshot_page2.ndjson"), **{"X-Has-More": "false"}
        )
    )
    respx.get(CHANGES).mock(return_value=ndjson_response(b"", **{"X-Has-More": "false"}))
    state = SyncState(phase="snapshot", cursor="cur-2", changes_cursor="chg-0")
    pages = list(OpenBesluitvorming(fast_client, "example-den-haag").pages(state))
    assert snapshot.calls[0].request.url.params["cursor"] == "cur-2"
    assert pages[0].next_state == SyncState(phase="changes", cursor="chg-0")


@respx.mock
def test_caught_up_change_feed_keeps_cursor(fast_client):
    respx.get(CHANGES).mock(return_value=ndjson_response(b"", **{"X-Has-More": "false"}))
    state = SyncState(phase="changes", cursor="chg-9")
    pages = list(OpenBesluitvorming(fast_client, "example-den-haag").pages(state))
    assert len(pages) == 1
    assert pages[0].next_state == state


@respx.mock
def test_refuses_to_loop_when_cursor_does_not_advance(fast_client, obv_fixture):
    respx.get(CHANGES).mock(
        return_value=ndjson_response(
            obv_fixture("changes_page1.ndjson"), **{"X-Next-Cursor": "chg-9", "X-Has-More": "true"}
        )
    )
    state = SyncState(phase="changes", cursor="chg-9")
    with pytest.raises(FeedError):
        list(OpenBesluitvorming(fast_client, "example-den-haag").pages(state))


@respx.mock
def test_refuses_to_cut_snapshot_short_without_cursor(fast_client, obv_fixture):
    respx.get(SNAPSHOT).mock(
        return_value=ndjson_response(obv_fixture("snapshot_page1.ndjson"), **{"X-Has-More": "true"})
    )
    with pytest.raises(FeedError):
        list(OpenBesluitvorming(fast_client, "example-den-haag").pages(SyncState.initial()))


@respx.mock
def test_max_pages_limits_requests(fast_client, obv_fixture):
    route = respx.get(SNAPSHOT).mock(
        side_effect=[
            ndjson_response(
                obv_fixture("snapshot_page1.ndjson"),
                **{"X-Next-Cursor": f"cur-{i}", "X-Has-More": "true"},
            )
            for i in range(10)
        ]
    )
    pages = list(
        OpenBesluitvorming(fast_client, "example-den-haag", max_pages=3).pages(SyncState.initial())
    )
    assert len(pages) == 3
    assert route.call_count == 3


@respx.mock
def test_missing_has_more_header_falls_back_to_cursor(fast_client, obv_fixture):
    respx.get(CHANGES).mock(
        side_effect=[
            ndjson_response(obv_fixture("changes_page1.ndjson"), **{"X-Next-Cursor": "chg-2"}),
            ndjson_response(b""),
        ]
    )
    pages = list(
        OpenBesluitvorming(fast_client, "example-den-haag").pages(
            SyncState(phase="changes", cursor="chg-1")
        )
    )
    assert len(pages) == 2
    assert pages[-1].next_state.cursor == "chg-2"


@respx.mock
def test_stops_when_changes_page_says_no_more(fast_client, obv_fixture):
    route = respx.get(CHANGES).mock(
        return_value=ndjson_response(
            obv_fixture("changes_page1.ndjson"), **{"X-Next-Cursor": "chg-2", "X-Has-More": "false"}
        )
    )
    pages = list(
        OpenBesluitvorming(fast_client, "example-den-haag").pages(
            SyncState(phase="changes", cursor="chg-1")
        )
    )
    assert route.call_count == 1
    assert pages[0].next_state.cursor == "chg-2"


def test_delete_without_content_hash_is_accepted():
    line = b'{"seq": 9, "op": "delete", "entity_id": "motion:x:1", "entity_type": "Motion"}'
    records, skipped = parse_ndjson(line)
    assert skipped == 0
    assert records[0].op == "delete" and records[0].content_hash == ""


def test_upsert_without_content_hash_is_rejected():
    line = b'{"seq": 9, "op": "upsert", "entity_id": "doc:x:1", "payload": {}}'
    records, skipped = parse_ndjson(line)
    assert records == [] and skipped == 1


def test_source_url_falls_back_to_entity_api():
    line = b'{"seq": 1, "op": "upsert", "entity_id": "document:a:b", "content_hash": "h"}'
    records, _ = parse_ndjson(line)
    assert records[0].source_url == f"{BASE_URL}/api/entities/document%3Aa%3Ab"
