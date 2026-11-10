"""Adapter for the OpenBesluitvorming bulk-export API (successor of Open Raadsinformatie).

API reference: https://github.com/ontola/openbesluitvorming/blob/main/API.md
- GET /api/export/snapshot?source=KEY[&cursor=C][&limit=N]  current state, paged
- GET /api/export/changes?source=KEY[&cursor=C][&limit=N]   change feed, paged
Paging uses the X-Next-Cursor and X-Has-More response headers. The first snapshot page
also sends X-Changes-Cursor: where the change feed should start once the snapshot is done.

This module only fetches and parses. It does not write anything.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Literal
from urllib.parse import quote

from pipeline.http import PoliteClient

log = logging.getLogger(__name__)

BASE_URL = "https://openbesluitvorming.nl"
SOURCE = "openbesluitvorming"
DEN_HAAG_LABELS = frozenset({"den haag", "'s-gravenhage", "s-gravenhage", "gemeente den haag"})

# API.md does not fully document the payload fields, so try the likely names in order.
# Check these against real data from scripts/probe.py.
_TITLE_KEYS = ("name", "title")
_DATE_KEYS = ("date", "start_date", "sortDate", "publication_date", "last_discussed_at")
_URL_KEYS = ("original_url", "url", "downloadUrl")

Phase = Literal["snapshot", "changes"]


class FeedError(RuntimeError):
    """The export feed returned paging headers we cannot safely follow."""


@dataclass(frozen=True)
class SyncState:
    phase: Phase
    cursor: str | None = None
    changes_cursor: str | None = None

    @classmethod
    def initial(cls) -> SyncState:
        return cls(phase="snapshot")


@dataclass(frozen=True)
class Record:
    """One line of the export feed, with the few fields we index pulled out."""

    entity_id: str
    entity_type: str
    op: Literal["upsert", "delete"]
    seq: int | None
    content_hash: str
    source_key: str | None
    time: str | None
    title: str | None
    doc_date: date | None
    source_url: str | None
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Page:
    records: list[Record]
    raw: bytes
    request_url: str
    next_state: SyncState
    skipped_lines: int
    has_more: bool
    headers: dict[str, str] = field(default_factory=dict)  # the X-* paging headers


def find_source_key(
    client: PoliteClient, labels: frozenset[str] = DEN_HAAG_LABELS, base_url: str = BASE_URL
) -> str:
    """Look up the source key for a municipality by its label (case-insensitive)."""
    sources = client.get(f"{base_url}/api/sources").json().get("sources", [])
    return match_source_key(sources, labels)


def match_source_key(
    sources: list[dict[str, Any]], labels: frozenset[str] = DEN_HAAG_LABELS
) -> str:
    for source in sources:
        if str(source.get("label", "")).strip().lower() in labels:
            return str(source["key"])
    raise LookupError(f"no OpenBesluitvorming source with a label in {sorted(labels)}")


def entity_api_url(entity_id: str, base_url: str = BASE_URL) -> str:
    return f"{base_url}/api/entities/{quote(entity_id, safe='')}"


def parse_ndjson(data: bytes) -> tuple[list[Record], int]:
    """Parse an export page. Returns the records and the number of unparseable lines."""
    records: list[Record] = []
    skipped = 0
    for number, line in enumerate(data.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            records.append(_to_record(json.loads(line)))
        except (ValueError, KeyError, TypeError) as exc:
            skipped += 1
            log.warning("skipping export line %d: %s", number, exc)
    return records, skipped


def _to_record(item: dict[str, Any]) -> Record:
    op = item["op"]
    if op not in ("upsert", "delete"):
        raise ValueError(f"unknown op {op!r}")
    payload = item.get("payload") or {}
    if not isinstance(payload, dict):
        raise TypeError("payload is not an object")
    entity_id = str(item["entity_id"])
    # Tombstones may omit content_hash; upserts must have one.
    content_hash = item.get("content_hash") if op == "delete" else item["content_hash"]
    return Record(
        entity_id=entity_id,
        entity_type=str(item.get("entity_type") or payload.get("type") or "Unknown"),
        op=op,
        seq=int(item["seq"]) if item.get("seq") is not None else None,
        content_hash=str(content_hash or ""),
        source_key=item.get("source_key"),
        time=item.get("time"),
        title=_first_str(payload, _TITLE_KEYS),
        doc_date=_first_date(payload, _DATE_KEYS),
        source_url=_first_str(payload, _URL_KEYS) or entity_api_url(entity_id),
        payload=payload,
    )


def _first_str(payload: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _first_date(payload: dict[str, Any], keys: tuple[str, ...]) -> date | None:
    for key in keys:
        value = payload.get(key)
        if not isinstance(value, str) or not value:
            continue
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
        except ValueError:
            try:
                return date.fromisoformat(value[:10])
            except ValueError:
                continue
    return None


def _has_more(headers: Any, next_cursor: str | None, record_count: int) -> bool:
    value = headers.get("X-Has-More")
    if value is None:
        return bool(next_cursor) and record_count > 0
    return value.strip().lower() == "true"


class OpenBesluitvorming:
    def __init__(
        self,
        client: PoliteClient,
        source_key: str,
        base_url: str = BASE_URL,
        page_size: int = 1000,
        max_pages: int | None = None,
    ) -> None:
        if not 1 <= page_size <= 1000:
            raise ValueError("page_size must be between 1 and 1000 (API maximum)")
        self.client = client
        self.source_key = source_key
        self.base_url = base_url
        self.page_size = page_size
        self.max_pages = max_pages

    def pages(self, state: SyncState) -> Iterator[Page]:
        """Yield pages until the feed is caught up. The caller must persist
        `page.next_state` after handling each page, so a crash resumes there."""
        fetched = 0
        while self.max_pages is None or fetched < self.max_pages:
            page = self._fetch(state)
            fetched += 1
            yield page
            if page.next_state == state and not page.records:
                return  # the server made no progress; avoid looping on the same cursor
            if state.phase == "changes" and not page.has_more:
                return  # change feed caught up; poll again later
            state = page.next_state

    def _fetch(self, state: SyncState) -> Page:
        endpoint = "snapshot" if state.phase == "snapshot" else "changes"
        params: dict[str, str | int] = {"source": self.source_key, "limit": self.page_size}
        if state.cursor:
            params["cursor"] = state.cursor
        response = self.client.get(f"{self.base_url}/api/export/{endpoint}", params=params)
        records, skipped = parse_ndjson(response.content)
        next_cursor = response.headers.get("X-Next-Cursor")
        more = _has_more(response.headers, next_cursor, len(records))
        if more and (not next_cursor or next_cursor == state.cursor):
            raise FeedError(
                f"{endpoint}: X-Has-More is true but the cursor did not advance "
                f"(cursor={state.cursor!r}, next={next_cursor!r}); refusing to loop or skip"
            )
        paging = {k: v for k, v in response.headers.items() if k.lower().startswith("x-")}

        if state.phase == "snapshot":
            changes_cursor = state.changes_cursor
            if changes_cursor is None and state.cursor is None:
                changes_cursor = response.headers.get("X-Changes-Cursor")
                if changes_cursor is None:
                    log.warning(
                        "no X-Changes-Cursor on first snapshot page; change feed will replay"
                    )
            if more and next_cursor:
                next_state = SyncState("snapshot", next_cursor, changes_cursor)
            else:
                next_state = SyncState("changes", changes_cursor)
        else:
            next_state = SyncState("changes", next_cursor or state.cursor)

        return Page(records, response.content, str(response.url), next_state, skipped, more, paging)
