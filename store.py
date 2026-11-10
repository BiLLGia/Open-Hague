"""Where ingested pages go. Two implementations behind one interface:

- PostgresStore: the real store. A page's raw-file row, its records and the new sync
  state are committed in one transaction, so a crash never moves the cursor past
  unsaved data. The connection must be in autocommit mode (see `connect()`), otherwise
  psycopg wraps the whole run in one outer transaction.
- JsonlStore: no database needed. An append-only log: records go to JSONL, page
  provenance to pages.jsonl, state to a JSON file. A crash between appending and
  saving state can repeat a page on the next run, so readers must keep, per entity_id,
  the line with the highest seq.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Protocol

import psycopg
from psycopg.types.json import Jsonb

from pipeline.raw_store import RawRef
from pipeline.sources.openbesluitvorming import Record, SyncState


@dataclass(frozen=True)
class PageMeta:
    source: str
    source_key: str
    request_url: str
    retrieved_at: datetime
    raw: RawRef
    headers: dict[str, str] = field(default_factory=dict)
    skipped_lines: int = 0


class Store(Protocol):
    def load_state(self, source: str, source_key: str) -> SyncState | None: ...

    def commit_page(self, meta: PageMeta, records: list[Record], state: SyncState) -> None: ...


def connect(url: str) -> psycopg.Connection:
    """Open a connection suitable for PostgresStore and migrate()."""
    return psycopg.connect(url, autocommit=True)


# --- JSONL ---------------------------------------------------------------------------------


class JsonlStore:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = Path(data_dir)

    def _path(self, kind: str, meta_source: str, source_key: str, ext: str) -> Path:
        return self.data_dir / kind / f"{meta_source}--{source_key}.{ext}"

    def load_state(self, source: str, source_key: str) -> SyncState | None:
        path = self._path("state", source, source_key, "json")
        if not path.exists():
            return None
        return SyncState(**json.loads(path.read_text(encoding="utf-8")))

    def commit_page(self, meta: PageMeta, records: list[Record], state: SyncState) -> None:
        rows = [
            asdict(r)
            | {
                "doc_date": r.doc_date.isoformat() if r.doc_date else None,
                "retrieved_at": meta.retrieved_at.isoformat(),
                "raw_sha256": meta.raw.sha256,
            }
            for r in records
        ]
        page = {
            "raw_sha256": meta.raw.sha256,
            "request_url": meta.request_url,
            "retrieved_at": meta.retrieved_at.isoformat(),
            "headers": meta.headers,
            "records": len(records),
            "skipped_lines": meta.skipped_lines,
        }
        _append_lines(self._path("entities", meta.source, meta.source_key, "jsonl"), rows)
        _append_lines(self._path("pages", meta.source, meta.source_key, "jsonl"), [page])
        _atomic_write(self._path("state", meta.source, meta.source_key, "json"), asdict(state))


def _append_lines(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    text = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(text)  # one write per page keeps partial-line risk to a crash mid-write
        fh.flush()
        os.fsync(fh.fileno())


def _atomic_write(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp-")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


# --- PostgreSQL ----------------------------------------------------------------------------

# A record applies when the stored row has no seq, or the incoming seq is at least as new.
# An incoming record without seq never overwrites a row that has one.
_NEWER = "(entity.seq IS NULL OR (EXCLUDED.seq IS NOT NULL AND EXCLUDED.seq >= entity.seq))"

_UPSERT = f"""
INSERT INTO entity (source, entity_id, source_key, entity_type, title, doc_date, source_url,
                    content_hash, seq, payload, deleted, retrieved_at, raw_sha256)
VALUES (%(source)s, %(entity_id)s, %(source_key)s, %(entity_type)s, %(title)s, %(doc_date)s,
        %(source_url)s, %(content_hash)s, %(seq)s, %(payload)s, false, %(retrieved_at)s,
        %(raw_sha256)s)
ON CONFLICT (source, entity_id) DO UPDATE SET
    source_key = EXCLUDED.source_key, entity_type = EXCLUDED.entity_type,
    title = EXCLUDED.title, doc_date = EXCLUDED.doc_date, source_url = EXCLUDED.source_url,
    content_hash = EXCLUDED.content_hash, seq = EXCLUDED.seq, payload = EXCLUDED.payload,
    deleted = false, retrieved_at = EXCLUDED.retrieved_at, raw_sha256 = EXCLUDED.raw_sha256,
    updated_at = now()
WHERE {_NEWER}
  AND (entity.content_hash IS DISTINCT FROM EXCLUDED.content_hash OR entity.deleted)
"""

# Deletes are stored as tombstones, so an older upsert replayed later cannot resurrect them.
_DELETE = f"""
INSERT INTO entity (source, entity_id, source_key, entity_type, source_url, content_hash, seq,
                    deleted, retrieved_at, raw_sha256)
VALUES (%(source)s, %(entity_id)s, %(source_key)s, %(entity_type)s, %(source_url)s,
        %(content_hash)s, %(seq)s, true, %(retrieved_at)s, %(raw_sha256)s)
ON CONFLICT (source, entity_id) DO UPDATE SET
    deleted = true, seq = EXCLUDED.seq, retrieved_at = EXCLUDED.retrieved_at,
    raw_sha256 = EXCLUDED.raw_sha256, updated_at = now()
WHERE {_NEWER} AND NOT entity.deleted
"""

_RAW = """
INSERT INTO raw_file (sha256, source, path, size_bytes, source_url, retrieved_at,
                      response_headers, skipped_lines)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (sha256) DO NOTHING
"""

_STATE = """
INSERT INTO sync_state (source, source_key, phase, cursor, changes_cursor)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (source, source_key) DO UPDATE SET
    phase = EXCLUDED.phase, cursor = EXCLUDED.cursor,
    changes_cursor = EXCLUDED.changes_cursor, updated_at = now()
"""


class PostgresStore:
    def __init__(self, conn: psycopg.Connection) -> None:
        if not conn.autocommit:
            raise ValueError("PostgresStore needs an autocommit connection; use store.connect()")
        self.conn = conn

    def load_state(self, source: str, source_key: str) -> SyncState | None:
        row = self.conn.execute(
            "SELECT phase, cursor, changes_cursor FROM sync_state"
            " WHERE source = %s AND source_key = %s",
            (source, source_key),
        ).fetchone()
        return SyncState(*row) if row else None

    def commit_page(self, meta: PageMeta, records: list[Record], state: SyncState) -> None:
        base = {
            "source": meta.source,
            "source_key": meta.source_key,
            "retrieved_at": meta.retrieved_at,
            "raw_sha256": meta.raw.sha256,
        }
        raw_row = (
            meta.raw.sha256,
            meta.source,
            str(meta.raw.path),
            meta.raw.size,
            meta.request_url,
            meta.retrieved_at,
            Jsonb(meta.headers),
            meta.skipped_lines,
        )
        with self.conn.transaction(), self.conn.cursor() as cur:
            cur.execute(_RAW, raw_row)
            for record in records:  # feed order matters for same-page delete/re-create
                cur.execute(_UPSERT if record.op == "upsert" else _DELETE, _row(record, base))
            cur.execute(
                _STATE,
                (meta.source, meta.source_key, state.phase, state.cursor, state.changes_cursor),
            )


def _row(record: Record, base: dict) -> dict:
    return base | {
        "entity_id": record.entity_id,
        "source_key": record.source_key or base["source_key"],
        "entity_type": record.entity_type,
        "title": record.title,
        "doc_date": record.doc_date,
        "source_url": record.source_url,
        "content_hash": record.content_hash,
        "seq": record.seq,
        "payload": Jsonb(record.payload),
    }
