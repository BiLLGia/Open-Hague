"""Run ingestion for one source.

    uv run python -m pipeline.run --source openbesluitvorming

Stores every record the feed returns (metadata only; PDFs are fetched in a later step).
Scoping by date (e.g. from 2026-07-01) happens downstream, so no record is ever skipped
here and the cursor can safely move on. Uses PostgreSQL when DATABASE_URL is set,
otherwise JSONL files under the data directory (override with --store). Safe to stop and
re-run: it resumes from the stored cursor.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime

from pipeline.config import Settings
from pipeline.http import PoliteClient
from pipeline.raw_store import RawStore
from pipeline.sources import openbesluitvorming as obv
from pipeline.store import JsonlStore, PageMeta, PostgresStore, Store, connect

log = logging.getLogger("pipeline.run")

# Never hit a source faster than this, whatever OPEN_HAGUE_MIN_INTERVAL says.
MIN_INTERVAL_FLOOR = 1.0


@dataclass
class RunStats:
    pages: int = 0
    upserts: int = 0
    deletes: int = 0
    skipped_lines: int = 0


def ingest(
    pages: Iterable[obv.Page],
    store: Store,
    raw_store: RawStore,
    source_key: str,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> RunStats:
    stats = RunStats()
    for page in pages:
        raw = raw_store.put(obv.SOURCE, page.raw, suffix=".ndjson")
        meta = PageMeta(
            obv.SOURCE, source_key, page.request_url, now(), raw, page.headers, page.skipped_lines
        )
        store.commit_page(meta, page.records, page.next_state)
        stats.pages += 1
        stats.upserts += sum(r.op == "upsert" for r in page.records)
        stats.deletes += sum(r.op == "delete" for r in page.records)
        stats.skipped_lines += page.skipped_lines
        if page.skipped_lines:
            log.warning(
                "page %d had %d unparseable lines; bytes kept in raw file %s",
                stats.pages,
                page.skipped_lines,
                raw.sha256,
            )
        log.info("page %d: %d records, state=%s", stats.pages, len(page.records), page.next_state)
    return stats


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--source", required=True, choices=["openbesluitvorming"])
    parser.add_argument(
        "--source-key", help="OpenBesluitvorming source key (default: look up Den Haag)"
    )
    parser.add_argument("--max-pages", type=int, help="stop after N pages (testing)")
    parser.add_argument("--store", choices=["auto", "postgres", "jsonl"], default="auto")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    settings = Settings.from_env()
    if not settings.contact:
        parser.error("set OPEN_HAGUE_CONTACT (a URL or mailbox) so source operators can reach you")
    use_pg = args.store == "postgres" or (args.store == "auto" and settings.database_url)
    if use_pg and not settings.database_url:
        parser.error("--store postgres needs DATABASE_URL")

    interval = max(settings.min_interval, MIN_INTERVAL_FLOOR)
    with PoliteClient(settings.user_agent, min_interval=interval) as client:
        source_key = args.source_key or obv.find_source_key(client)
        log.info("source key: %s", source_key)
        adapter = obv.OpenBesluitvorming(client, source_key, max_pages=args.max_pages)
        raw_store = RawStore(settings.data_dir)
        if use_pg:
            from pipeline.db.migrate import migrate

            with connect(settings.database_url) as conn:
                migrate(conn)
                stats = _run(adapter, PostgresStore(conn), raw_store, source_key)
        else:
            stats = _run(adapter, JsonlStore(settings.data_dir), raw_store, source_key)
    log.info("done: %s", stats)
    return 0


def _run(
    adapter: obv.OpenBesluitvorming, store: Store, raw_store: RawStore, source_key: str
) -> RunStats:
    state = store.load_state(obv.SOURCE, source_key) or obv.SyncState.initial()
    log.info("starting from %s", state)
    return ingest(adapter.pages(state), store, raw_store, source_key)


if __name__ == "__main__":
    raise SystemExit(main())
