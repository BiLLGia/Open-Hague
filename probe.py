"""Data probe (playbook step 1). Run it on a machine with internet access:

    uv sync --extra probe
    uv run python scripts/probe.py --save-fixtures

It answers:
- Does OpenBesluitvorming cover Den Haag, and under which source key?
- How many documents, meetings and motions per month since --since?
- Which payload fields carry the title and date (so the adapter can be checked)?
- What share of sampled PDFs has a text layer, and how big are they?
- How many Gemeenteblad publications does KOOP's SRU API return for Den Haag?

Writes probe-report.json, and with --save-fixtures stores real responses under
tests/fixtures/*/real/ for the test suite. Read-only and rate-limited (1 request/second).
"""

from __future__ import annotations

import argparse
import io
import json
import re
import statistics
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline.config import Settings  # noqa: E402
from pipeline.http import PoliteClient  # noqa: E402
from pipeline.sources import openbesluitvorming as obv  # noqa: E402

SRU_URL = "https://repository.overheid.nl/sru"
FIXTURES = ROOT / "tests" / "fixtures"


@dataclass
class Volume:
    pages: int = 0
    records: int = 0
    by_type: Counter = field(default_factory=Counter)
    by_month: Counter = field(default_factory=Counter)  # (YYYY-MM, type) for in-scope records
    undated: Counter = field(default_factory=Counter)
    payload_keys: Counter = field(default_factory=Counter)
    title_found: int = 0
    date_found: int = 0
    in_scope_ids: list[tuple[str, str]] = field(default_factory=list)  # (entity_id, type)


def tally(volume: Volume, records: list[obv.Record], since: date) -> None:
    """Add one page of export records to the running counts."""
    for record in records:
        if record.op != "upsert":
            continue
        volume.records += 1
        volume.by_type[record.entity_type] += 1
        volume.payload_keys.update(record.payload.keys())
        volume.title_found += record.title is not None
        volume.date_found += record.doc_date is not None
        if record.doc_date is None:
            volume.undated[record.entity_type] += 1
        elif record.doc_date >= since:
            volume.by_month[(record.doc_date.strftime("%Y-%m"), record.entity_type)] += 1
            volume.in_scope_ids.append((record.entity_id, record.entity_type))


def monthly_table(volume: Volume) -> dict[str, dict[str, int]]:
    table: dict[str, dict[str, int]] = {}
    for (month, entity_type), count in sorted(volume.by_month.items()):
        table.setdefault(month, {})[entity_type] = count
    return table


def save(path: Path, data: bytes | str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data if isinstance(data, bytes) else data.encode("utf-8"))


def probe_obv(client: PoliteClient, args: argparse.Namespace, report: dict[str, Any]) -> list[str]:
    print("\n== OpenBesluitvorming ==")
    sources = client.get(f"{obv.BASE_URL}/api/sources").json().get("sources", [])
    if args.save_fixtures:
        save(
            FIXTURES / "openbesluitvorming" / "real" / "sources.json", json.dumps(sources, indent=1)
        )
    matches = [
        s
        for s in sources
        if "haag" in str(s.get("label", "")).lower()
        or "gravenhage" in str(s.get("label", "")).lower()
    ]
    report["obv_sources_total"] = len(sources)
    report["obv_den_haag_candidates"] = matches
    candidates = json.dumps(matches, ensure_ascii=False)
    print(f"sources listed: {len(sources)}; Den Haag candidates: {candidates}")
    try:
        key = args.source_key or obv.match_source_key(sources)
    except LookupError:
        print("!! Den Haag not found by label. Re-run with --source-key KEY from the list above.")
        report["obv_covered"] = False
        return []
    report["obv_covered"] = True
    report["obv_source_key"] = key
    print(f"source key: {key}")

    stats = attempt("stats", lambda: client.get(f"{obv.BASE_URL}/api/stats").json(), report)
    report["obv_stats"] = stats
    print(f"index stats: {stats}")

    volume = Volume()
    adapter = obv.OpenBesluitvorming(client, key, max_pages=args.max_pages or None)
    complete = False
    try:
        for page in adapter.pages(obv.SyncState.initial()):
            volume.pages += 1
            tally(volume, page.records, args.since)
            if args.save_fixtures and volume.pages == 1:
                real = FIXTURES / "openbesluitvorming" / "real"
                save(real / "snapshot_page1.ndjson", page.raw)
                save(real / "snapshot_page1.headers.json", json.dumps(page.headers))
            print(f"  page {volume.pages}: {len(page.records)} records, {page.skipped_lines} bad")
            if page.next_state.phase == "changes":
                complete = True  # snapshot finished; volume counts are complete
                break
    except Exception as exc:  # keep what we have and still write the report
        report.setdefault("errors", {})["snapshot"] = repr(exc)
        print(f"  !! snapshot read stopped: {exc!r}")

    report["obv_volume"] = {
        "pages_read": volume.pages,
        "complete_snapshot": complete,
        "records": volume.records,
        "by_type": dict(volume.by_type),
        "since": args.since.isoformat(),
        "per_month_in_scope": monthly_table(volume),
        "undated_by_type": dict(volume.undated),
        "payload_keys_top30": volume.payload_keys.most_common(30),
        "title_found_pct": round(100 * volume.title_found / max(volume.records, 1), 1),
        "date_found_pct": round(100 * volume.date_found / max(volume.records, 1), 1),
    }
    if not complete:
        print("  NOTE: snapshot is sorted by entity id, not date. Counts from a partial read are a")
        print("  sample, not a total. Use --max-pages 0 for the full snapshot.")
    print(f"  per month since {args.since}: {json.dumps(monthly_table(volume))}")
    print(
        f"  title parsed: {report['obv_volume']['title_found_pct']}%, "
        f"date parsed: {report['obv_volume']['date_found_pct']}%"
    )
    print(f"  most common payload keys: {[k for k, _ in volume.payload_keys.most_common(15)]}")

    # One change-feed page, for the fixtures.
    if args.save_fixtures:

        def changes_page() -> None:
            url = f"{obv.BASE_URL}/api/export/changes"
            response = client.get(url, params={"source": key, "limit": 20})
            real = FIXTURES / "openbesluitvorming" / "real"
            save(real / "changes_page.ndjson", response.content)
            headers = {k: v for k, v in response.headers.items() if k.lower().startswith("x-")}
            save(real / "changes_page.headers.json", json.dumps(headers))

        attempt("changes_page", changes_page, report)

    # Entity details: one of each type, plus document detail for the PDF sample.
    pdf_urls: list[str] = []
    seen_types: set[str] = set()
    docs_checked = 0
    for entity_id, entity_type in volume.in_scope_ids:
        want_pdf = entity_type == "Document" and docs_checked < args.pdf_sample
        if entity_type in seen_types and not want_pdf:
            continue
        url = obv.entity_api_url(entity_id)
        detail = attempt(f"entity {entity_id}", lambda u=url: client.get(u).json(), report)
        if not isinstance(detail, dict):
            continue
        if entity_type not in seen_types:
            seen_types.add(entity_type)
            report.setdefault("obv_detail_keys", {})[entity_type] = sorted(detail.keys())
            if args.save_fixtures:
                name = f"entity_{entity_type.lower()}.json"
                save(FIXTURES / "openbesluitvorming" / "real" / name, json.dumps(detail, indent=1))
        if want_pdf:
            docs_checked += 1
            pdf = pdf_url(detail)
            if pdf:
                pdf_urls.append(pdf)
        if docs_checked >= args.pdf_sample and len(seen_types) >= 3:
            break
    return pdf_urls


def attempt(label: str, fn: Any, report: dict[str, Any]) -> Any:
    """Run one probe call; record the error instead of aborting the whole probe."""
    try:
        return fn()
    except Exception as exc:
        report.setdefault("errors", {})[label] = repr(exc)
        print(f"  !! {label}: {exc!r}")
        return None


def pdf_url(detail: dict[str, Any], base_url: str = obv.BASE_URL) -> str | None:
    """Best guess at a document's PDF link from an /api/entities response."""
    content_type = str(detail.get("contentType") or "").split(";")[0].strip().lower()
    candidates = [detail.get("pdfUrl"), detail.get("downloadUrl")]
    payload = detail.get("payload") if isinstance(detail.get("payload"), dict) else {}
    candidates += list(detail.get("media_urls") or payload.get("media_urls") or [])
    for url in candidates:
        if not isinstance(url, str) or not url:
            continue
        absolute = urljoin(base_url + "/", url)
        if content_type in ("", "application/pdf") or absolute.lower().endswith(".pdf"):
            return absolute
    return None


def probe_pdfs(
    client: PoliteClient, urls: list[str], args: argparse.Namespace, report: dict[str, Any]
) -> None:
    print("\n== PDF sample ==")
    try:
        import pdfplumber
    except ImportError:
        print("pdfplumber not installed: run `uv sync --extra probe`. Skipping.")
        return
    sizes: list[int] = []
    with_text = 0
    pages: list[int] = []
    for index, url in enumerate(urls):
        data = attempt(f"pdf {url}", lambda u=url: client.get(u).content, report)
        if not data:
            continue
        try:
            with pdfplumber.open(io.BytesIO(data)) as pdf:
                page_count = len(pdf.pages)
                chars = sum(len(p.extract_text() or "") for p in pdf.pages[:3])
        except Exception as exc:
            print(f"  could not open {url}: {exc}")
            continue
        sizes.append(len(data))
        pages.append(page_count)
        with_text += chars > 200
        if args.save_fixtures and index < 3:
            save(FIXTURES / "pdf" / "real" / f"sample_{index + 1}.pdf", data)
    if not sizes:
        print("no PDFs sampled")
        return
    report["pdf_sample"] = {
        "count": len(sizes),
        "with_text_layer_pct": round(100 * with_text / len(sizes), 1),
        "median_size_kb": round(statistics.median(sizes) / 1024),
        "median_pages": statistics.median(pages) if pages else None,
    }
    print(f"  {report['pdf_sample']}")


def sru_query(creator: str, since: date) -> str:
    return (
        f'c.product-area==officielepublicaties AND w.publicatienaam=="Gemeenteblad" '
        f'AND dt.creator=="{creator}" AND dt.modified>={since.isoformat()}'
    )


def probe_koop(client: PoliteClient, args: argparse.Namespace, report: dict[str, Any]) -> None:
    print("\n== KOOP SRU (Gemeenteblad) ==")
    results = {}
    for creator in ("Den Haag", "'s-Gravenhage"):
        query = sru_query(creator, args.since)
        params = {
            "operation": "searchRetrieve",
            "version": "2.0",
            "query": query,
            "maximumRecords": 5,
            "startRecord": 1,
        }
        response = attempt(f"sru {creator}", lambda p=params: client.get(SRU_URL, params=p), report)
        if response is None:
            continue
        match = re.search(rb"<(?:\w+:)?numberOfRecords>(\d+)<", response.content)
        count = int(match.group(1)) if match else None
        results[creator] = count
        print(f'  dt.creator=="{creator}": {count} records since {args.since}')
        if args.save_fixtures and count:
            slug = re.sub(r"[^a-z]+", "-", creator.lower()).strip("-")
            save(FIXTURES / "koop" / "real" / f"sru_{slug}.xml", response.content)
    report["koop_sru_counts"] = results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Probe the data sources for Den Haag.")
    parser.add_argument("--since", type=date.fromisoformat, default=date(2026, 7, 1))
    parser.add_argument("--source-key", help="skip lookup and use this OpenBesluitvorming key")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=50,
        help="snapshot pages to read (0 = all; 1000 records per page)",
    )
    parser.add_argument("--pdf-sample", type=int, default=10)
    parser.add_argument("--save-fixtures", action="store_true")
    parser.add_argument("--out", type=Path, default=ROOT / "probe-report.json")
    parser.add_argument("--skip-koop", action="store_true")
    args = parser.parse_args(argv)

    settings = Settings.from_env()
    report: dict[str, Any] = {"since": args.since.isoformat()}
    if not settings.contact:
        parser.error("set OPEN_HAGUE_CONTACT (a URL or mailbox) so source operators can reach you")
    interval = max(settings.min_interval, 1.0)
    try:
        with PoliteClient(settings.user_agent, min_interval=interval) as client:
            pdf_urls = probe_obv(client, args, report)
            if pdf_urls:
                probe_pdfs(client, pdf_urls, args, report)
            if not args.skip_koop:
                probe_koop(client, args, report)
    finally:
        args.out.write_text(
            json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
        )
        print(f"\nreport written to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
