import importlib.util
import sys
from datetime import date
from pathlib import Path

import httpx
import respx

from pipeline.sources.openbesluitvorming import BASE_URL, parse_ndjson

spec = importlib.util.spec_from_file_location(
    "probe", Path(__file__).parents[1] / "scripts" / "probe.py"
)
probe = importlib.util.module_from_spec(spec)
sys.modules["probe"] = probe  # dataclasses need the module registered
spec.loader.exec_module(probe)


def test_tally_counts_in_scope_months_and_field_coverage(obv_fixture):
    volume = probe.Volume()
    for name in ("snapshot_page1.ndjson", "snapshot_page2.ndjson", "changes_page1.ndjson"):
        records, _ = parse_ndjson(obv_fixture(name))
        probe.tally(volume, records, date(2026, 7, 1))
    assert volume.records == 5  # deletes are not counted
    assert probe.monthly_table(volume) == {
        "2026-07": {"Document": 2},
        "2026-09": {"Meeting": 1, "Motion": 1},
    }
    assert volume.title_found == 5 and volume.date_found == 5
    assert volume.payload_keys["original_url"] == 5


@respx.mock
def test_probe_obv_reports_missing_coverage(fast_client, tmp_path):
    respx.get(f"{BASE_URL}/api/sources").mock(
        return_value=httpx.Response(200, json={"sources": []})
    )
    args = probe.argparse.Namespace(
        source_key=None, save_fixtures=False, since=date(2026, 7, 1), max_pages=1, pdf_sample=0
    )
    report = {}
    assert probe.probe_obv(fast_client, args, report) == []
    assert report["obv_covered"] is False


def test_pdf_url_handles_content_type_params_relative_and_media_urls():
    assert (
        probe.pdf_url({"downloadUrl": "/files/a.pdf", "contentType": "application/pdf; v=1"})
        == f"{BASE_URL}/files/a.pdf"
    )
    assert probe.pdf_url({"contentType": "text/html", "downloadUrl": "https://x/page"}) is None
    assert probe.pdf_url({"payload": {"media_urls": ["https://x/doc.pdf"]}}) == "https://x/doc.pdf"


def test_sru_query_filters_gemeenteblad():
    q = probe.sru_query("Den Haag", date(2026, 7, 1))
    assert 'w.publicatienaam=="Gemeenteblad"' in q and "dt.modified>=2026-07-01" in q


@respx.mock
def test_probe_keeps_going_when_a_detail_call_fails(fast_client, obv_fixture):
    respx.get(f"{BASE_URL}/api/sources").mock(
        return_value=httpx.Response(200, content=obv_fixture("sources.json"))
    )
    respx.get(f"{BASE_URL}/api/stats").mock(return_value=httpx.Response(500))
    respx.get(f"{BASE_URL}/api/export/snapshot").mock(
        return_value=httpx.Response(
            200, content=obv_fixture("snapshot_page1.ndjson"), headers={"X-Has-More": "false"}
        )
    )
    respx.get(url__startswith=f"{BASE_URL}/api/entities/").mock(return_value=httpx.Response(404))
    args = probe.argparse.Namespace(
        source_key=None, save_fixtures=False, since=date(2026, 7, 1), max_pages=0, pdf_sample=2
    )
    report = {}
    assert probe.probe_obv(fast_client, args, report) == []
    assert report["obv_volume"]["complete_snapshot"] is True
    assert "stats" in report["errors"]
    assert any(k.startswith("entity ") for k in report["errors"])
