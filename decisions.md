# Architecture decisions

Record one entry per significant choice: date, decision, why, alternatives considered, licence.

## 2026-10 Initial choices
- **Data source:** OpenBesluitvorming API (bulk snapshot plus change feed). The old ORI API shuts down on 1 Nov 2026. Code licence not yet confirmed; ask Ontola.
- **Storage:** PostgreSQL 17 + pgvector (PostgreSQL licence), with Dutch and English full-text search. Raw files on local disk, content-addressed.
- **PDF extraction:** Docling (MIT). pdfplumber (MIT) as fallback. Not PyMuPDF (AGPL).
- **Translation:** OPUS-MT nl-en (Apache-2.0) or MADLAD-400 (Apache-2.0). Compare both on the gold set.
- **Summaries:** EuroLLM-9B-Instruct (Apache-2.0) via llama.cpp (MIT).
- **Labels:** bge-m3 embeddings (MIT), SetFit (Apache-2.0), annotation in Argilla (Apache-2.0).
- **Data model:** inspired by Popolo (CC-BY-4.0) and OParl (CC-BY-SA-4.0).
- **Project licence:** EUPL-1.2 (to confirm).

## 2026-11 Ingestion (steps 0–2)
- **Python deps:** httpx (BSD-3), psycopg 3 (LGPL-3.0, used unmodified as a library), pdfplumber (MIT, probe only). Dev: pytest (MIT), respx (BSD-3), ruff (MIT).
- **Generic `entity` table first, typed tables later.** API.md does not fully document the payload fields, so v1 stores each export record with a few indexed columns (title, date, URL, type) and the full payload as JSONB. Typed document/meeting/motion/vote tables come after the probe confirms the real shapes.
- **No date filter at ingestion:** every record's metadata is stored, and the MVP date window is applied downstream (PDF download, enrichment, site). An earlier `--since` filter was removed after review. Records it filtered out would have been skipped for good once the cursor moved on, and a wrong guess at the date field would have dropped a whole entity type.
- **Atomic commits:** each page's records, raw-file row and new cursor are written in one transaction on an autocommit connection (`store.connect()`), so a crash never moves the cursor past unsaved data. Records apply in feed order. A record applies only if its `seq` is at least the stored one, and a record without `seq` never overwrites one that has it. Deletes leave tombstones, so a replayed older upsert cannot bring a deleted record back.
- **Paging safety:** if `X-Has-More` is true but the cursor is missing or unchanged, the adapter raises `FeedError` instead of looping or cutting the snapshot short. Unparseable lines are counted per page (`raw_file.skipped_lines`), and their bytes stay in the raw archive for reprocessing.
- **Politeness is enforced:** at most 1 request per second whatever the configuration says, and a contact address (`OPEN_HAGUE_CONTACT`) is required before any live run.
- **Raw archive:** every response page is stored as fetched, under `data/raw/{source}/{sha256[:2]}/{sha256}.ndjson`, read-only.
- **User-Agent:** `OpenHague/<version> (+$OPEN_HAGUE_CONTACT)`. The contact is configured per deployment, not hard-coded.
- **Search endpoint not used for harvesting**, as API.md asks. The probe uses the bulk export only.
