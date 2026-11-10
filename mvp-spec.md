# Open Hague MVP spec

## Goal
An international resident of The Hague can see, in English, what the council decided this week on each coalition priority, and subscribe by RSS. No accounts.

## In scope (MVP)
- Source: OpenBesluitvorming API, Den Haag only, documents dated from 2026-07-01.
- Ingestion: initial snapshot, then hourly incremental sync via the change feed.
- Extraction: Docling to text. OCR only when no text layer exists.
- Enrichment for every document:
  - English title
  - English summary (max 120 words)
  - key decisions (bullet list)
  - labels: coalition chapters 2–16, multi-label, with confidence
- Full English translation on request, cached after the first request.
- Site, static and rebuilt after each sync:
  - home page: latest items
  - one page per coalition chapter
  - item page: English summary, labels, link to the Dutch original, and a disclaimer that the translation is unofficial
  - RSS feed per chapter, plus an "all" feed
- Evaluation: a gold set of 50 Hague documents with checked translations and labels. Report label precision and recall, and translation quality scores.

## Out of scope (later)
- Official publications (KOOP SRU), Woo, participation pages
- Notubiz fallback adapter
- Votes and party-stance search
- Maps
- Other languages and other cities

## Acceptance criteria
- A fresh `docker compose up` plus one pipeline run produces the site from an empty database.
- `uv run pytest` passes with no network access.
- Label micro-F1 is at least 0.70 on the gold set. Any label below 0.5 F1 is hidden from the site.
- Every item page links to its Dutch source and shows the retrieval date.
- The site passes an automated WCAG check (axe) on the home, chapter and item pages.
- Runs on one 4 vCPU / 8 GB server. Enrichment can run in batches on rented GPU time.
