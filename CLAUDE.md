# Open Hague

An open-source pipeline and static site. It makes The Hague's council decisions readable in English, tagged by the 2026–2030 coalition-agreement chapters.
Product spec: @docs/mvp-spec.md. Architecture decisions: @docs/decisions.md

## Stack
- Python 3.12, uv for dependencies, ruff for lint and format, pytest for tests
- PostgreSQL 17 + pgvector (docker compose in `infra/`)
- Docling for PDF extraction
- Static site generated from the database into `web/dist/`, with RSS feeds in `web/dist/feeds/`

## Layout
- `pipeline/sources/`: one adapter per data source (fetch only, no transformation)
- `pipeline/extract/`: PDF to text
- `pipeline/enrich/`: translation, summaries, labels (model runners behind interfaces)
- `pipeline/db/`: schema and migrations
- `web/`: static site generator and templates (not named `site`, which would shadow the Python stdlib module)
- `tests/fixtures/`: recorded API responses and sample PDFs. Tests never hit the network.

## Commands
- `uv run pytest`: all tests (must pass before every commit). Set `OPEN_HAGUE_TEST_DATABASE_URL` to also run the PostgreSQL tests.
- `uv run ruff check --fix . && uv run ruff format .`
- `docker compose -f infra/compose.yml up -d db`: local database
- `uv run python -m pipeline.run --source openbesluitvorming`: run ingestion (stores every record; date scoping is downstream)
- `uv run python scripts/probe.py --save-fixtures`: step-1 data probe (needs internet)
- `uv run python -m web.build`: build the static site

## Rules
- Runtime models must be open source and self-hostable (Apache-2.0, MIT or similar). Never add NLLB, Tower, GEITje or any non-commercial or gated model.
- Licences: check the licence of every new dependency and record it in docs/decisions.md. Do not use PyMuPDF (AGPL).
- Every stored record keeps `source_url`, `retrieved_at` and `content_hash`. Raw files are immutable, stored under `data/raw/{source}/{sha256}`.
- Be polite to sources: send a User-Agent with a contact address, rate-limit requests, use incremental or change-feed endpoints.
- Privacy: skip official publications marked `/noindex/`. Never build feeds that list named private individuals.
- Neutrality: summaries are factual. Party stances are shown only as cited quotes with a link to the source.
- Write or update tests first, then implement. Keep functions small and typed.
- Never read or print `.env` or any secrets.

## Taxonomy
Labels are coalition chapters 2–15 plus 16 (finances, cross-cutting). The mapping to budget programmes is in `pipeline/enrich/taxonomy.yaml`.
