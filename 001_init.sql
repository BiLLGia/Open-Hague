-- 001: raw archive index, source entities and sync state.
-- Typed tables (document, meeting, motion, vote) follow once the probe has confirmed
-- the real payload shapes. Until then the full payload is kept as JSONB.

CREATE TABLE raw_file (
    sha256        text PRIMARY KEY,
    source        text NOT NULL,
    path          text NOT NULL,
    size_bytes    bigint NOT NULL,
    source_url    text NOT NULL,
    retrieved_at  timestamptz NOT NULL,
    response_headers jsonb NOT NULL DEFAULT '{}'::jsonb,  -- X-* paging headers, for replay
    skipped_lines int NOT NULL DEFAULT 0                  -- unparseable lines; bytes kept in raw
);

CREATE TABLE entity (
    source        text NOT NULL,
    entity_id     text NOT NULL,
    source_key    text NOT NULL,
    entity_type   text NOT NULL,
    title         text,
    doc_date      date,
    source_url    text NOT NULL,
    content_hash  text NOT NULL,
    seq           bigint,
    payload       jsonb NOT NULL DEFAULT '{}'::jsonb,
    deleted       boolean NOT NULL DEFAULT false,
    retrieved_at  timestamptz NOT NULL,
    raw_sha256    text NOT NULL REFERENCES raw_file (sha256),
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, entity_id)
);

CREATE INDEX entity_type_date_idx ON entity (source_key, entity_type, doc_date DESC)
    WHERE NOT deleted;

CREATE TABLE sync_state (
    source         text NOT NULL,
    source_key     text NOT NULL,
    phase          text NOT NULL CHECK (phase IN ('snapshot', 'changes')),
    cursor         text,
    changes_cursor text,
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, source_key)
);
