CREATE TABLE IF NOT EXISTS mqtt_ingest.topic_overview (
    topic             TEXT PRIMARY KEY,
    first_seen_at     TIMESTAMPTZ NOT NULL,
    last_seen_at      TIMESTAMPTZ NOT NULL,
    message_count     BIGINT NOT NULL DEFAULT 0,
    last_payload      TEXT,
    last_metadata     JSONB NOT NULL DEFAULT '{}'::JSONB,
    last_event_id     TEXT,
    last_trace_id     TEXT,
    last_publisher_id TEXT,
    last_sequence     BIGINT,
    refreshed_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
