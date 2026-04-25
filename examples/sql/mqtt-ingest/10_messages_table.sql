CREATE TABLE IF NOT EXISTS mqtt_ingest.messages (
    received_at    TIMESTAMPTZ NOT NULL,
    topic          TEXT NOT NULL,
    payload        TEXT NOT NULL,
    numeric_value  DOUBLE PRECISION,
    event_id       TEXT,
    trace_id       TEXT,
    publisher_id   TEXT,
    sequence       BIGINT,
    published_at   TIMESTAMPTZ,
    metadata       JSONB NOT NULL DEFAULT '{}'::JSONB,
    committed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable('mqtt_ingest.messages', 'received_at', if_not_exists => TRUE);
