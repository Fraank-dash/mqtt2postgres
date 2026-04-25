CREATE TABLE IF NOT EXISTS mqtt_ingest.messages (
    received_at    TIMESTAMPTZ NOT NULL,
    topic          TEXT NOT NULL,
    device_id      TEXT,
    metric_name    TEXT,
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

ALTER TABLE mqtt_ingest.messages
    ADD COLUMN IF NOT EXISTS device_id TEXT,
    ADD COLUMN IF NOT EXISTS metric_name TEXT;
