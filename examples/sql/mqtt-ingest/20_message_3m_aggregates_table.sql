CREATE TABLE IF NOT EXISTS mqtt_ingest.message_3m_aggregates (
    bucket_start       TIMESTAMPTZ NOT NULL,
    bucket_end         TIMESTAMPTZ NOT NULL,
    topic              TEXT NOT NULL,
    device_id          TEXT,
    metric_name        TEXT,
    sample_count       BIGINT NOT NULL,
    numeric_count      BIGINT NOT NULL,
    numeric_avg        DOUBLE PRECISION,
    numeric_min        DOUBLE PRECISION,
    numeric_max        DOUBLE PRECISION,
    first_received_at  TIMESTAMPTZ,
    last_received_at   TIMESTAMPTZ,
    locf_value_at_bucket_start     DOUBLE PRECISION,
    locf_value_at_bucket_end       DOUBLE PRECISION,
    locf_time_weighted_avg         DOUBLE PRECISION,
    linear_value_at_bucket_start   DOUBLE PRECISION,
    linear_value_at_bucket_end     DOUBLE PRECISION,
    linear_time_weighted_avg       DOUBLE PRECISION,
    status             TEXT NOT NULL,
    refreshed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (bucket_start, topic),
    CONSTRAINT message_3m_aggregates_device_bucket_key
        UNIQUE (bucket_start, device_id, metric_name),
    CONSTRAINT message_3m_aggregates_status_check
        CHECK (status IN ('aggregated', 'tba')),
    CONSTRAINT message_3m_aggregates_bucket_check
        CHECK (bucket_end = bucket_start + INTERVAL '3 minutes')
);

SELECT create_hypertable(
    'mqtt_ingest.message_3m_aggregates',
    'bucket_start',
    if_not_exists => TRUE
);

ALTER TABLE mqtt_ingest.message_3m_aggregates
    ADD COLUMN IF NOT EXISTS device_id TEXT,
    ADD COLUMN IF NOT EXISTS metric_name TEXT,
    ADD COLUMN IF NOT EXISTS locf_value_at_bucket_start DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS locf_value_at_bucket_end DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS locf_time_weighted_avg DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS linear_value_at_bucket_start DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS linear_value_at_bucket_end DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS linear_time_weighted_avg DOUBLE PRECISION;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'message_3m_aggregates_device_bucket_key'
          AND conrelid = 'mqtt_ingest.message_3m_aggregates'::regclass
    ) THEN
        ALTER TABLE mqtt_ingest.message_3m_aggregates
            ADD CONSTRAINT message_3m_aggregates_device_bucket_key
                UNIQUE (bucket_start, device_id, metric_name);
    END IF;
END;
$$;
