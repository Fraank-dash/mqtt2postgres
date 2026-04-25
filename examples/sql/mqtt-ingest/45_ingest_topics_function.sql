CREATE OR REPLACE FUNCTION mqtt_ingest.ingest_topics(
    topic TEXT,
    payload TEXT,
    received_at TIMESTAMPTZ,
    metadata JSONB DEFAULT '{}'::JSONB
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO mqtt_ingest.topic_overview (
        topic,
        first_seen_at,
        last_seen_at,
        message_count,
        last_payload,
        last_metadata,
        last_event_id,
        last_trace_id,
        last_publisher_id,
        last_sequence,
        refreshed_at
    )
    VALUES (
        $1,
        $3,
        $3,
        1,
        $2,
        $4,
        $4 ->> 'event_id',
        $4 ->> 'trace_id',
        $4 ->> 'publisher_id',
        NULLIF($4 ->> 'sequence', '')::BIGINT,
        now()
    )
    ON CONFLICT ON CONSTRAINT topic_overview_pkey DO UPDATE SET
        last_seen_at = EXCLUDED.last_seen_at,
        message_count = mqtt_ingest.topic_overview.message_count + 1,
        last_payload = EXCLUDED.last_payload,
        last_metadata = EXCLUDED.last_metadata,
        last_event_id = EXCLUDED.last_event_id,
        last_trace_id = EXCLUDED.last_trace_id,
        last_publisher_id = EXCLUDED.last_publisher_id,
        last_sequence = EXCLUDED.last_sequence,
        refreshed_at = EXCLUDED.refreshed_at;
END;
$$;
