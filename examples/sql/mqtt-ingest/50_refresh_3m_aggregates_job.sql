CREATE OR REPLACE PROCEDURE mqtt_ingest.refresh_message_3m_aggregates_job(
    job_id INTEGER,
    config JSONB
)
LANGUAGE plpgsql
AS $$
BEGIN
    CALL mqtt_ingest.run_message_aggregates_job(
        'message_3m_aggregates',
        INTERVAL '3 minutes'
    );
END;
$$;

CALL mqtt_ingest.ensure_message_aggregates_job('refresh_message_3m_aggregates_job');
