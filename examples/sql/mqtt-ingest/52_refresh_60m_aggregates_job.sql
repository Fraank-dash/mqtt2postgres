CREATE OR REPLACE PROCEDURE mqtt_ingest.refresh_message_60m_aggregates_job(
    job_id INTEGER,
    config JSONB
)
LANGUAGE plpgsql
AS $$
BEGIN
    CALL mqtt_ingest.run_message_aggregates_job(
        'message_60m_aggregates',
        INTERVAL '60 minutes'
    );
END;
$$;

CALL mqtt_ingest.ensure_message_aggregates_job('refresh_message_60m_aggregates_job');
