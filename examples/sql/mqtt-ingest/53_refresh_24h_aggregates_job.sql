CREATE OR REPLACE PROCEDURE mqtt_ingest.refresh_message_24h_aggregates_job(
    job_id INTEGER,
    config JSONB
)
LANGUAGE plpgsql
AS $$
BEGIN
    CALL mqtt_ingest.run_message_aggregates_job(
        'message_24h_aggregates',
        INTERVAL '24 hours'
    );
END;
$$;

CALL mqtt_ingest.ensure_message_aggregates_job('refresh_message_24h_aggregates_job');
