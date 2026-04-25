CREATE OR REPLACE PROCEDURE mqtt_ingest.refresh_message_3m_aggregates_job(
    job_id INTEGER,
    config JSONB
)
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM mqtt_ingest.refresh_message_3m_aggregates(
        NULL,
        NULL,
        date_trunc('minute', now())
    );
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM timescaledb_information.jobs
        WHERE proc_schema = 'mqtt_ingest'
          AND proc_name = 'refresh_message_3m_aggregates_job'
    ) THEN
        PERFORM add_job(
            'mqtt_ingest.refresh_message_3m_aggregates_job'::REGPROC,
            INTERVAL '1 minute'
        );
    END IF;
END;
$$;
