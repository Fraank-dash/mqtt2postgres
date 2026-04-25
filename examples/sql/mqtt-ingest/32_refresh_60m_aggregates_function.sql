CREATE OR REPLACE FUNCTION mqtt_ingest.refresh_message_60m_aggregates(
    from_time TIMESTAMPTZ DEFAULT NULL,
    to_time TIMESTAMPTZ DEFAULT NULL,
    reference_time TIMESTAMPTZ DEFAULT now()
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM mqtt_ingest.refresh_message_aggregates(
        'message_60m_aggregates',
        INTERVAL '60 minutes',
        from_time,
        to_time,
        reference_time
    );
END;
$$;
