CREATE OR REPLACE FUNCTION mqtt_ingest.refresh_message_24h_aggregates(
    from_time TIMESTAMPTZ DEFAULT NULL,
    to_time TIMESTAMPTZ DEFAULT NULL,
    reference_time TIMESTAMPTZ DEFAULT now()
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM mqtt_ingest.refresh_message_aggregates(
        'message_24h_aggregates',
        INTERVAL '24 hours',
        from_time,
        to_time,
        reference_time
    );
END;
$$;
