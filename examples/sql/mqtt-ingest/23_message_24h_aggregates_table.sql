SELECT mqtt_ingest.ensure_message_aggregates_table(
    'message_24h_aggregates',
    INTERVAL '24 hours'
);
