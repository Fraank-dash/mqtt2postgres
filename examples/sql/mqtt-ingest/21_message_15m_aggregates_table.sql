SELECT mqtt_ingest.ensure_message_aggregates_table(
    'message_15m_aggregates',
    INTERVAL '15 minutes'
);
