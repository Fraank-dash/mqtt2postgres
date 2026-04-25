SELECT mqtt_ingest.ensure_message_aggregates_table(
    'message_60m_aggregates',
    INTERVAL '60 minutes'
);
