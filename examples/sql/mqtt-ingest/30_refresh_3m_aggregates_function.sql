CREATE OR REPLACE FUNCTION mqtt_ingest.refresh_message_3m_aggregates(
    from_time TIMESTAMPTZ DEFAULT NULL,
    to_time TIMESTAMPTZ DEFAULT NULL,
    reference_time TIMESTAMPTZ DEFAULT now()
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    refresh_from TIMESTAMPTZ;
    refresh_to TIMESTAMPTZ;
BEGIN
    refresh_from := COALESCE(
        time_bucket(INTERVAL '3 minutes', from_time),
        (SELECT time_bucket(INTERVAL '3 minutes', MIN(received_at)) FROM mqtt_ingest.messages)
    );
    refresh_to := COALESCE(
        time_bucket(INTERVAL '3 minutes', to_time) + INTERVAL '3 minutes',
        date_trunc('minute', reference_time) + INTERVAL '1 minute'
    );

    IF refresh_from IS NULL THEN
        RETURN;
    END IF;

    INSERT INTO mqtt_ingest.message_3m_aggregates (
        bucket_start,
        bucket_end,
        topic,
        sample_count,
        numeric_count,
        numeric_avg,
        numeric_min,
        numeric_max,
        first_received_at,
        last_received_at,
        locf_value_at_bucket_start,
        locf_value_at_bucket_end,
        locf_time_weighted_avg,
        linear_value_at_bucket_start,
        linear_value_at_bucket_end,
        linear_time_weighted_avg,
        status,
        refreshed_at
    )
    WITH bucket_base AS (
        SELECT
            time_bucket(INTERVAL '3 minutes', received_at) AS bucket_start,
            time_bucket(INTERVAL '3 minutes', received_at) + INTERVAL '3 minutes' AS bucket_end,
            topic,
            COUNT(*) AS sample_count,
            COUNT(numeric_value) AS numeric_count,
            AVG(numeric_value) AS numeric_avg,
            MIN(numeric_value) AS numeric_min,
            MAX(numeric_value) AS numeric_max,
            MIN(received_at) AS first_received_at,
            MAX(received_at) AS last_received_at
        FROM mqtt_ingest.messages
        WHERE received_at >= refresh_from
          AND received_at < refresh_to
        GROUP BY time_bucket(INTERVAL '3 minutes', received_at), topic
    ),
    topic_scope AS (
        SELECT DISTINCT topic
        FROM bucket_base
    ),
    numeric_bucket_summaries AS (
        SELECT
            time_bucket(INTERVAL '3 minutes', received_at) AS bucket_start,
            topic,
            time_weight('LOCF', received_at, numeric_value) AS locf_tws,
            time_weight('Linear', received_at, numeric_value) AS linear_tws
        FROM mqtt_ingest.messages
        WHERE numeric_value IS NOT NULL
          AND topic IN (SELECT topic FROM topic_scope)
        GROUP BY time_bucket(INTERVAL '3 minutes', received_at), topic
    ),
    summary_windows AS (
        SELECT
            bucket_start,
            topic,
            locf_tws,
            linear_tws,
            LAG(locf_tws) OVER (PARTITION BY topic ORDER BY bucket_start) AS prev_locf_tws,
            LEAD(locf_tws) OVER (PARTITION BY topic ORDER BY bucket_start) AS next_locf_tws,
            LAG(linear_tws) OVER (PARTITION BY topic ORDER BY bucket_start) AS prev_linear_tws,
            LEAD(linear_tws) OVER (PARTITION BY topic ORDER BY bucket_start) AS next_linear_tws
        FROM numeric_bucket_summaries
    ),
    bucket_boundaries AS (
        SELECT
            b.bucket_start,
            b.bucket_end,
            b.topic,
            b.sample_count,
            b.numeric_count,
            b.numeric_avg,
            b.numeric_min,
            b.numeric_max,
            b.first_received_at,
            b.last_received_at,
            start_prev.received_at AS start_prev_received_at,
            start_prev.numeric_value AS start_prev_numeric_value,
            start_next.received_at AS start_next_received_at,
            start_next.numeric_value AS start_next_numeric_value,
            end_prev.received_at AS end_prev_received_at,
            end_prev.numeric_value AS end_prev_numeric_value,
            end_next.received_at AS end_next_received_at,
            end_next.numeric_value AS end_next_numeric_value
        FROM bucket_base b
        LEFT JOIN LATERAL (
            SELECT received_at, numeric_value
            FROM mqtt_ingest.messages
            WHERE topic = b.topic
              AND numeric_value IS NOT NULL
              AND received_at <= b.bucket_start
            ORDER BY received_at DESC
            LIMIT 1
        ) AS start_prev ON TRUE
        LEFT JOIN LATERAL (
            SELECT received_at, numeric_value
            FROM mqtt_ingest.messages
            WHERE topic = b.topic
              AND numeric_value IS NOT NULL
              AND received_at >= b.bucket_start
            ORDER BY received_at ASC
            LIMIT 1
        ) AS start_next ON TRUE
        LEFT JOIN LATERAL (
            SELECT received_at, numeric_value
            FROM mqtt_ingest.messages
            WHERE topic = b.topic
              AND numeric_value IS NOT NULL
              AND received_at <= b.bucket_end
            ORDER BY received_at DESC
            LIMIT 1
        ) AS end_prev ON TRUE
        LEFT JOIN LATERAL (
            SELECT received_at, numeric_value
            FROM mqtt_ingest.messages
            WHERE topic = b.topic
              AND numeric_value IS NOT NULL
              AND received_at >= b.bucket_end
            ORDER BY received_at ASC
            LIMIT 1
        ) AS end_next ON TRUE
    ),
    final_rows AS (
        SELECT
            b.bucket_start,
            b.bucket_end,
            b.topic,
            b.sample_count,
            b.numeric_count,
            b.numeric_avg,
            b.numeric_min,
            b.numeric_max,
            b.first_received_at,
            b.last_received_at,
            CASE
                WHEN b.start_prev_received_at IS NOT NULL
                THEN b.start_prev_numeric_value
                ELSE NULL
            END AS locf_value_at_bucket_start,
            CASE
                WHEN b.end_prev_received_at IS NOT NULL
                 AND b.end_next_received_at IS NOT NULL
                THEN b.end_prev_numeric_value
                ELSE NULL
            END AS locf_value_at_bucket_end,
            CASE
                WHEN sw.locf_tws IS NOT NULL
                 AND b.start_prev_received_at IS NOT NULL
                 AND b.end_prev_received_at IS NOT NULL
                 AND b.end_next_received_at IS NOT NULL
                THEN interpolated_average(
                    sw.locf_tws,
                    b.bucket_start,
                    INTERVAL '3 minutes',
                    sw.prev_locf_tws,
                    sw.next_locf_tws
                )
                ELSE NULL
            END AS locf_time_weighted_avg,
            CASE
                WHEN b.start_prev_received_at IS NOT NULL
                 AND b.start_next_received_at IS NOT NULL
                THEN CASE
                    WHEN b.start_prev_received_at = b.start_next_received_at
                    THEN b.start_prev_numeric_value
                    ELSE b.start_prev_numeric_value
                        + (b.start_next_numeric_value - b.start_prev_numeric_value)
                            * (
                                EXTRACT(EPOCH FROM (b.bucket_start - b.start_prev_received_at))
                                / NULLIF(EXTRACT(EPOCH FROM (b.start_next_received_at - b.start_prev_received_at)), 0)
                            )
                END
                ELSE NULL
            END AS linear_value_at_bucket_start,
            CASE
                WHEN b.end_prev_received_at IS NOT NULL
                 AND b.end_next_received_at IS NOT NULL
                THEN CASE
                    WHEN b.end_prev_received_at = b.end_next_received_at
                    THEN b.end_prev_numeric_value
                    ELSE b.end_prev_numeric_value
                        + (b.end_next_numeric_value - b.end_prev_numeric_value)
                            * (
                                EXTRACT(EPOCH FROM (b.bucket_end - b.end_prev_received_at))
                                / NULLIF(EXTRACT(EPOCH FROM (b.end_next_received_at - b.end_prev_received_at)), 0)
                            )
                END
                ELSE NULL
            END AS linear_value_at_bucket_end,
            CASE
                WHEN sw.linear_tws IS NOT NULL
                 AND b.start_prev_received_at IS NOT NULL
                 AND b.start_next_received_at IS NOT NULL
                 AND b.end_prev_received_at IS NOT NULL
                 AND b.end_next_received_at IS NOT NULL
                THEN interpolated_average(
                    sw.linear_tws,
                    b.bucket_start,
                    INTERVAL '3 minutes',
                    sw.prev_linear_tws,
                    sw.next_linear_tws
                )
                ELSE NULL
            END AS linear_time_weighted_avg,
            CASE
                WHEN b.bucket_end <= date_trunc('minute', reference_time)
                THEN 'aggregated'
                ELSE 'tba'
            END AS status,
            now() AS refreshed_at
        FROM bucket_boundaries b
        LEFT JOIN summary_windows sw
          ON sw.bucket_start = b.bucket_start
         AND sw.topic = b.topic
    )
    SELECT
        bucket_start,
        bucket_end,
        topic,
        sample_count,
        numeric_count,
        numeric_avg,
        numeric_min,
        numeric_max,
        first_received_at,
        last_received_at,
        locf_value_at_bucket_start,
        locf_value_at_bucket_end,
        locf_time_weighted_avg,
        linear_value_at_bucket_start,
        linear_value_at_bucket_end,
        linear_time_weighted_avg,
        status,
        refreshed_at
    FROM final_rows
    ON CONFLICT (bucket_start, topic) DO UPDATE SET
        bucket_end = EXCLUDED.bucket_end,
        sample_count = EXCLUDED.sample_count,
        numeric_count = EXCLUDED.numeric_count,
        numeric_avg = EXCLUDED.numeric_avg,
        numeric_min = EXCLUDED.numeric_min,
        numeric_max = EXCLUDED.numeric_max,
        first_received_at = EXCLUDED.first_received_at,
        last_received_at = EXCLUDED.last_received_at,
        locf_value_at_bucket_start = EXCLUDED.locf_value_at_bucket_start,
        locf_value_at_bucket_end = EXCLUDED.locf_value_at_bucket_end,
        locf_time_weighted_avg = EXCLUDED.locf_time_weighted_avg,
        linear_value_at_bucket_start = EXCLUDED.linear_value_at_bucket_start,
        linear_value_at_bucket_end = EXCLUDED.linear_value_at_bucket_end,
        linear_time_weighted_avg = EXCLUDED.linear_time_weighted_avg,
        status = EXCLUDED.status,
        refreshed_at = EXCLUDED.refreshed_at;

    UPDATE mqtt_ingest.message_3m_aggregates
    SET status = 'aggregated',
        refreshed_at = now()
    WHERE status = 'tba'
      AND bucket_end <= date_trunc('minute', reference_time);
END;
$$;
