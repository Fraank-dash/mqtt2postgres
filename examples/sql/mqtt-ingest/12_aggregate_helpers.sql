CREATE OR REPLACE FUNCTION mqtt_ingest.ensure_message_aggregates_table(
    table_suffix TEXT,
    bucket_width INTERVAL
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    qualified_table TEXT := format('mqtt_ingest.%I', table_suffix);
    unique_constraint_name TEXT := format('%s_device_bucket_key', table_suffix);
    status_constraint_name TEXT := format('%s_status_check', table_suffix);
    bucket_constraint_name TEXT := format('%s_bucket_check', table_suffix);
BEGIN
    EXECUTE format(
        $sql$
        CREATE TABLE IF NOT EXISTS %s (
            bucket_start       TIMESTAMPTZ NOT NULL,
            bucket_end         TIMESTAMPTZ NOT NULL,
            topic              TEXT NOT NULL,
            device_id          TEXT,
            metric_name        TEXT,
            sample_count       BIGINT NOT NULL,
            numeric_count      BIGINT NOT NULL,
            numeric_avg        DOUBLE PRECISION,
            numeric_min        DOUBLE PRECISION,
            numeric_max        DOUBLE PRECISION,
            first_received_at  TIMESTAMPTZ,
            last_received_at   TIMESTAMPTZ,
            locf_value_at_bucket_start     DOUBLE PRECISION,
            locf_value_at_bucket_end       DOUBLE PRECISION,
            locf_time_weighted_avg         DOUBLE PRECISION,
            linear_value_at_bucket_start   DOUBLE PRECISION,
            linear_value_at_bucket_end     DOUBLE PRECISION,
            linear_time_weighted_avg       DOUBLE PRECISION,
            status             TEXT NOT NULL,
            refreshed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (bucket_start, topic),
            CONSTRAINT %I
                UNIQUE (bucket_start, device_id, metric_name),
            CONSTRAINT %I
                CHECK (status IN ('aggregated', 'tba')),
            CONSTRAINT %I
                CHECK (bucket_end = bucket_start + %L::INTERVAL)
        );
        $sql$,
        qualified_table,
        unique_constraint_name,
        status_constraint_name,
        bucket_constraint_name,
        bucket_width::TEXT
    );

    PERFORM create_hypertable(
        qualified_table,
        'bucket_start',
        if_not_exists => TRUE
    );

    EXECUTE format(
        $sql$
        ALTER TABLE %s
            ADD COLUMN IF NOT EXISTS device_id TEXT,
            ADD COLUMN IF NOT EXISTS metric_name TEXT,
            ADD COLUMN IF NOT EXISTS locf_value_at_bucket_start DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS locf_value_at_bucket_end DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS locf_time_weighted_avg DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS linear_value_at_bucket_start DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS linear_value_at_bucket_end DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS linear_time_weighted_avg DOUBLE PRECISION;
        $sql$,
        qualified_table
    );

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = unique_constraint_name
          AND conrelid = to_regclass(qualified_table)
    ) THEN
        EXECUTE format(
            'ALTER TABLE %s ADD CONSTRAINT %I UNIQUE (bucket_start, device_id, metric_name)',
            qualified_table,
            unique_constraint_name
        );
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION mqtt_ingest.refresh_message_aggregates(
    table_suffix TEXT,
    bucket_width INTERVAL,
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
    qualified_table TEXT := format('mqtt_ingest.%I', table_suffix);
    bucket_width_text TEXT := bucket_width::TEXT;
BEGIN
    refresh_from := COALESCE(
        time_bucket(bucket_width, from_time),
        (SELECT time_bucket(bucket_width, MIN(received_at)) FROM mqtt_ingest.messages)
    );
    refresh_to := COALESCE(
        time_bucket(bucket_width, to_time) + bucket_width,
        date_trunc('minute', reference_time) + INTERVAL '1 minute'
    );

    IF refresh_from IS NULL THEN
        RETURN;
    END IF;

    EXECUTE format(
        $sql$
        INSERT INTO %s (
            bucket_start,
            bucket_end,
            topic,
            device_id,
            metric_name,
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
                time_bucket(%L::INTERVAL, received_at) AS bucket_start,
                time_bucket(%L::INTERVAL, received_at) + %L::INTERVAL AS bucket_end,
                MIN(topic) AS topic,
                device_id,
                metric_name,
                COUNT(*) AS sample_count,
                COUNT(numeric_value) AS numeric_count,
                AVG(numeric_value) AS numeric_avg,
                MIN(numeric_value) AS numeric_min,
                MAX(numeric_value) AS numeric_max,
                MIN(received_at) AS first_received_at,
                MAX(received_at) AS last_received_at
            FROM mqtt_ingest.messages
            WHERE received_at >= $1
              AND received_at < $2
              AND device_id IS NOT NULL
              AND metric_name IS NOT NULL
            GROUP BY
                time_bucket(%L::INTERVAL, received_at),
                device_id,
                metric_name
        ),
        topic_scope AS (
            SELECT DISTINCT device_id, metric_name
            FROM bucket_base
        ),
        numeric_bucket_summaries AS (
            SELECT
                time_bucket(%L::INTERVAL, received_at) AS bucket_start,
                device_id,
                metric_name,
                time_weight('LOCF', received_at, numeric_value) AS locf_tws,
                time_weight('Linear', received_at, numeric_value) AS linear_tws
            FROM mqtt_ingest.messages
            WHERE numeric_value IS NOT NULL
              AND device_id IS NOT NULL
              AND metric_name IS NOT NULL
              AND (device_id, metric_name) IN (
                  SELECT device_id, metric_name
                  FROM topic_scope
              )
            GROUP BY
                time_bucket(%L::INTERVAL, received_at),
                device_id,
                metric_name
        ),
        summary_windows AS (
            SELECT
                bucket_start,
                device_id,
                metric_name,
                locf_tws,
                linear_tws,
                LAG(locf_tws) OVER (
                    PARTITION BY device_id, metric_name
                    ORDER BY bucket_start
                ) AS prev_locf_tws,
                LEAD(locf_tws) OVER (
                    PARTITION BY device_id, metric_name
                    ORDER BY bucket_start
                ) AS next_locf_tws,
                LAG(linear_tws) OVER (
                    PARTITION BY device_id, metric_name
                    ORDER BY bucket_start
                ) AS prev_linear_tws,
                LEAD(linear_tws) OVER (
                    PARTITION BY device_id, metric_name
                    ORDER BY bucket_start
                ) AS next_linear_tws
            FROM numeric_bucket_summaries
        ),
        bucket_boundaries AS (
            SELECT
                b.bucket_start,
                b.bucket_end,
                b.topic,
                b.device_id,
                b.metric_name,
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
                WHERE device_id = b.device_id
                  AND metric_name = b.metric_name
                  AND numeric_value IS NOT NULL
                  AND received_at <= b.bucket_start
                ORDER BY received_at DESC
                LIMIT 1
            ) AS start_prev ON TRUE
            LEFT JOIN LATERAL (
                SELECT received_at, numeric_value
                FROM mqtt_ingest.messages
                WHERE device_id = b.device_id
                  AND metric_name = b.metric_name
                  AND numeric_value IS NOT NULL
                  AND received_at >= b.bucket_start
                ORDER BY received_at ASC
                LIMIT 1
            ) AS start_next ON TRUE
            LEFT JOIN LATERAL (
                SELECT received_at, numeric_value
                FROM mqtt_ingest.messages
                WHERE device_id = b.device_id
                  AND metric_name = b.metric_name
                  AND numeric_value IS NOT NULL
                  AND received_at <= b.bucket_end
                ORDER BY received_at DESC
                LIMIT 1
            ) AS end_prev ON TRUE
            LEFT JOIN LATERAL (
                SELECT received_at, numeric_value
                FROM mqtt_ingest.messages
                WHERE device_id = b.device_id
                  AND metric_name = b.metric_name
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
                b.device_id,
                b.metric_name,
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
                        %L::INTERVAL,
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
                        %L::INTERVAL,
                        sw.prev_linear_tws,
                        sw.next_linear_tws
                    )
                    ELSE NULL
                END AS linear_time_weighted_avg,
                CASE
                    WHEN b.bucket_end <= date_trunc('minute', $3)
                    THEN 'aggregated'
                    ELSE 'tba'
                END AS status,
                now() AS refreshed_at
            FROM bucket_boundaries b
            LEFT JOIN summary_windows sw
              ON sw.bucket_start = b.bucket_start
             AND sw.device_id = b.device_id
             AND sw.metric_name = b.metric_name
        )
        SELECT
            bucket_start,
            bucket_end,
            topic,
            device_id,
            metric_name,
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
        ON CONFLICT (bucket_start, device_id, metric_name) DO UPDATE SET
            bucket_end = EXCLUDED.bucket_end,
            topic = EXCLUDED.topic,
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
            refreshed_at = EXCLUDED.refreshed_at
        $sql$,
        qualified_table,
        bucket_width_text,
        bucket_width_text,
        bucket_width_text,
        bucket_width_text,
        bucket_width_text,
        bucket_width_text,
        bucket_width_text,
        bucket_width_text,
        bucket_width_text
    )
    USING refresh_from, refresh_to, reference_time;
END;
$$;

CREATE OR REPLACE PROCEDURE mqtt_ingest.run_message_aggregates_job(
    table_suffix TEXT,
    bucket_width INTERVAL
)
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM mqtt_ingest.refresh_message_aggregates(
        table_suffix,
        bucket_width,
        NULL,
        NULL,
        date_trunc('minute', now())
    );
END;
$$;

CREATE OR REPLACE PROCEDURE mqtt_ingest.ensure_message_aggregates_job(
    procedure_name TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM timescaledb_information.jobs
        WHERE proc_schema = 'mqtt_ingest'
          AND proc_name = procedure_name
    ) THEN
        PERFORM add_job(
            format('mqtt_ingest.%I', procedure_name)::REGPROC,
            INTERVAL '1 minute'
        );
    END IF;
END;
$$;
