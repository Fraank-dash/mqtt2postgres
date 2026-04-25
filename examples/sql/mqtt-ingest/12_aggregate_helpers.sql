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
            numeric_median     DOUBLE PRECISION,
            numeric_p25        DOUBLE PRECISION,
            numeric_p75        DOUBLE PRECISION,
            numeric_variance_samp DOUBLE PRECISION,
            numeric_stddev_samp DOUBLE PRECISION,
            numeric_stderr     DOUBLE PRECISION,
            numeric_ci95_lower DOUBLE PRECISION,
            numeric_ci95_upper DOUBLE PRECISION,
            first_received_at  TIMESTAMPTZ,
            last_received_at   TIMESTAMPTZ,
            locf_value_at_bucket_start     DOUBLE PRECISION,
            locf_value_at_bucket_end       DOUBLE PRECISION,
            locf_time_weighted_avg         DOUBLE PRECISION,
            linear_value_at_bucket_start   DOUBLE PRECISION,
            linear_value_at_bucket_end     DOUBLE PRECISION,
            linear_time_weighted_avg       DOUBLE PRECISION,
            status             TEXT NOT NULL,
            quality_score      DOUBLE PRECISION,
            quality_boundary_score DOUBLE PRECISION,
            quality_count_score DOUBLE PRECISION,
            quality_stats_score DOUBLE PRECISION,
            interval_gap_count BIGINT,
            interval_gap_avg_seconds DOUBLE PRECISION,
            interval_gap_stddev_seconds DOUBLE PRECISION,
            interval_gap_cv DOUBLE PRECISION,
            quality_interval_score DOUBLE PRECISION,
            quality_flags      TEXT[],
            quality_status     TEXT,
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
            ADD COLUMN IF NOT EXISTS linear_time_weighted_avg DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS numeric_median DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS numeric_p25 DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS numeric_p75 DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS numeric_variance_samp DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS numeric_stddev_samp DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS numeric_stderr DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS numeric_ci95_lower DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS numeric_ci95_upper DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS quality_score DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS quality_boundary_score DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS quality_count_score DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS quality_stats_score DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS interval_gap_count BIGINT,
            ADD COLUMN IF NOT EXISTS interval_gap_avg_seconds DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS interval_gap_stddev_seconds DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS interval_gap_cv DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS quality_interval_score DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS quality_flags TEXT[],
            ADD COLUMN IF NOT EXISTS quality_status TEXT;
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

    EXECUTE format(
        $comment$COMMENT ON TABLE %s IS %L$comment$,
        qualified_table,
        format(
            'Bucketed sensor aggregates for %s windows, including raw bucket statistics, trust metrics, and boundary-aware interpolation fields.',
            bucket_width::TEXT
        )
    );

    EXECUTE format($sql$COMMENT ON COLUMN %s.bucket_start IS 'Aligned bucket start timestamp.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.bucket_end IS 'Aligned bucket end timestamp.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.topic IS 'Representative full MQTT topic retained for traceability.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.device_id IS 'Parsed device identifier from sensors/<device>/<metric> topics.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.metric_name IS 'Parsed metric name from sensors/<device>/<metric> topics.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.sample_count IS 'Total messages observed in the bucket, including non-numeric payloads.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.numeric_count IS 'Count of numeric samples contributing to numeric statistics.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.numeric_avg IS 'Arithmetic mean of raw numeric values inside the bucket.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.numeric_min IS 'Minimum raw numeric value inside the bucket.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.numeric_max IS 'Maximum raw numeric value inside the bucket.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.numeric_median IS 'Median raw numeric value inside the bucket.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.numeric_p25 IS '25th percentile of raw numeric values inside the bucket.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.numeric_p75 IS '75th percentile of raw numeric values inside the bucket.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.numeric_variance_samp IS 'Sample variance of raw numeric values inside the bucket.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.numeric_stddev_samp IS 'Sample standard deviation of raw numeric values inside the bucket.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.numeric_stderr IS 'Standard error of the bucket mean based on numeric_count.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.numeric_ci95_lower IS 'Lower 95 percent confidence bound for the bucket mean.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.numeric_ci95_upper IS 'Upper 95 percent confidence bound for the bucket mean.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.first_received_at IS 'First receive timestamp observed in the bucket.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.last_received_at IS 'Last receive timestamp observed in the bucket.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.locf_value_at_bucket_start IS 'Last-observation-carried-forward value at bucket_start.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.locf_value_at_bucket_end IS 'Last-observation-carried-forward value at bucket_end.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.locf_time_weighted_avg IS 'Boundary-aware LOCF time-weighted average across the bucket.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.linear_value_at_bucket_start IS 'Linearly interpolated value at bucket_start.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.linear_value_at_bucket_end IS 'Linearly interpolated value at bucket_end.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.linear_time_weighted_avg IS 'Boundary-aware linear time-weighted average across the bucket.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.status IS 'Bucket state: tba for open buckets, aggregated for completed buckets.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.quality_score IS 'Final retained-data quality score on a 0.0 to 10.0 scale.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.quality_boundary_score IS 'Quality sub-score for boundary and interpolation completeness.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.quality_count_score IS 'Quality sub-score for numeric sample volume.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.quality_stats_score IS 'Quality sub-score for statistical confidence and spread.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.interval_gap_count IS 'Number of consecutive spacing gaps used for interval-regularity quality.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.interval_gap_avg_seconds IS 'Average spacing in seconds between successive boundary-aware measurement timestamps.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.interval_gap_stddev_seconds IS 'Sample standard deviation of boundary-aware spacing gaps in seconds.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.interval_gap_cv IS 'Coefficient of variation of spacing gaps in seconds.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.quality_interval_score IS 'Quality sub-score for temporal regularity of measurement spacing.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.quality_flags IS 'Machine-readable reasons that reduced or qualified the quality score.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.quality_status IS 'Quality lifecycle: provisional for open buckets, rated for completed buckets.'$sql$, qualified_table);
    EXECUTE format($sql$COMMENT ON COLUMN %s.refreshed_at IS 'Timestamp of the most recent aggregate refresh.'$sql$, qualified_table);
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
            numeric_median,
            numeric_p25,
            numeric_p75,
            numeric_variance_samp,
            numeric_stddev_samp,
            numeric_stderr,
            numeric_ci95_lower,
            numeric_ci95_upper,
            first_received_at,
            last_received_at,
            locf_value_at_bucket_start,
            locf_value_at_bucket_end,
            locf_time_weighted_avg,
            linear_value_at_bucket_start,
            linear_value_at_bucket_end,
            linear_time_weighted_avg,
            status,
            quality_score,
            quality_boundary_score,
            quality_count_score,
            quality_stats_score,
            interval_gap_count,
            interval_gap_avg_seconds,
            interval_gap_stddev_seconds,
            interval_gap_cv,
            quality_interval_score,
            quality_flags,
            quality_status,
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
                percentile_cont(0.5) WITHIN GROUP (ORDER BY numeric_value)
                    FILTER (WHERE numeric_value IS NOT NULL) AS numeric_median,
                percentile_cont(0.25) WITHIN GROUP (ORDER BY numeric_value)
                    FILTER (WHERE numeric_value IS NOT NULL) AS numeric_p25,
                percentile_cont(0.75) WITHIN GROUP (ORDER BY numeric_value)
                    FILTER (WHERE numeric_value IS NOT NULL) AS numeric_p75,
                VAR_SAMP(numeric_value) AS numeric_variance_samp,
                STDDEV_SAMP(numeric_value) AS numeric_stddev_samp,
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
                b.numeric_median,
                b.numeric_p25,
                b.numeric_p75,
                b.numeric_variance_samp,
                b.numeric_stddev_samp,
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
                b.numeric_median,
                b.numeric_p25,
                b.numeric_p75,
                b.numeric_variance_samp,
                b.numeric_stddev_samp,
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
                    WHEN b.numeric_count >= 2
                     AND b.numeric_stddev_samp IS NOT NULL
                    THEN b.numeric_stddev_samp / sqrt(b.numeric_count::DOUBLE PRECISION)
                    ELSE NULL
                END AS numeric_stderr,
                CASE
                    WHEN b.numeric_count >= 2
                     AND b.numeric_avg IS NOT NULL
                     AND b.numeric_stddev_samp IS NOT NULL
                    THEN b.numeric_avg
                        - 1.96 * (b.numeric_stddev_samp / sqrt(b.numeric_count::DOUBLE PRECISION))
                    ELSE NULL
                END AS numeric_ci95_lower,
                CASE
                    WHEN b.numeric_count >= 2
                     AND b.numeric_avg IS NOT NULL
                     AND b.numeric_stddev_samp IS NOT NULL
                    THEN b.numeric_avg
                        + 1.96 * (b.numeric_stddev_samp / sqrt(b.numeric_count::DOUBLE PRECISION))
                    ELSE NULL
                END AS numeric_ci95_upper,
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
        ),
        quality_base AS (
            SELECT
                f.*,
                CASE
                    WHEN f.status = 'aggregated'
                    THEN GREATEST(
                        0.0,
                        10.0
                        - CASE WHEN f.locf_value_at_bucket_start IS NULL THEN 3.0 ELSE 0.0 END
                        - CASE WHEN f.linear_value_at_bucket_start IS NULL THEN 3.0 ELSE 0.0 END
                        - CASE WHEN f.locf_value_at_bucket_end IS NULL THEN 2.0 ELSE 0.0 END
                        - CASE WHEN f.linear_value_at_bucket_end IS NULL THEN 2.0 ELSE 0.0 END
                    )
                    ELSE NULL
                END AS quality_boundary_score,
                CASE
                    WHEN f.status <> 'aggregated' THEN NULL
                    WHEN f.numeric_count >= 50 THEN 10.0
                    WHEN f.numeric_count >= 20 THEN 8.5
                    WHEN f.numeric_count >= 10 THEN 7.0
                    WHEN f.numeric_count >= 5 THEN 5.0
                    WHEN f.numeric_count >= 2 THEN 3.0
                    WHEN f.numeric_count = 1 THEN 1.0
                    ELSE 0.0
                END AS quality_count_score,
                CASE
                    WHEN f.status <> 'aggregated' THEN NULL
                    WHEN f.numeric_count < 2
                      OR f.numeric_stderr IS NULL
                      OR f.numeric_ci95_lower IS NULL
                      OR f.numeric_ci95_upper IS NULL
                    THEN NULL
                    ELSE
                        (f.numeric_ci95_upper - f.numeric_ci95_lower)
                        / GREATEST(
                            ABS(COALESCE(f.numeric_avg, 0.0)),
                            ABS(COALESCE(f.numeric_median, 0.0)),
                            ABS(COALESCE(f.numeric_p75, 0.0) - COALESCE(f.numeric_p25, 0.0)),
                            ABS(COALESCE(f.numeric_max, 0.0) - COALESCE(f.numeric_min, 0.0)),
                            1.0
                        )
                END AS quality_relative_uncertainty
            FROM final_rows f
        ),
        interval_points AS (
            SELECT
                q.bucket_start,
                q.bucket_end,
                q.device_id,
                q.metric_name,
                q.bucket_start AS point_time
            FROM quality_base q
            WHERE q.status = 'aggregated'
              AND q.locf_value_at_bucket_start IS NOT NULL

            UNION ALL

            SELECT
                q.bucket_start,
                q.bucket_end,
                q.device_id,
                q.metric_name,
                m.received_at AS point_time
            FROM quality_base q
            JOIN mqtt_ingest.messages m
              ON m.device_id = q.device_id
             AND m.metric_name = q.metric_name
             AND m.numeric_value IS NOT NULL
             AND m.received_at >= q.bucket_start
             AND m.received_at <= q.bucket_end
            WHERE q.status = 'aggregated'

            UNION ALL

            SELECT
                q.bucket_start,
                q.bucket_end,
                q.device_id,
                q.metric_name,
                q.bucket_end AS point_time
            FROM quality_base q
            WHERE q.status = 'aggregated'
              AND q.locf_value_at_bucket_end IS NOT NULL
        ),
        interval_points_ranked AS (
            SELECT
                ip.bucket_start,
                ip.bucket_end,
                ip.device_id,
                ip.metric_name,
                ip.point_time,
                ROW_NUMBER() OVER (
                    PARTITION BY ip.bucket_start, ip.device_id, ip.metric_name
                    ORDER BY ip.point_time
                ) AS point_order
            FROM (
                SELECT DISTINCT
                    bucket_start,
                    bucket_end,
                    device_id,
                    metric_name,
                    point_time
                FROM interval_points
            ) ip
        ),
        interval_gaps AS (
            SELECT
                ipr.bucket_start,
                ipr.device_id,
                ipr.metric_name,
                EXTRACT(
                    EPOCH FROM (
                        ipr.point_time
                        - LAG(ipr.point_time) OVER (
                            PARTITION BY ipr.bucket_start, ipr.device_id, ipr.metric_name
                            ORDER BY ipr.point_order
                        )
                    )
                ) AS gap_seconds
            FROM interval_points_ranked ipr
        ),
        interval_metrics AS (
            SELECT
                q.bucket_start,
                q.device_id,
                q.metric_name,
                COUNT(ig.gap_seconds) AS interval_gap_count,
                AVG(ig.gap_seconds) AS interval_gap_avg_seconds,
                STDDEV_SAMP(ig.gap_seconds) AS interval_gap_stddev_seconds,
                CASE
                    WHEN COUNT(ig.gap_seconds) >= 2
                     AND AVG(ig.gap_seconds) > 0
                     AND STDDEV_SAMP(ig.gap_seconds) IS NOT NULL
                    THEN STDDEV_SAMP(ig.gap_seconds) / AVG(ig.gap_seconds)
                    ELSE NULL
                END AS interval_gap_cv
            FROM quality_base q
            LEFT JOIN interval_gaps ig
              ON ig.bucket_start = q.bucket_start
             AND ig.device_id = q.device_id
             AND ig.metric_name = q.metric_name
            WHERE q.status = 'aggregated'
            GROUP BY
                q.bucket_start,
                q.device_id,
                q.metric_name
        ),
        quality_rows AS (
            SELECT
                q.bucket_start,
                q.bucket_end,
                q.topic,
                q.device_id,
                q.metric_name,
                q.sample_count,
                q.numeric_count,
                q.numeric_avg,
                q.numeric_min,
                q.numeric_max,
                q.numeric_median,
                q.numeric_p25,
                q.numeric_p75,
                q.numeric_variance_samp,
                q.numeric_stddev_samp,
                q.numeric_stderr,
                q.numeric_ci95_lower,
                q.numeric_ci95_upper,
                q.first_received_at,
                q.last_received_at,
                q.locf_value_at_bucket_start,
                q.locf_value_at_bucket_end,
                q.locf_time_weighted_avg,
                q.linear_value_at_bucket_start,
                q.linear_value_at_bucket_end,
                q.linear_time_weighted_avg,
                q.status,
                CASE
                    WHEN q.status = 'aggregated' THEN 'rated'
                    ELSE 'provisional'
                END AS quality_status,
                q.quality_boundary_score,
                q.quality_count_score,
                im.interval_gap_count,
                im.interval_gap_avg_seconds,
                im.interval_gap_stddev_seconds,
                im.interval_gap_cv,
                CASE
                    WHEN q.status <> 'aggregated' THEN NULL
                    WHEN q.numeric_count = 0 THEN 0.0
                    WHEN q.numeric_count = 1 THEN 1.0
                    WHEN q.quality_relative_uncertainty IS NULL THEN 1.0
                    WHEN q.quality_relative_uncertainty <= 0.10 THEN 10.0
                    WHEN q.quality_relative_uncertainty <= 0.25 THEN 8.0
                    WHEN q.quality_relative_uncertainty <= 0.50 THEN 6.0
                    WHEN q.quality_relative_uncertainty <= 1.00 THEN 4.0
                    ELSE 2.0
                END AS quality_stats_score,
                CASE
                    WHEN q.status <> 'aggregated' THEN NULL
                    WHEN im.interval_gap_cv IS NULL THEN 1.0
                    WHEN im.interval_gap_cv <= 0.05 THEN 10.0
                    WHEN im.interval_gap_cv <= 0.10 THEN 9.0
                    WHEN im.interval_gap_cv <= 0.20 THEN 8.0
                    WHEN im.interval_gap_cv <= 0.35 THEN 6.0
                    WHEN im.interval_gap_cv <= 0.50 THEN 4.0
                    ELSE 2.0
                END AS quality_interval_score,
                CASE
                    WHEN q.status <> 'aggregated' THEN NULL
                    ELSE array_remove(
                        ARRAY[
                            CASE WHEN q.locf_value_at_bucket_start IS NULL THEN 'missing_locf_start' END,
                            CASE WHEN q.linear_value_at_bucket_start IS NULL THEN 'missing_linear_start' END,
                            CASE WHEN q.locf_value_at_bucket_end IS NULL THEN 'missing_locf_end' END,
                            CASE WHEN q.linear_value_at_bucket_end IS NULL THEN 'missing_linear_end' END,
                            CASE WHEN q.numeric_count < 5 THEN 'low_numeric_count' END,
                            CASE WHEN q.numeric_count = 1 THEN 'single_numeric_sample' END,
                            CASE WHEN q.quality_relative_uncertainty > 0.50 THEN 'high_mean_uncertainty' END,
                            CASE WHEN COALESCE(im.interval_gap_count, 0) < 2 THEN 'insufficient_interval_support' END,
                            CASE WHEN im.interval_gap_cv > 0.35 THEN 'irregular_measurement_intervals' END
                        ]::TEXT[],
                        NULL
                    )
                END AS quality_flags,
                CASE
                    WHEN q.status <> 'aggregated' THEN NULL
                    ELSE ROUND(
                        (
                            COALESCE(q.quality_boundary_score, 0.0)
                            + COALESCE(q.quality_count_score, 0.0)
                            + CASE
                                WHEN q.numeric_count = 0 THEN 0.0
                                WHEN q.numeric_count = 1 THEN 1.0
                                WHEN q.quality_relative_uncertainty IS NULL THEN 1.0
                                WHEN q.quality_relative_uncertainty <= 0.10 THEN 10.0
                                WHEN q.quality_relative_uncertainty <= 0.25 THEN 8.0
                                WHEN q.quality_relative_uncertainty <= 0.50 THEN 6.0
                                WHEN q.quality_relative_uncertainty <= 1.00 THEN 4.0
                                ELSE 2.0
                            END
                            + CASE
                                WHEN im.interval_gap_cv IS NULL THEN 1.0
                                WHEN im.interval_gap_cv <= 0.05 THEN 10.0
                                WHEN im.interval_gap_cv <= 0.10 THEN 9.0
                                WHEN im.interval_gap_cv <= 0.20 THEN 8.0
                                WHEN im.interval_gap_cv <= 0.35 THEN 6.0
                                WHEN im.interval_gap_cv <= 0.50 THEN 4.0
                                ELSE 2.0
                            END
                        ) / 4.0,
                        2
                    )
                END AS quality_score,
                q.refreshed_at
            FROM quality_base q
            LEFT JOIN interval_metrics im
              ON im.bucket_start = q.bucket_start
             AND im.device_id = q.device_id
             AND im.metric_name = q.metric_name
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
            numeric_median,
            numeric_p25,
            numeric_p75,
            numeric_variance_samp,
            numeric_stddev_samp,
            numeric_stderr,
            numeric_ci95_lower,
            numeric_ci95_upper,
            first_received_at,
            last_received_at,
            locf_value_at_bucket_start,
            locf_value_at_bucket_end,
            locf_time_weighted_avg,
            linear_value_at_bucket_start,
            linear_value_at_bucket_end,
            linear_time_weighted_avg,
            status,
            quality_score,
            quality_boundary_score,
            quality_count_score,
            quality_stats_score,
            interval_gap_count,
            interval_gap_avg_seconds,
            interval_gap_stddev_seconds,
            interval_gap_cv,
            quality_interval_score,
            quality_flags,
            quality_status,
            refreshed_at
        FROM quality_rows
        ON CONFLICT (bucket_start, device_id, metric_name) DO UPDATE SET
            bucket_end = EXCLUDED.bucket_end,
            topic = EXCLUDED.topic,
            sample_count = EXCLUDED.sample_count,
            numeric_count = EXCLUDED.numeric_count,
            numeric_avg = EXCLUDED.numeric_avg,
            numeric_min = EXCLUDED.numeric_min,
            numeric_max = EXCLUDED.numeric_max,
            numeric_median = EXCLUDED.numeric_median,
            numeric_p25 = EXCLUDED.numeric_p25,
            numeric_p75 = EXCLUDED.numeric_p75,
            numeric_variance_samp = EXCLUDED.numeric_variance_samp,
            numeric_stddev_samp = EXCLUDED.numeric_stddev_samp,
            numeric_stderr = EXCLUDED.numeric_stderr,
            numeric_ci95_lower = EXCLUDED.numeric_ci95_lower,
            numeric_ci95_upper = EXCLUDED.numeric_ci95_upper,
            first_received_at = EXCLUDED.first_received_at,
            last_received_at = EXCLUDED.last_received_at,
            locf_value_at_bucket_start = EXCLUDED.locf_value_at_bucket_start,
            locf_value_at_bucket_end = EXCLUDED.locf_value_at_bucket_end,
            locf_time_weighted_avg = EXCLUDED.locf_time_weighted_avg,
            linear_value_at_bucket_start = EXCLUDED.linear_value_at_bucket_start,
            linear_value_at_bucket_end = EXCLUDED.linear_value_at_bucket_end,
            linear_time_weighted_avg = EXCLUDED.linear_time_weighted_avg,
            status = EXCLUDED.status,
            quality_score = EXCLUDED.quality_score,
            quality_boundary_score = EXCLUDED.quality_boundary_score,
            quality_count_score = EXCLUDED.quality_count_score,
            quality_stats_score = EXCLUDED.quality_stats_score,
            interval_gap_count = EXCLUDED.interval_gap_count,
            interval_gap_avg_seconds = EXCLUDED.interval_gap_avg_seconds,
            interval_gap_stddev_seconds = EXCLUDED.interval_gap_stddev_seconds,
            interval_gap_cv = EXCLUDED.interval_gap_cv,
            quality_interval_score = EXCLUDED.quality_interval_score,
            quality_flags = EXCLUDED.quality_flags,
            quality_status = EXCLUDED.quality_status,
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
