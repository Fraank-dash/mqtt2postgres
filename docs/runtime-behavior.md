# Runtime Behavior

## Subscription

The app subscribes to every topic filter listed in the subscriber JSON config file.

When a message arrives, the subscriber confirms that the topic still matches one configured filter and then passes the raw MQTT message to the configured database function. The Python process does not validate target tables or payload schema.

See [Ingest pipeline](ingest-pipeline.md) for the subscriber-to-database flowchart and table/function overview.

## Database Ingest

The default local TimescaleDB bootstrap creates:

- schema `mqtt_ingest`
- hypertable `mqtt_ingest.messages`
- hypertable `mqtt_ingest.message_3m_aggregates`
- hypertable `mqtt_ingest.message_15m_aggregates`
- hypertable `mqtt_ingest.message_60m_aggregates`
- hypertable `mqtt_ingest.message_24h_aggregates`
- table `mqtt_ingest.topic_overview`
- extensions `timescaledb` and `timescaledb_toolkit`
- function `mqtt_ingest.ingest_message(topic text, payload text, received_at timestamptz, metadata jsonb)`
- function `mqtt_ingest.ingest_topics(topic text, payload text, received_at timestamptz, metadata jsonb)`
- function `mqtt_ingest.refresh_message_3m_aggregates(...)`
- function `mqtt_ingest.refresh_message_15m_aggregates(...)`
- function `mqtt_ingest.refresh_message_60m_aggregates(...)`
- function `mqtt_ingest.refresh_message_24h_aggregates(...)`
- TimescaleDB background job `mqtt_ingest.refresh_message_3m_aggregates_job`
- TimescaleDB background job `mqtt_ingest.refresh_message_15m_aggregates_job`
- TimescaleDB background job `mqtt_ingest.refresh_message_60m_aggregates_job`
- TimescaleDB background job `mqtt_ingest.refresh_message_24h_aggregates_job`

The function stores every message in the generic hypertable. It keeps the raw payload, extracts a numeric value when possible, and accepts both plain numeric payloads and the traced JSON payloads emitted by the local publisher.

For topics that match exactly `sensors/<device>/<metric>`, the ingest function also stores parsed `device_id` and `metric_name` columns. Non-matching topics stay in the raw table, but those parsed columns remain `NULL`.

Stored fields include topic, parsed device and metric dimensions when available, payload, numeric value, trace identifiers, publisher id, sequence, publish/receive/commit timestamps, and metadata JSON.

## Topic Overview

The database also stores a broker topic inventory in `mqtt_ingest.topic_overview`.

Each row tracks one distinct topic with first-seen time, last-seen time, message count, and the most recent trace-related metadata. This is intended for operational visibility rather than time-series aggregation. In the local stack, the overview subscriber explicitly subscribes to both `#` and `$SYS/#` so broker status topics are included too.

## Aggregates

The database stores boundary-aware aggregates in:

- `mqtt_ingest.message_3m_aggregates`
- `mqtt_ingest.message_15m_aggregates`
- `mqtt_ingest.message_60m_aggregates`
- `mqtt_ingest.message_24h_aggregates`

Buckets are aligned with TimescaleDB `time_bucket(...)` at 3-minute, 15-minute, 60-minute, and 24-hour widths. For parsed sensor topics, each row is grouped by bucket, `device_id`, and `metric_name`, while retaining the full topic for traceability. Rows include sample count, numeric count, average, minimum, maximum, median, 25th percentile, 75th percentile, variance, standard deviation, standard error, 95% confidence bounds, first receive time, last receive time, explicit bucket-boundary values, boundary-aware time-weighted averages, interval-regularity metrics, a technical `status`, an analytical `quality_score`, and refresh time.

Only topics that match exactly `sensors/<device>/<metric>` participate in device-level aggregation. A subscription such as `sensors/+/temp` therefore produces one aggregate row per 3-minute bucket per device, for example separate rows for `node-1/temp` and `node-2/temp`.

Boundary-aware fields are additive; `numeric_avg` remains the plain in-bucket `AVG(numeric_value)`.

Additional numeric fields:

- `numeric_median`: median raw numeric value inside the bucket.
- `numeric_p25` and `numeric_p75`: 25th and 75th percentiles of raw numeric values inside the bucket.
- `numeric_variance_samp` and `numeric_stddev_samp`: sample variance and sample standard deviation of the raw numeric values inside the bucket.
- `numeric_stderr`: standard error of `numeric_avg`, calculated as `numeric_stddev_samp / sqrt(numeric_count)`.
- `numeric_ci95_lower` and `numeric_ci95_upper`: 95% confidence bounds for `numeric_avg`, calculated as `numeric_avg ± 1.96 * numeric_stderr`.
- `locf_value_at_bucket_start` and `locf_value_at_bucket_end`: last-observation-carried-forward values at the bucket boundaries.
- `linear_value_at_bucket_start` and `linear_value_at_bucket_end`: linearly interpolated values at the bucket boundaries based on the nearest numeric samples before and after each boundary.
- `locf_time_weighted_avg` and `linear_time_weighted_avg`: boundary-aware time-weighted averages calculated with Timescale Toolkit `time_weight(...)` and `interpolated_average(...)`.
- `interval_gap_count`, `interval_gap_avg_seconds`, `interval_gap_stddev_seconds`, and `interval_gap_cv`: boundary-inclusive spacing metrics derived from bucket start, in-bucket numeric timestamps, and bucket end when boundary support exists.

The percentile fields are based only on raw numeric samples inside the bucket and remain `NULL` when there are no numeric values. The trust fields are based only on raw numeric samples inside the bucket and stay `NULL` when `numeric_count < 2`. Boundary fields stay `NULL` when the required surrounding numeric samples do not exist. For the latest open bucket, end-side fields stay `NULL` until a numeric sample at or after `bucket_end` exists.

Aggregate status values:

- `aggregated`: the bucket end is in the past at the last refresh.
- `tba`: the bucket has started but has not ended yet.

Aggregate quality fields:

- `quality_status`: `provisional` for open buckets and `rated` for completed buckets.
- `quality_score`: 0.0 to 10.0 retained-data quality score for completed buckets.
- `quality_boundary_score`, `quality_count_score`, `quality_stats_score`, and `quality_interval_score`: explainable quality sub-scores.
- `quality_flags`: machine-readable reasons such as missing boundary support, low sample count, high mean uncertainty, or irregular measurement spacing.

See [Aggregate status and quality](aggregate-status-and-quality.md) for the scoring thresholds, state transitions, and Mermaid statechart.

The ingest function refreshes the touched 3-minute, 15-minute, 60-minute, and 24-hour buckets immediately so ongoing rows appear on the fly as `tba`. TimescaleDB background jobs refresh those aggregate tables once per minute so completed buckets move to `aggregated`.

## Logging

The runtime writes logs to stdout/stderr.

- `log_format: "json"` is the default and is intended for Docker.
- `log_format: "text"` is intended for local development and debugger sessions.
- `log_level: "INFO"` is the default verbosity.

Typical event names:

- `service.starting`
- `mqtt.connected`
- `mqtt.subscribed`
- `message.routed`
- `message.unrouted`
- `db.write_succeeded`

MQTT payload bodies are not logged. Logs include metadata such as topic, payload size, matching topic filter, status, and error details.

Local verbose run:

```bash
export POSTGRES_USERNAME=postgres
export POSTGRES_PASSWORD=postgres

python main.py --config path/to/subscriber.json
```
