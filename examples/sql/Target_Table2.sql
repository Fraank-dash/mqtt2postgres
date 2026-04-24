CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE public.tbl_broker_metrics (
    msg_date      TIMESTAMPTZ NOT NULL,
    msg_topic     TEXT NOT NULL,
    msg_value     TEXT NOT NULL,
    event_id      TEXT,
    trace_id      TEXT,
    publisher_id  TEXT,
    sequence      BIGINT,
    published_at  TIMESTAMPTZ,
    received_at   TIMESTAMPTZ,
    committed_at  TIMESTAMPTZ
);

SELECT create_hypertable('public.tbl_broker_metrics', 'msg_date', if_not_exists => TRUE);
