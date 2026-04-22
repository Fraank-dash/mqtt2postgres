CREATE TABLE public.tbl_broker_metrics (
    msg_date  TIMESTAMPTZ NOT NULL,
    msg_topic TEXT NOT NULL,
    msg_value TEXT NOT NULL
);
