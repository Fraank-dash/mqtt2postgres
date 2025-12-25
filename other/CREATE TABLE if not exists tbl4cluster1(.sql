CREATE TABLE if not exists tbl4cluster1(
msg_date   TIMESTAMPTZ NOT NULL,
msg_topic  TEXT        NOT NULL,
msg_value  TEXT        NOT NULL
);
SELECT create_hypertable('tbl4cluster1', by_range('msg_date'));

CREATE TABLE if not exists tbl4cluster2(
msg_date   TIMESTAMPTZ NOT NULL,
msg_topic  TEXT        NOT NULL,
msg_value  TEXT        NOT NULL
);
SELECT create_hypertable('tbl4cluster2', by_range('msg_date'));