CREATE DATABASE staging_mqtt
WITH
OWNER = tsdb_user
ENCODING = 'UTF8'
LC_COLLATE = 'C.UTF-8'
LC_CTYPE = 'C.UTF-8'
LOCALE_PROVIDER = 'libc'
TABLESPACE = pg_default
CONNECTION LIMIT = -1
 IS_TEMPLATE = False;

COMMENT ON DATABASE staging_mqtt
IS 'Staging für den mqtt_client';

CREATE TABLE tbl_staging_mqtt (
msg_date   TIMESTAMPTZ NOT NULL,
msg_topic  TEXT        NOT NULL,
msg_value  TEXT        NOT NULL
);
SELECT create_hypertable('tbl_staging_mqtt', by_range('msg_date'));