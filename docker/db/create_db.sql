BEGIN
    CREATE DATABASE ___database_name___
    WITH
    ENCODING = 'UTF8'
    LC_COLLATE = 'C.UTF-8'
    LC_CTYPE = 'C.UTF-8'
    LOCALE_PROVIDER = 'libc'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

    COMMENT ON DATABASE ___database_name___
        IS ___database_comment___;


    CREATE SCHEMA IF NOT EXISTS ___schema_name___;
    COMMENT ON SCHEMA ___schema_name___
        IS ___schema_comment___;

END;