DO $$ 
    BEGIN
        BEGIN
            ALTER TABLE mytable ADD COLUMN counter integer default 0; 
        EXCEPTION
            WHEN duplicate_column THEN RAISE NOTICE 'counter column already exists';
        END;
    END;
$$;

CREATE DATABASE $($database_name)
  WITH
  ENCODING = 'UTF8'
  LC_COLLATE = 'C.UTF-8'
  LC_CTYPE = 'C.UTF-8'
  LOCALE_PROVIDER = 'libc'
  TABLESPACE = pg_default
  CONNECTION LIMIT = -1
  IS_TEMPLATE = False;

COMMENT ON DATABASE $($database_name)
    IS $($database_comment);

COMMIT;

CREATE SCHEMA IF NOT EXISTS $($schema_name);
COMMENT ON SCHEMA $($schema_name)
    IS $($schema_comment);

COMMIT;
