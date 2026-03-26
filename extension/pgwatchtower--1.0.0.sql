-- pgwatchtower--1.0.0.sql
-- Base install: Core metrics collection and shipping

CREATE EXTENSION IF NOT EXISTS dblink;

-- =============================================================================
-- CONFIGURATION
-- =============================================================================

CREATE TABLE watchtower.remote_config (
    id              INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    remote_host     TEXT NOT NULL DEFAULT 'localhost',
    remote_port     INTEGER NOT NULL DEFAULT 5432,
    remote_dbname   TEXT NOT NULL DEFAULT 'metrics_db',
    remote_user     TEXT NOT NULL DEFAULT 'metrics_user',
    remote_password TEXT NOT NULL DEFAULT '',
    remote_schema   TEXT NOT NULL DEFAULT 'watchtower',
    source_name     TEXT,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE watchtower.remote_config IS
    'Singleton configuration table for the remote metrics database connection';

INSERT INTO watchtower.remote_config (id) VALUES (1);

-- =============================================================================
-- LOCAL COLLECTION LOG
-- =============================================================================

CREATE TABLE watchtower.collection_log (
    id              BIGSERIAL PRIMARY KEY,
    collected_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    db_count        INTEGER,
    table_count     INTEGER,
    shipped_ok      BOOLEAN NOT NULL DEFAULT FALSE,
    error_message   TEXT,
    duration_ms     INTEGER
);

COMMENT ON TABLE watchtower.collection_log IS
    'Local log of each collection run for debugging';

-- =============================================================================
-- HELPER: Build connection string
-- =============================================================================

CREATE OR REPLACE FUNCTION watchtower._get_connstr()
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    cfg RECORD;
BEGIN
    SELECT * INTO cfg FROM watchtower.remote_config WHERE id = 1;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'PgWatchtower: no configuration found. Call watchtower.set_remote_config() first.';
    END IF;

    IF NOT cfg.is_active THEN
        RAISE EXCEPTION 'PgWatchtower: collection is disabled (is_active = false)';
    END IF;

    RETURN format(
        'host=%s port=%s dbname=%s user=%s password=%s connect_timeout=10',
        cfg.remote_host, cfg.remote_port, cfg.remote_dbname,
        cfg.remote_user, cfg.remote_password
    );
END;
$$;

-- =============================================================================
-- HELPER: Get source identifier
-- =============================================================================

CREATE OR REPLACE FUNCTION watchtower._get_source_name()
RETURNS TEXT
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    src TEXT;
BEGIN
    SELECT source_name INTO src FROM watchtower.remote_config WHERE id = 1;

    IF src IS NULL OR src = '' THEN
        SELECT format('%s/%s', inet_server_addr()::text, current_setting('cluster_name', true))
        INTO src;
        IF src IS NULL OR src = '/' THEN
            src := format('%s:%s', inet_server_addr()::text, inet_server_port()::text);
        END IF;
        IF src IS NULL OR src = ':' THEN
            src := 'unknown';
        END IF;
    END IF;

    RETURN src;
END;
$$;

-- =============================================================================
-- CONFIGURATION SETTER
-- =============================================================================

CREATE OR REPLACE FUNCTION watchtower.set_remote_config(
    p_host      TEXT,
    p_port      INTEGER DEFAULT 5432,
    p_dbname    TEXT DEFAULT 'metrics_db',
    p_user      TEXT DEFAULT 'metrics_user',
    p_password  TEXT DEFAULT '',
    p_schema    TEXT DEFAULT 'watchtower',
    p_source    TEXT DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    UPDATE watchtower.remote_config
    SET remote_host     = p_host,
        remote_port     = p_port,
        remote_dbname   = p_dbname,
        remote_user     = p_user,
        remote_password = p_password,
        remote_schema   = p_schema,
        source_name     = COALESCE(p_source, source_name),
        updated_at      = now()
    WHERE id = 1;

    RAISE NOTICE 'PgWatchtower: remote config updated -> %:%/%', p_host, p_port, p_dbname;
END;
$$;

COMMENT ON FUNCTION watchtower.set_remote_config IS
    'Configure the remote metrics database connection';

-- =============================================================================
-- TEST CONNECTION
-- =============================================================================

CREATE OR REPLACE FUNCTION watchtower.test_connection()
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    connstr TEXT;
    result  TEXT;
BEGIN
    connstr := watchtower._get_connstr();
    SELECT val INTO result
    FROM dblink(connstr, 'SELECT ''OK''::text AS val') AS t(val text);
    RETURN 'Connection successful';
EXCEPTION
    WHEN OTHERS THEN
        RETURN format('Connection failed: %s', SQLERRM);
END;
$$;

COMMENT ON FUNCTION watchtower.test_connection IS
    'Test connectivity to the remote metrics database';

-- =============================================================================
-- CORE: Collect database sizes
-- =============================================================================

CREATE OR REPLACE FUNCTION watchtower._collect_db_sizes()
RETURNS TABLE (database_name TEXT, size_bytes BIGINT, size_pretty TEXT)
LANGUAGE sql STABLE
AS $$
    SELECT d.datname::text, pg_database_size(d.datname),
           pg_size_pretty(pg_database_size(d.datname))
    FROM pg_database d
    WHERE d.datistemplate = false AND d.datallowconn = true
    ORDER BY 2 DESC;
$$;

-- =============================================================================
-- CORE: Collect table sizes
-- =============================================================================

CREATE OR REPLACE FUNCTION watchtower._collect_table_sizes()
RETURNS TABLE (
    schema_name TEXT, table_name TEXT, total_bytes BIGINT,
    table_bytes BIGINT, index_bytes BIGINT, toast_bytes BIGINT,
    row_estimate BIGINT, total_pretty TEXT
)
LANGUAGE sql STABLE
AS $$
    SELECT
        n.nspname::text, c.relname::text,
        pg_total_relation_size(c.oid),
        pg_relation_size(c.oid),
        COALESCE(pg_indexes_size(c.oid), 0),
        COALESCE(pg_total_relation_size(c.oid) - pg_relation_size(c.oid)
                 - COALESCE(pg_indexes_size(c.oid), 0), 0),
        c.reltuples::bigint,
        pg_size_pretty(pg_total_relation_size(c.oid))
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r'
      AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'watchtower')
    ORDER BY 3 DESC;
$$;

-- =============================================================================
-- CORE: Collect and ship
-- =============================================================================

CREATE OR REPLACE FUNCTION watchtower.collect_and_ship_sizes()
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    connstr         TEXT;
    source          TEXT;
    remote_schema   TEXT;
    db_rec          RECORD;
    tbl_rec         RECORD;
    db_count        INTEGER := 0;
    tbl_count       INTEGER := 0;
    start_ts        TIMESTAMPTZ := clock_timestamp();
    collection_ts   TIMESTAMPTZ := date_trunc('day', now());
    current_dbname  TEXT := current_database();
    ship_sql        TEXT;
    err_msg         TEXT;
BEGIN
    connstr := watchtower._get_connstr();
    source  := watchtower._get_source_name();
    SELECT rc.remote_schema INTO remote_schema FROM watchtower.remote_config rc WHERE id = 1;

    FOR db_rec IN SELECT * FROM watchtower._collect_db_sizes()
    LOOP
        ship_sql := format(
            'INSERT INTO %I.db_size_history (collected_at, source_server, database_name, size_bytes, size_pretty)
             VALUES (%L, %L, %L, %s, %L)
             ON CONFLICT (collected_at, source_server, database_name) DO UPDATE
             SET size_bytes = EXCLUDED.size_bytes, size_pretty = EXCLUDED.size_pretty',
            remote_schema, collection_ts, source,
            db_rec.database_name, db_rec.size_bytes, db_rec.size_pretty);
        PERFORM dblink_exec(connstr, ship_sql);
        db_count := db_count + 1;
    END LOOP;

    FOR tbl_rec IN SELECT * FROM watchtower._collect_table_sizes()
    LOOP
        ship_sql := format(
            'INSERT INTO %I.table_size_history
             (collected_at, source_server, database_name, schema_name, table_name,
              total_bytes, table_bytes, index_bytes, toast_bytes, row_estimate, total_pretty)
             VALUES (%L, %L, %L, %L, %L, %s, %s, %s, %s, %s, %L)
             ON CONFLICT (collected_at, source_server, database_name, schema_name, table_name)
             DO UPDATE SET total_bytes=EXCLUDED.total_bytes, table_bytes=EXCLUDED.table_bytes,
                index_bytes=EXCLUDED.index_bytes, toast_bytes=EXCLUDED.toast_bytes,
                row_estimate=EXCLUDED.row_estimate, total_pretty=EXCLUDED.total_pretty',
            remote_schema, collection_ts, source, current_dbname,
            tbl_rec.schema_name, tbl_rec.table_name,
            tbl_rec.total_bytes, tbl_rec.table_bytes, tbl_rec.index_bytes,
            tbl_rec.toast_bytes, tbl_rec.row_estimate, tbl_rec.total_pretty);
        PERFORM dblink_exec(connstr, ship_sql);
        tbl_count := tbl_count + 1;
    END LOOP;

    ship_sql := format(
        'INSERT INTO %I.collection_log (collected_at, source_server, database_name, db_count, table_count, duration_ms, status)
         VALUES (%L, %L, %L, %s, %s, %s, %L)',
        remote_schema, collection_ts, source, current_dbname,
        db_count, tbl_count,
        extract(millisecond FROM clock_timestamp() - start_ts)::integer, 'success');
    PERFORM dblink_exec(connstr, ship_sql);

    INSERT INTO watchtower.collection_log (db_count, table_count, shipped_ok, duration_ms)
    VALUES (db_count, tbl_count, true,
            extract(millisecond FROM clock_timestamp() - start_ts)::integer);

    RETURN format('Shipped %s databases + %s tables in %s ms',
                  db_count, tbl_count,
                  extract(millisecond FROM clock_timestamp() - start_ts)::integer);

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS err_msg = MESSAGE_TEXT;
    INSERT INTO watchtower.collection_log (db_count, table_count, shipped_ok, error_message, duration_ms)
    VALUES (db_count, tbl_count, false, err_msg,
            extract(millisecond FROM clock_timestamp() - start_ts)::integer);
    RAISE WARNING 'PgWatchtower collection failed: %', err_msg;
    RETURN format('FAILED: %s', err_msg);
END;
$$;

COMMENT ON FUNCTION watchtower.collect_and_ship_sizes IS
    'Collect current DB and table sizes and ship them to the configured remote metrics database';

-- =============================================================================
-- CONVENIENCE VIEWS
-- =============================================================================

CREATE OR REPLACE FUNCTION watchtower.show_db_sizes()
RETURNS TABLE (database_name TEXT, size_bytes BIGINT, size_pretty TEXT)
LANGUAGE sql STABLE
AS $$ SELECT * FROM watchtower._collect_db_sizes(); $$;

CREATE OR REPLACE FUNCTION watchtower.show_table_sizes()
RETURNS TABLE (
    schema_name TEXT, table_name TEXT, total_bytes BIGINT,
    table_bytes BIGINT, index_bytes BIGINT, toast_bytes BIGINT,
    row_estimate BIGINT, total_pretty TEXT
)
LANGUAGE sql STABLE
AS $$ SELECT * FROM watchtower._collect_table_sizes(); $$;

-- =============================================================================
-- GRANTS
-- =============================================================================

GRANT USAGE ON SCHEMA watchtower TO pg_monitor;
GRANT SELECT ON ALL TABLES IN SCHEMA watchtower TO pg_monitor;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA watchtower TO pg_monitor;
