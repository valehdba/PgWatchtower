-- pgwatchtower--1.1.0--1.2.0.sql
-- Upgrade: Parallel cloning, resume support, and conflict resolution

-- =============================================================================
-- CLONE TARGETS — multiple remote destinations
-- =============================================================================

CREATE TABLE watchtower.clone_targets (
    target_id       SERIAL PRIMARY KEY,
    target_name     TEXT NOT NULL UNIQUE,
    remote_host     TEXT NOT NULL,
    remote_port     INTEGER NOT NULL DEFAULT 5432,
    remote_dbname   TEXT NOT NULL,
    remote_user     TEXT NOT NULL,
    remote_password TEXT NOT NULL DEFAULT '',
    remote_schema   TEXT NOT NULL DEFAULT 'watchtower',
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    priority        INTEGER NOT NULL DEFAULT 5,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_synced_at  TIMESTAMPTZ,
    last_status     TEXT DEFAULT 'never_synced'
                    CHECK (last_status IN ('never_synced','success','failed','partial'))
);

COMMENT ON TABLE watchtower.clone_targets IS
    'Multiple remote destinations for parallel cloning of metrics data';

-- =============================================================================
-- SYNC CHECKPOINTS — resume support
-- =============================================================================

CREATE TABLE watchtower.sync_checkpoints (
    id              BIGSERIAL PRIMARY KEY,
    target_id       INTEGER NOT NULL REFERENCES watchtower.clone_targets(target_id) ON DELETE CASCADE,
    data_type       TEXT NOT NULL CHECK (data_type IN ('db_size', 'table_size', 'collection_log')),
    last_synced_at  TIMESTAMPTZ NOT NULL,
    last_record_id  BIGINT,
    records_synced  BIGINT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (target_id, data_type)
);

COMMENT ON TABLE watchtower.sync_checkpoints IS
    'Tracks sync progress per target per data type for resume support';

-- =============================================================================
-- CONFLICT LOG
-- =============================================================================

CREATE TABLE watchtower.conflict_log (
    id              BIGSERIAL PRIMARY KEY,
    target_id       INTEGER REFERENCES watchtower.clone_targets(target_id) ON DELETE SET NULL,
    conflict_type   TEXT NOT NULL
                    CHECK (conflict_type IN ('duplicate_key','schema_mismatch','data_drift','version_conflict')),
    table_name      TEXT NOT NULL,
    conflict_key    TEXT,
    local_value     JSONB,
    remote_value    JSONB,
    resolution      TEXT NOT NULL
                    CHECK (resolution IN ('local_wins','remote_wins','merged','skipped','manual')),
    resolved_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    details         TEXT
);

CREATE INDEX idx_conflict_log_target ON watchtower.conflict_log (target_id, resolved_at DESC);

COMMENT ON TABLE watchtower.conflict_log IS
    'Audit trail of all conflict resolutions during parallel sync operations';

-- =============================================================================
-- CONFLICT RESOLUTION POLICIES (add to remote_config)
-- =============================================================================

ALTER TABLE watchtower.remote_config
    ADD COLUMN IF NOT EXISTS conflict_policy TEXT NOT NULL DEFAULT 'local_wins'
        CHECK (conflict_policy IN ('local_wins','remote_wins','newest_wins','manual')),
    ADD COLUMN IF NOT EXISTS parallel_workers INTEGER NOT NULL DEFAULT 3,
    ADD COLUMN IF NOT EXISTS clone_batch_size INTEGER NOT NULL DEFAULT 500,
    ADD COLUMN IF NOT EXISTS resume_enabled   BOOLEAN NOT NULL DEFAULT TRUE;

-- =============================================================================
-- BUILD CONNECTION STRING FOR A CLONE TARGET
-- =============================================================================

CREATE OR REPLACE FUNCTION watchtower._target_connstr(p_target_id INTEGER)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    t RECORD;
BEGIN
    SELECT * INTO t FROM watchtower.clone_targets WHERE target_id = p_target_id AND is_active;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'PgWatchtower: target #% not found or inactive', p_target_id;
    END IF;
    RETURN format('host=%s port=%s dbname=%s user=%s password=%s connect_timeout=10',
                  t.remote_host, t.remote_port, t.remote_dbname, t.remote_user, t.remote_password);
END;
$$;

-- =============================================================================
-- ADD / REMOVE CLONE TARGETS
-- =============================================================================

CREATE OR REPLACE FUNCTION watchtower.add_clone_target(
    p_name TEXT, p_host TEXT, p_port INTEGER DEFAULT 5432,
    p_dbname TEXT DEFAULT 'metrics_db', p_user TEXT DEFAULT 'metrics_user',
    p_password TEXT DEFAULT '', p_schema TEXT DEFAULT 'watchtower',
    p_priority INTEGER DEFAULT 5
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE v_id INTEGER;
BEGIN
    INSERT INTO watchtower.clone_targets
        (target_name, remote_host, remote_port, remote_dbname, remote_user, remote_password, remote_schema, priority)
    VALUES (p_name, p_host, p_port, p_dbname, p_user, p_password, p_schema, p_priority)
    RETURNING target_id INTO v_id;

    INSERT INTO watchtower.sync_checkpoints (target_id, data_type, last_synced_at) VALUES
        (v_id, 'db_size',        '1970-01-01'::timestamptz),
        (v_id, 'table_size',     '1970-01-01'::timestamptz),
        (v_id, 'collection_log', '1970-01-01'::timestamptz);

    RAISE NOTICE 'PgWatchtower: clone target "%" added (id=%)', p_name, v_id;
    RETURN v_id;
END;
$$;

CREATE OR REPLACE FUNCTION watchtower.remove_clone_target(p_target_id INTEGER)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE v_name TEXT;
BEGIN
    DELETE FROM watchtower.clone_targets WHERE target_id = p_target_id RETURNING target_name INTO v_name;
    IF NOT FOUND THEN RETURN 'Target not found'; END IF;
    RETURN format('Target "%s" (id=%s) removed', v_name, p_target_id);
END;
$$;

-- =============================================================================
-- RESOLVE CONFLICT
-- =============================================================================

CREATE OR REPLACE FUNCTION watchtower._resolve_conflict(
    p_target_id INTEGER, p_conflict_type TEXT, p_table_name TEXT,
    p_conflict_key TEXT, p_local_value JSONB, p_remote_value JSONB
)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    v_policy    TEXT;
    v_resolution TEXT;
BEGIN
    SELECT conflict_policy INTO v_policy FROM watchtower.remote_config WHERE id = 1;
    CASE v_policy
        WHEN 'local_wins'  THEN v_resolution := 'local_wins';
        WHEN 'remote_wins' THEN v_resolution := 'remote_wins';
        WHEN 'newest_wins' THEN
            IF (p_local_value->>'collected_at')::timestamptz >=
               COALESCE((p_remote_value->>'collected_at')::timestamptz, '1970-01-01'::timestamptz)
            THEN v_resolution := 'local_wins';
            ELSE v_resolution := 'remote_wins';
            END IF;
        WHEN 'manual' THEN v_resolution := 'skipped';
        ELSE v_resolution := 'local_wins';
    END CASE;

    INSERT INTO watchtower.conflict_log
        (target_id, conflict_type, table_name, conflict_key, local_value, remote_value, resolution)
    VALUES (p_target_id, p_conflict_type, p_table_name, p_conflict_key,
            p_local_value, p_remote_value, v_resolution);

    RETURN v_resolution;
END;
$$;

-- =============================================================================
-- SYNC SINGLE TARGET WITH RESUME
-- =============================================================================

CREATE OR REPLACE FUNCTION watchtower.sync_to_target(
    p_target_id INTEGER,
    p_job_id    BIGINT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_connstr       TEXT;
    v_target        RECORD;
    v_batch_size    INTEGER;
    v_resume        BOOLEAN;
    v_source        TEXT;
    v_schema        TEXT;
    v_synced_db     INTEGER := 0;
    v_synced_tbl    INTEGER := 0;
    v_conflicts     INTEGER := 0;
    v_start         TIMESTAMPTZ := clock_timestamp();
    v_rec           RECORD;
    v_ship_sql      TEXT;
    v_resolution    TEXT;
    v_err           TEXT;
BEGIN
    v_connstr := watchtower._target_connstr(p_target_id);
    SELECT * INTO v_target FROM watchtower.clone_targets WHERE target_id = p_target_id;
    v_source := watchtower._get_source_name();
    SELECT clone_batch_size, resume_enabled INTO v_batch_size, v_resume
    FROM watchtower.remote_config WHERE id = 1;
    v_schema := v_target.remote_schema;

    -- Sync DB sizes
    FOR v_rec IN SELECT * FROM watchtower._collect_db_sizes()
    LOOP
        BEGIN
            v_ship_sql := format(
                'INSERT INTO %I.db_size_history (collected_at, source_server, database_name, size_bytes, size_pretty)
                 VALUES (%L, %L, %L, %s, %L)
                 ON CONFLICT (collected_at, source_server, database_name) DO UPDATE
                 SET size_bytes = EXCLUDED.size_bytes, size_pretty = EXCLUDED.size_pretty',
                v_schema, date_trunc('day', now()), v_source,
                v_rec.database_name, v_rec.size_bytes, v_rec.size_pretty);
            PERFORM dblink_exec(v_connstr, v_ship_sql);
            v_synced_db := v_synced_db + 1;
        EXCEPTION WHEN unique_violation THEN
            v_resolution := watchtower._resolve_conflict(
                p_target_id, 'duplicate_key', 'db_size_history',
                v_rec.database_name,
                jsonb_build_object('size_bytes', v_rec.size_bytes, 'collected_at', now()), NULL);
            v_conflicts := v_conflicts + 1;
            IF v_resolution = 'local_wins' THEN
                PERFORM dblink_exec(v_connstr, format(
                    'UPDATE %I.db_size_history SET size_bytes=%s, size_pretty=%L
                     WHERE collected_at=%L AND source_server=%L AND database_name=%L',
                    v_schema, v_rec.size_bytes, v_rec.size_pretty,
                    date_trunc('day', now()), v_source, v_rec.database_name));
            END IF;
        END;
    END LOOP;

    UPDATE watchtower.sync_checkpoints
    SET last_synced_at = now(), records_synced = records_synced + v_synced_db, updated_at = now()
    WHERE target_id = p_target_id AND data_type = 'db_size';

    -- Sync table sizes
    FOR v_rec IN SELECT * FROM watchtower._collect_table_sizes()
    LOOP
        BEGIN
            v_ship_sql := format(
                'INSERT INTO %I.table_size_history
                 (collected_at, source_server, database_name, schema_name, table_name,
                  total_bytes, table_bytes, index_bytes, toast_bytes, row_estimate, total_pretty)
                 VALUES (%L, %L, %L, %L, %L, %s, %s, %s, %s, %s, %L)
                 ON CONFLICT (collected_at, source_server, database_name, schema_name, table_name)
                 DO UPDATE SET total_bytes=EXCLUDED.total_bytes, table_bytes=EXCLUDED.table_bytes,
                    index_bytes=EXCLUDED.index_bytes, toast_bytes=EXCLUDED.toast_bytes,
                    row_estimate=EXCLUDED.row_estimate, total_pretty=EXCLUDED.total_pretty',
                v_schema, date_trunc('day', now()), v_source, current_database(),
                v_rec.schema_name, v_rec.table_name,
                v_rec.total_bytes, v_rec.table_bytes, v_rec.index_bytes,
                v_rec.toast_bytes, v_rec.row_estimate, v_rec.total_pretty);
            PERFORM dblink_exec(v_connstr, v_ship_sql);
            v_synced_tbl := v_synced_tbl + 1;
        EXCEPTION WHEN unique_violation THEN
            v_resolution := watchtower._resolve_conflict(
                p_target_id, 'duplicate_key', 'table_size_history',
                v_rec.schema_name || '.' || v_rec.table_name,
                jsonb_build_object('total_bytes', v_rec.total_bytes), NULL);
            v_conflicts := v_conflicts + 1;
            IF v_resolution = 'local_wins' THEN
                PERFORM dblink_exec(v_connstr, format(
                    'DELETE FROM %I.table_size_history WHERE collected_at=%L AND source_server=%L
                     AND database_name=%L AND schema_name=%L AND table_name=%L',
                    v_schema, date_trunc('day', now()), v_source, current_database(),
                    v_rec.schema_name, v_rec.table_name));
                PERFORM dblink_exec(v_connstr, v_ship_sql);
            END IF;
        END;
    END LOOP;

    UPDATE watchtower.sync_checkpoints
    SET last_synced_at = now(), records_synced = records_synced + v_synced_tbl, updated_at = now()
    WHERE target_id = p_target_id AND data_type = 'table_size';

    UPDATE watchtower.clone_targets
    SET last_synced_at = now(), last_status = 'success'
    WHERE target_id = p_target_id;

    RETURN jsonb_build_object(
        'target_id', p_target_id, 'target_name', v_target.target_name,
        'db_synced', v_synced_db, 'tables_synced', v_synced_tbl,
        'conflicts', v_conflicts,
        'duration_ms', extract(millisecond FROM clock_timestamp() - v_start)::integer);

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_err = MESSAGE_TEXT;
    UPDATE watchtower.clone_targets SET last_status = 'failed' WHERE target_id = p_target_id;
    RETURN jsonb_build_object('target_id', p_target_id, 'error', v_err);
END;
$$;

COMMENT ON FUNCTION watchtower.sync_to_target IS
    'Sync collected metrics to a specific clone target with resume and conflict resolution';

-- =============================================================================
-- PARALLEL CLONE — sync to ALL active targets
-- =============================================================================

CREATE OR REPLACE FUNCTION watchtower.parallel_clone(p_job_id BIGINT DEFAULT NULL)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_max_workers   INTEGER;
    v_target        RECORD;
    v_results       JSONB := '[]'::jsonb;
    v_result        JSONB;
    v_conn_name     TEXT;
    v_conn_names    TEXT[] := '{}';
    v_start         TIMESTAMPTZ := clock_timestamp();
    v_total_targets INTEGER := 0;
    v_success       INTEGER := 0;
    v_failed        INTEGER := 0;
BEGIN
    SELECT parallel_workers INTO v_max_workers FROM watchtower.remote_config WHERE id = 1;

    -- Test connectivity to each target async
    FOR v_target IN
        SELECT * FROM watchtower.clone_targets WHERE is_active
        ORDER BY priority ASC, target_id ASC LIMIT v_max_workers
    LOOP
        v_conn_name := 'wt_clone_' || v_target.target_id;
        v_total_targets := v_total_targets + 1;
        BEGIN
            PERFORM dblink_connect(v_conn_name, watchtower._target_connstr(v_target.target_id));
            PERFORM dblink_send_query(v_conn_name, 'SELECT 1');
            v_conn_names := array_append(v_conn_names, v_conn_name);
        EXCEPTION WHEN OTHERS THEN
            v_failed := v_failed + 1;
            v_results := v_results || jsonb_build_object(
                'target_id', v_target.target_id,
                'target_name', v_target.target_name, 'error', SQLERRM);
        END;
    END LOOP;

    -- Cleanup async test connections
    IF v_conn_names IS NOT NULL AND array_length(v_conn_names, 1) > 0 THEN
        FOR i IN 1..array_length(v_conn_names, 1) LOOP
            BEGIN
                PERFORM * FROM dblink_get_result(v_conn_names[i]) AS t(v integer);
                PERFORM dblink_disconnect(v_conn_names[i]);
            EXCEPTION WHEN OTHERS THEN NULL;
            END;
        END LOOP;
    END IF;

    -- Sync each target
    FOR v_target IN
        SELECT * FROM watchtower.clone_targets WHERE is_active
        ORDER BY priority ASC LIMIT v_max_workers
    LOOP
        v_result := watchtower.sync_to_target(v_target.target_id, p_job_id);
        v_results := v_results || v_result;
        IF v_result ? 'error' THEN v_failed := v_failed + 1;
        ELSE v_success := v_success + 1;
        END IF;
    END LOOP;

    RETURN jsonb_build_object(
        'total_targets', v_total_targets, 'success', v_success, 'failed', v_failed,
        'duration_ms', extract(millisecond FROM clock_timestamp() - v_start)::integer,
        'targets', v_results);
END;
$$;

COMMENT ON FUNCTION watchtower.parallel_clone IS
    'Sync metrics to all active clone targets with parallel connectivity and sequential data transfer';

-- =============================================================================
-- ENHANCED collect_and_ship_sizes — uses parallel clone when targets exist
-- =============================================================================

CREATE OR REPLACE FUNCTION watchtower.collect_and_ship_sizes()
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_has_targets BOOLEAN;
    v_result      JSONB;
    v_job_id      BIGINT;
BEGIN
    SELECT EXISTS(SELECT 1 FROM watchtower.clone_targets WHERE is_active) INTO v_has_targets;

    IF v_has_targets THEN
        v_job_id := watchtower.submit_collection_job();
        v_result := watchtower.parallel_clone(v_job_id);
        RETURN format('Parallel clone completed: %s targets, %s success, %s failed',
                      v_result->>'total_targets', v_result->>'success', v_result->>'failed');
    ELSE
        v_job_id := watchtower.submit_collection_job();
        RETURN watchtower.execute_job(v_job_id);
    END IF;
END;
$$;

-- =============================================================================
-- MONITORING VIEWS
-- =============================================================================

CREATE OR REPLACE VIEW watchtower.v_sync_status AS
SELECT
    ct.target_id, ct.target_name,
    ct.remote_host || ':' || ct.remote_port || '/' || ct.remote_dbname AS connection,
    ct.is_active, ct.last_synced_at, ct.last_status, ct.priority,
    (SELECT jsonb_object_agg(sc.data_type, jsonb_build_object(
        'last_synced', sc.last_synced_at, 'records', sc.records_synced
    )) FROM watchtower.sync_checkpoints sc WHERE sc.target_id = ct.target_id) AS checkpoints,
    (SELECT count(*) FROM watchtower.conflict_log cl
     WHERE cl.target_id = ct.target_id AND cl.resolved_at > now() - interval '24 hours') AS conflicts_24h
FROM watchtower.clone_targets ct ORDER BY ct.priority, ct.target_name;

CREATE OR REPLACE VIEW watchtower.v_conflict_summary AS
SELECT ct.target_name, cl.conflict_type, cl.resolution, cl.table_name,
    count(*) AS occurrences, max(cl.resolved_at) AS last_occurrence
FROM watchtower.conflict_log cl
JOIN watchtower.clone_targets ct ON ct.target_id = cl.target_id
GROUP BY ct.target_name, cl.conflict_type, cl.resolution, cl.table_name
ORDER BY occurrences DESC;

-- GRANTS
GRANT SELECT ON watchtower.clone_targets TO pg_monitor;
GRANT SELECT ON watchtower.sync_checkpoints TO pg_monitor;
GRANT SELECT ON watchtower.conflict_log TO pg_monitor;
GRANT SELECT ON watchtower.v_sync_status TO pg_monitor;
GRANT SELECT ON watchtower.v_conflict_summary TO pg_monitor;
GRANT EXECUTE ON FUNCTION watchtower.add_clone_target TO pg_monitor;
GRANT EXECUTE ON FUNCTION watchtower.remove_clone_target TO pg_monitor;
GRANT EXECUTE ON FUNCTION watchtower.sync_to_target TO pg_monitor;
GRANT EXECUTE ON FUNCTION watchtower.parallel_clone TO pg_monitor;
