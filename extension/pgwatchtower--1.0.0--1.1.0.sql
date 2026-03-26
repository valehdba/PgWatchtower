-- pgwatchtower--1.0.0--1.1.0.sql
-- Upgrade: Background worker for async operations with progress tracking

-- =============================================================================
-- JOB QUEUE TABLE
-- =============================================================================

CREATE TABLE watchtower.job_queue (
    job_id          BIGSERIAL PRIMARY KEY,
    job_type        TEXT NOT NULL DEFAULT 'collect_and_ship',
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','running','completed','failed','cancelled')),
    priority        INTEGER NOT NULL DEFAULT 5,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    scheduled_for   TIMESTAMPTZ NOT NULL DEFAULT now(),
    retry_count     INTEGER NOT NULL DEFAULT 0,
    max_retries     INTEGER NOT NULL DEFAULT 3,
    error_message   TEXT,
    result_summary  JSONB,
    created_by      TEXT DEFAULT current_user
);

CREATE INDEX idx_job_queue_status ON watchtower.job_queue (status, scheduled_for)
    WHERE status IN ('pending', 'running');
CREATE INDEX idx_job_queue_created ON watchtower.job_queue (created_at DESC);

COMMENT ON TABLE watchtower.job_queue IS
    'Async job queue for background collection operations';

-- =============================================================================
-- PROGRESS TRACKING TABLE
-- =============================================================================

CREATE TABLE watchtower.job_progress (
    id              BIGSERIAL PRIMARY KEY,
    job_id          BIGINT NOT NULL REFERENCES watchtower.job_queue(job_id) ON DELETE CASCADE,
    step_name       TEXT NOT NULL,
    step_order      INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','running','completed','failed','skipped')),
    total_items     INTEGER,
    processed_items INTEGER DEFAULT 0,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    details         JSONB,
    error_message   TEXT
);

CREATE INDEX idx_job_progress_job ON watchtower.job_progress (job_id, step_order);

COMMENT ON TABLE watchtower.job_progress IS
    'Granular progress tracking for each step within a collection job';

-- =============================================================================
-- WORKER CONFIGURATION (add columns to remote_config)
-- =============================================================================

ALTER TABLE watchtower.remote_config
    ADD COLUMN IF NOT EXISTS worker_enabled        BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS worker_interval_sec    INTEGER NOT NULL DEFAULT 86400,
    ADD COLUMN IF NOT EXISTS worker_max_parallel    INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS worker_retry_delay_sec INTEGER NOT NULL DEFAULT 300,
    ADD COLUMN IF NOT EXISTS worker_timeout_sec     INTEGER NOT NULL DEFAULT 600;

-- =============================================================================
-- SUBMIT ASYNC JOB
-- =============================================================================

CREATE OR REPLACE FUNCTION watchtower.submit_collection_job(
    p_scheduled_for TIMESTAMPTZ DEFAULT now(),
    p_priority      INTEGER DEFAULT 5
)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_job_id BIGINT;
BEGIN
    INSERT INTO watchtower.job_queue (job_type, priority, scheduled_for)
    VALUES ('collect_and_ship', p_priority, p_scheduled_for)
    RETURNING job_id INTO v_job_id;

    INSERT INTO watchtower.job_progress (job_id, step_name, step_order) VALUES
        (v_job_id, 'connect_remote',     1),
        (v_job_id, 'collect_db_sizes',   2),
        (v_job_id, 'ship_db_sizes',      3),
        (v_job_id, 'collect_table_sizes', 4),
        (v_job_id, 'ship_table_sizes',   5),
        (v_job_id, 'update_log',         6);

    RAISE NOTICE 'PgWatchtower: job #% queued for %', v_job_id, p_scheduled_for;
    RETURN v_job_id;
END;
$$;

COMMENT ON FUNCTION watchtower.submit_collection_job IS
    'Submit an async collection job to the background worker queue';

-- =============================================================================
-- UPDATE STEP PROGRESS
-- =============================================================================

CREATE OR REPLACE FUNCTION watchtower._update_step(
    p_job_id    BIGINT,
    p_step_name TEXT,
    p_status    TEXT,
    p_processed INTEGER DEFAULT NULL,
    p_total     INTEGER DEFAULT NULL,
    p_details   JSONB DEFAULT NULL,
    p_error     TEXT DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE watchtower.job_progress
    SET status          = p_status,
        processed_items = COALESCE(p_processed, processed_items),
        total_items     = COALESCE(p_total, total_items),
        started_at      = CASE WHEN p_status = 'running' AND started_at IS NULL THEN now() ELSE started_at END,
        completed_at    = CASE WHEN p_status IN ('completed','failed','skipped') THEN now() ELSE completed_at END,
        details         = COALESCE(p_details, details),
        error_message   = p_error
    WHERE job_id = p_job_id AND step_name = p_step_name;
END;
$$;

-- =============================================================================
-- EXECUTE JOB
-- =============================================================================

CREATE OR REPLACE FUNCTION watchtower.execute_job(p_job_id BIGINT)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_job       RECORD;
    v_connstr   TEXT;
    v_source    TEXT;
    v_schema    TEXT;
    v_db_count  INTEGER := 0;
    v_tbl_count INTEGER := 0;
    v_start     TIMESTAMPTZ := clock_timestamp();
    v_coll_ts   TIMESTAMPTZ := date_trunc('day', now());
    v_dbname    TEXT := current_database();
    v_db_rec    RECORD;
    v_tbl_rec   RECORD;
    v_ship_sql  TEXT;
    v_err       TEXT;
BEGIN
    UPDATE watchtower.job_queue
    SET status = 'running', started_at = now()
    WHERE job_id = p_job_id AND status = 'pending'
    RETURNING * INTO v_job;

    IF NOT FOUND THEN
        RETURN 'Job not available (already running or completed)';
    END IF;

    BEGIN
        -- Step 1: Connect
        PERFORM watchtower._update_step(p_job_id, 'connect_remote', 'running');
        v_connstr := watchtower._get_connstr();
        v_source  := watchtower._get_source_name();
        SELECT rc.remote_schema INTO v_schema FROM watchtower.remote_config rc WHERE id = 1;
        PERFORM watchtower._update_step(p_job_id, 'connect_remote', 'completed',
            p_details := jsonb_build_object('server', v_source, 'schema', v_schema));

        -- Step 2: Collect DB sizes
        PERFORM watchtower._update_step(p_job_id, 'collect_db_sizes', 'running');
        SELECT count(*) INTO v_db_count FROM watchtower._collect_db_sizes();
        PERFORM watchtower._update_step(p_job_id, 'collect_db_sizes', 'completed',
            p_processed := v_db_count, p_total := v_db_count);

        -- Step 3: Ship DB sizes
        PERFORM watchtower._update_step(p_job_id, 'ship_db_sizes', 'running', p_total := v_db_count);
        v_db_count := 0;
        FOR v_db_rec IN SELECT * FROM watchtower._collect_db_sizes()
        LOOP
            v_ship_sql := format(
                'INSERT INTO %I.db_size_history (collected_at, source_server, database_name, size_bytes, size_pretty)
                 VALUES (%L, %L, %L, %s, %L)
                 ON CONFLICT (collected_at, source_server, database_name) DO UPDATE
                 SET size_bytes = EXCLUDED.size_bytes, size_pretty = EXCLUDED.size_pretty',
                v_schema, v_coll_ts, v_source,
                v_db_rec.database_name, v_db_rec.size_bytes, v_db_rec.size_pretty);
            PERFORM dblink_exec(v_connstr, v_ship_sql);
            v_db_count := v_db_count + 1;
            PERFORM watchtower._update_step(p_job_id, 'ship_db_sizes', 'running', p_processed := v_db_count);
        END LOOP;
        PERFORM watchtower._update_step(p_job_id, 'ship_db_sizes', 'completed', p_processed := v_db_count);

        -- Step 4: Collect table sizes
        PERFORM watchtower._update_step(p_job_id, 'collect_table_sizes', 'running');
        SELECT count(*) INTO v_tbl_count FROM watchtower._collect_table_sizes();
        PERFORM watchtower._update_step(p_job_id, 'collect_table_sizes', 'completed',
            p_processed := v_tbl_count, p_total := v_tbl_count);

        -- Step 5: Ship table sizes
        PERFORM watchtower._update_step(p_job_id, 'ship_table_sizes', 'running', p_total := v_tbl_count);
        v_tbl_count := 0;
        FOR v_tbl_rec IN SELECT * FROM watchtower._collect_table_sizes()
        LOOP
            v_ship_sql := format(
                'INSERT INTO %I.table_size_history
                 (collected_at, source_server, database_name, schema_name, table_name,
                  total_bytes, table_bytes, index_bytes, toast_bytes, row_estimate, total_pretty)
                 VALUES (%L, %L, %L, %L, %L, %s, %s, %s, %s, %s, %L)
                 ON CONFLICT (collected_at, source_server, database_name, schema_name, table_name)
                 DO UPDATE SET total_bytes=EXCLUDED.total_bytes, table_bytes=EXCLUDED.table_bytes,
                    index_bytes=EXCLUDED.index_bytes, toast_bytes=EXCLUDED.toast_bytes,
                    row_estimate=EXCLUDED.row_estimate, total_pretty=EXCLUDED.total_pretty',
                v_schema, v_coll_ts, v_source, v_dbname,
                v_tbl_rec.schema_name, v_tbl_rec.table_name,
                v_tbl_rec.total_bytes, v_tbl_rec.table_bytes, v_tbl_rec.index_bytes,
                v_tbl_rec.toast_bytes, v_tbl_rec.row_estimate, v_tbl_rec.total_pretty);
            PERFORM dblink_exec(v_connstr, v_ship_sql);
            v_tbl_count := v_tbl_count + 1;
            IF v_tbl_count % 10 = 0 THEN
                PERFORM watchtower._update_step(p_job_id, 'ship_table_sizes', 'running', p_processed := v_tbl_count);
            END IF;
        END LOOP;
        PERFORM watchtower._update_step(p_job_id, 'ship_table_sizes', 'completed', p_processed := v_tbl_count);

        -- Step 6: Log
        PERFORM watchtower._update_step(p_job_id, 'update_log', 'running');
        v_ship_sql := format(
            'INSERT INTO %I.collection_log (collected_at, source_server, database_name, db_count, table_count, duration_ms, status)
             VALUES (%L, %L, %L, %s, %s, %s, %L)',
            v_schema, v_coll_ts, v_source, v_dbname, v_db_count, v_tbl_count,
            extract(millisecond FROM clock_timestamp() - v_start)::integer, 'success');
        PERFORM dblink_exec(v_connstr, v_ship_sql);
        PERFORM watchtower._update_step(p_job_id, 'update_log', 'completed');

        -- Mark job completed
        UPDATE watchtower.job_queue
        SET status = 'completed', completed_at = now(),
            result_summary = jsonb_build_object(
                'db_count', v_db_count, 'table_count', v_tbl_count,
                'duration_ms', extract(millisecond FROM clock_timestamp() - v_start)::integer)
        WHERE job_id = p_job_id;

        INSERT INTO watchtower.collection_log (db_count, table_count, shipped_ok, duration_ms)
        VALUES (v_db_count, v_tbl_count, true,
                extract(millisecond FROM clock_timestamp() - v_start)::integer);

        RETURN format('Job #%s completed: %s DBs + %s tables in %s ms',
                      p_job_id, v_db_count, v_tbl_count,
                      extract(millisecond FROM clock_timestamp() - v_start)::integer);

    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_err = MESSAGE_TEXT;
        UPDATE watchtower.job_queue
        SET status = 'failed', completed_at = now(), error_message = v_err,
            retry_count = retry_count + 1
        WHERE job_id = p_job_id;

        INSERT INTO watchtower.collection_log (db_count, table_count, shipped_ok, error_message, duration_ms)
        VALUES (v_db_count, v_tbl_count, false, v_err,
                extract(millisecond FROM clock_timestamp() - v_start)::integer);

        RETURN format('Job #%s FAILED: %s', p_job_id, v_err);
    END;
END;
$$;

COMMENT ON FUNCTION watchtower.execute_job IS
    'Execute a queued collection job with step-by-step progress tracking';

-- =============================================================================
-- POLL AND EXECUTE (background worker entry point)
-- =============================================================================

CREATE OR REPLACE FUNCTION watchtower.poll_and_execute()
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_job_id    BIGINT;
    v_timeout   INTEGER;
BEGIN
    SELECT worker_timeout_sec INTO v_timeout FROM watchtower.remote_config WHERE id = 1;

    -- Cancel stale running jobs
    UPDATE watchtower.job_queue
    SET status = 'failed', completed_at = now(),
        error_message = 'Timed out after ' || v_timeout || ' seconds'
    WHERE status = 'running'
      AND started_at < now() - (v_timeout || ' seconds')::interval;

    -- Re-queue failed jobs under retry limit
    UPDATE watchtower.job_queue
    SET status = 'pending',
        scheduled_for = now() + (
            SELECT (worker_retry_delay_sec || ' seconds')::interval
            FROM watchtower.remote_config WHERE id = 1)
    WHERE status = 'failed'
      AND retry_count < max_retries;

    -- Pick next job
    SELECT job_id INTO v_job_id
    FROM watchtower.job_queue
    WHERE status = 'pending' AND scheduled_for <= now()
    ORDER BY priority ASC, created_at ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED;

    IF NOT FOUND THEN
        RETURN 'No pending jobs';
    END IF;

    RETURN watchtower.execute_job(v_job_id);
END;
$$;

COMMENT ON FUNCTION watchtower.poll_and_execute IS
    'Background worker entry point: pick next pending job and execute it';

-- =============================================================================
-- JOB STATUS & MANAGEMENT
-- =============================================================================

CREATE OR REPLACE FUNCTION watchtower.get_job_status(p_job_id BIGINT)
RETURNS TABLE (
    job_id BIGINT, job_status TEXT, created_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ, completed_at TIMESTAMPTZ, duration_ms INTEGER,
    retry_count INTEGER, error_message TEXT, result_summary JSONB, steps JSONB
)
LANGUAGE sql STABLE
AS $$
    SELECT j.job_id, j.status, j.created_at, j.started_at, j.completed_at,
        CASE WHEN j.completed_at IS NOT NULL
             THEN extract(millisecond FROM j.completed_at - j.started_at)::integer
             WHEN j.started_at IS NOT NULL
             THEN extract(millisecond FROM now() - j.started_at)::integer
             ELSE NULL END,
        j.retry_count, j.error_message, j.result_summary,
        (SELECT jsonb_agg(jsonb_build_object(
            'step', p.step_name, 'status', p.status, 'order', p.step_order,
            'processed', p.processed_items, 'total', p.total_items,
            'error', p.error_message
         ) ORDER BY p.step_order)
         FROM watchtower.job_progress p WHERE p.job_id = j.job_id)
    FROM watchtower.job_queue j WHERE j.job_id = p_job_id;
$$;

CREATE OR REPLACE FUNCTION watchtower.list_jobs(
    p_limit INTEGER DEFAULT 20,
    p_status TEXT DEFAULT NULL
)
RETURNS TABLE (
    job_id BIGINT, status TEXT, priority INTEGER,
    created_at TIMESTAMPTZ, started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ, retry_count INTEGER, result_summary JSONB
)
LANGUAGE sql STABLE
AS $$
    SELECT job_id, status, priority, created_at, started_at,
           completed_at, retry_count, result_summary
    FROM watchtower.job_queue
    WHERE (p_status IS NULL OR watchtower.job_queue.status = p_status)
    ORDER BY created_at DESC LIMIT p_limit;
$$;

CREATE OR REPLACE FUNCTION watchtower.cancel_job(p_job_id BIGINT)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE watchtower.job_queue
    SET status = 'cancelled', completed_at = now()
    WHERE job_id = p_job_id AND status IN ('pending', 'running');
    IF NOT FOUND THEN
        RETURN format('Job #%s cannot be cancelled (not pending/running)', p_job_id);
    END IF;
    RETURN format('Job #%s cancelled', p_job_id);
END;
$$;

CREATE OR REPLACE FUNCTION watchtower.cleanup_jobs(p_older_than INTERVAL DEFAULT '30 days')
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE v_deleted INTEGER;
BEGIN
    DELETE FROM watchtower.job_queue
    WHERE status IN ('completed', 'failed', 'cancelled')
      AND completed_at < now() - p_older_than;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    RETURN v_deleted;
END;
$$;

-- =============================================================================
-- PG_CRON INTEGRATION
-- =============================================================================

CREATE OR REPLACE FUNCTION watchtower.setup_cron_worker(
    p_schedule TEXT DEFAULT '*/5 * * * *'
)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM cron.schedule('pgwatchtower-worker', p_schedule,
        $$SELECT watchtower.poll_and_execute()$$);
    PERFORM cron.schedule('pgwatchtower-daily-submit', '0 2 * * *',
        $$SELECT watchtower.submit_collection_job()$$);
    PERFORM cron.schedule('pgwatchtower-cleanup', '0 4 * * 0',
        $$SELECT watchtower.cleanup_jobs('30 days')$$);
    RETURN 'Cron jobs scheduled: worker (poll), daily collection, weekly cleanup';
EXCEPTION WHEN OTHERS THEN
    RETURN 'pg_cron not available. Use external scheduler to call watchtower.poll_and_execute() periodically.';
END;
$$;

COMMENT ON FUNCTION watchtower.setup_cron_worker IS
    'Set up pg_cron schedules for the background worker, daily collection, and cleanup';

-- GRANTS
GRANT EXECUTE ON FUNCTION watchtower.submit_collection_job TO pg_monitor;
GRANT EXECUTE ON FUNCTION watchtower.get_job_status TO pg_monitor;
GRANT EXECUTE ON FUNCTION watchtower.list_jobs TO pg_monitor;
GRANT EXECUTE ON FUNCTION watchtower.cancel_job TO pg_monitor;
GRANT SELECT ON watchtower.job_queue TO pg_monitor;
GRANT SELECT ON watchtower.job_progress TO pg_monitor;
