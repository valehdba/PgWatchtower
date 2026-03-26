-- =============================================================================
-- 001_create_schema.sql
-- Remote Metrics Database Schema for pgwatchtower
-- Run this on the central metrics PostgreSQL instance
-- =============================================================================

BEGIN;

CREATE SCHEMA IF NOT EXISTS watchtower;

-- =============================================================================
-- Database-level size history
-- =============================================================================

CREATE TABLE IF NOT EXISTS watchtower.db_size_history (
    id              BIGSERIAL,
    collected_at    TIMESTAMPTZ NOT NULL,
    source_server   TEXT        NOT NULL,
    database_name   TEXT        NOT NULL,
    size_bytes      BIGINT      NOT NULL,
    size_pretty     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (id),
    UNIQUE (collected_at, source_server, database_name)
);

COMMENT ON TABLE watchtower.db_size_history IS
    'Daily database-level size snapshots from all monitored servers';

CREATE INDEX idx_db_size_server_date
    ON watchtower.db_size_history (source_server, collected_at DESC);

CREATE INDEX idx_db_size_dbname_date
    ON watchtower.db_size_history (database_name, collected_at DESC);

-- =============================================================================
-- Table-level size history
-- =============================================================================

CREATE TABLE IF NOT EXISTS watchtower.table_size_history (
    id              BIGSERIAL,
    collected_at    TIMESTAMPTZ NOT NULL,
    source_server   TEXT        NOT NULL,
    database_name   TEXT        NOT NULL,
    schema_name     TEXT        NOT NULL,
    table_name      TEXT        NOT NULL,
    total_bytes     BIGINT      NOT NULL,
    table_bytes     BIGINT      NOT NULL DEFAULT 0,
    index_bytes     BIGINT      NOT NULL DEFAULT 0,
    toast_bytes     BIGINT      NOT NULL DEFAULT 0,
    row_estimate    BIGINT      DEFAULT 0,
    total_pretty    TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (id),
    UNIQUE (collected_at, source_server, database_name, schema_name, table_name)
);

COMMENT ON TABLE watchtower.table_size_history IS
    'Daily table-level size snapshots with breakdown (table/index/toast)';

CREATE INDEX idx_tbl_size_server_date
    ON watchtower.table_size_history (source_server, collected_at DESC);

CREATE INDEX idx_tbl_size_full_lookup
    ON watchtower.table_size_history (source_server, database_name, schema_name, table_name, collected_at DESC);

-- =============================================================================
-- Collection log (shipped from source servers)
-- =============================================================================

CREATE TABLE IF NOT EXISTS watchtower.collection_log (
    id              BIGSERIAL PRIMARY KEY,
    collected_at    TIMESTAMPTZ NOT NULL,
    source_server   TEXT        NOT NULL,
    database_name   TEXT        NOT NULL,
    db_count        INTEGER,
    table_count     INTEGER,
    duration_ms     INTEGER,
    status          TEXT        NOT NULL DEFAULT 'success',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_collection_log_date
    ON watchtower.collection_log (collected_at DESC);

-- =============================================================================
-- ANALYTICAL VIEWS
-- =============================================================================

-- Daily database growth (day-over-day)
CREATE OR REPLACE VIEW watchtower.v_db_daily_growth AS
SELECT
    h.source_server,
    h.database_name,
    h.collected_at,
    h.size_bytes,
    h.size_pretty,
    LAG(h.size_bytes) OVER w AS prev_size_bytes,
    h.size_bytes - COALESCE(LAG(h.size_bytes) OVER w, h.size_bytes) AS growth_bytes,
    CASE
        WHEN LAG(h.size_bytes) OVER w > 0
        THEN ROUND(
            ((h.size_bytes - LAG(h.size_bytes) OVER w)::numeric
             / LAG(h.size_bytes) OVER w) * 100, 2
        )
        ELSE 0
    END AS growth_pct
FROM watchtower.db_size_history h
WINDOW w AS (PARTITION BY h.source_server, h.database_name ORDER BY h.collected_at)
ORDER BY h.source_server, h.database_name, h.collected_at;

-- Daily table growth (day-over-day)
CREATE OR REPLACE VIEW watchtower.v_table_daily_growth AS
SELECT
    h.source_server,
    h.database_name,
    h.schema_name,
    h.table_name,
    h.collected_at,
    h.total_bytes,
    h.table_bytes,
    h.index_bytes,
    h.toast_bytes,
    h.row_estimate,
    h.total_pretty,
    LAG(h.total_bytes) OVER w AS prev_total_bytes,
    h.total_bytes - COALESCE(LAG(h.total_bytes) OVER w, h.total_bytes) AS growth_bytes,
    CASE
        WHEN LAG(h.total_bytes) OVER w > 0
        THEN ROUND(
            ((h.total_bytes - LAG(h.total_bytes) OVER w)::numeric
             / LAG(h.total_bytes) OVER w) * 100, 2
        )
        ELSE 0
    END AS growth_pct,
    h.row_estimate - COALESCE(LAG(h.row_estimate) OVER w, h.row_estimate) AS row_growth
FROM watchtower.table_size_history h
WINDOW w AS (PARTITION BY h.source_server, h.database_name, h.schema_name, h.table_name
             ORDER BY h.collected_at)
ORDER BY h.source_server, h.database_name, h.schema_name, h.table_name, h.collected_at;

-- Top N fastest-growing tables (last 30 days)
CREATE OR REPLACE VIEW watchtower.v_top_growing_tables AS
WITH date_range AS (
    SELECT
        source_server,
        database_name,
        schema_name,
        table_name,
        MIN(total_bytes) FILTER (WHERE collected_at >= now() - interval '30 days') AS start_bytes,
        MAX(total_bytes) FILTER (WHERE collected_at >= now() - interval '30 days') AS end_bytes,
        MIN(collected_at) FILTER (WHERE collected_at >= now() - interval '30 days') AS first_seen,
        MAX(collected_at) FILTER (WHERE collected_at >= now() - interval '30 days') AS last_seen,
        COUNT(*) FILTER (WHERE collected_at >= now() - interval '30 days') AS data_points
    FROM watchtower.table_size_history
    GROUP BY source_server, database_name, schema_name, table_name
    HAVING COUNT(*) FILTER (WHERE collected_at >= now() - interval '30 days') >= 2
)
SELECT
    *,
    end_bytes - start_bytes AS absolute_growth,
    pg_size_pretty(end_bytes - start_bytes) AS growth_pretty,
    CASE WHEN start_bytes > 0
         THEN ROUND(((end_bytes - start_bytes)::numeric / start_bytes) * 100, 2)
         ELSE 0
    END AS growth_pct
FROM date_range
ORDER BY (end_bytes - start_bytes) DESC;

-- Server summary
CREATE OR REPLACE VIEW watchtower.v_server_summary AS
SELECT
    source_server,
    COUNT(DISTINCT database_name) AS database_count,
    MIN(collected_at) AS tracking_since,
    MAX(collected_at) AS last_collection,
    (SELECT COUNT(*)
     FROM watchtower.collection_log cl
     WHERE cl.source_server = dh.source_server
       AND cl.status = 'success') AS successful_collections
FROM watchtower.db_size_history dh
GROUP BY source_server;

-- =============================================================================
-- SEED: Optional demo data function for testing
-- =============================================================================

CREATE OR REPLACE FUNCTION watchtower.generate_demo_data(
    p_days INTEGER DEFAULT 90,
    p_server TEXT DEFAULT 'demo-server-1'
)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    d INTEGER;
    ts TIMESTAMPTZ;
    base_db_size BIGINT;
    base_tbl_sizes BIGINT[];
    tbl_names TEXT[] := ARRAY['users','orders','products','sessions','audit_log','payments','inventory','notifications'];
    schema_names TEXT[] := ARRAY['public','public','public','public','audit','payments','inventory','public'];
    i INTEGER;
BEGIN
    -- Generate database sizes with realistic growth
    base_db_size := 1073741824; -- 1 GB starting

    FOR d IN 0..p_days LOOP
        ts := date_trunc('day', now()) - (p_days - d) * interval '1 day';

        -- DB size grows ~0.5-2% per day with some variance
        INSERT INTO watchtower.db_size_history (collected_at, source_server, database_name, size_bytes, size_pretty)
        VALUES
            (ts, p_server, 'production',
             base_db_size + (d * base_db_size * (5 + (random()*15)::int)) / 1000,
             pg_size_pretty(base_db_size + (d * base_db_size * (5 + (random()*15)::int)) / 1000)),
            (ts, p_server, 'analytics',
             (base_db_size/2) + (d * (base_db_size/2) * (8 + (random()*20)::int)) / 1000,
             pg_size_pretty((base_db_size/2) + (d * (base_db_size/2) * (8 + (random()*20)::int)) / 1000))
        ON CONFLICT DO NOTHING;

        -- Table sizes
        FOR i IN 1..array_length(tbl_names, 1) LOOP
            INSERT INTO watchtower.table_size_history
                (collected_at, source_server, database_name, schema_name, table_name,
                 total_bytes, table_bytes, index_bytes, toast_bytes, row_estimate, total_pretty)
            VALUES (
                ts, p_server, 'production',
                schema_names[i], tbl_names[i],
                (50000000 * i) + (d * 50000000 * i * (3 + (random()*10)::int)) / 1000,
                (35000000 * i) + (d * 35000000 * i * (3 + (random()*10)::int)) / 1000,
                (12000000 * i) + (d * 12000000 * i * (2 + (random()*8)::int))  / 1000,
                (3000000  * i) + (d * 3000000  * i * (1 + (random()*5)::int))  / 1000,
                (10000 * i)    + (d * 100 * i * (5 + (random()*20)::int)),
                pg_size_pretty(((50000000 * i) + (d * 50000000 * i * (3 + (random()*10)::int)) / 1000)::bigint)
            )
            ON CONFLICT DO NOTHING;
        END LOOP;

        -- Collection log
        INSERT INTO watchtower.collection_log
            (collected_at, source_server, database_name, db_count, table_count, duration_ms, status)
        VALUES (ts, p_server, 'production', 2, array_length(tbl_names, 1), 150 + (random()*200)::int, 'success')
        ON CONFLICT DO NOTHING;
    END LOOP;

    RETURN format('Generated %s days of demo data for server %s', p_days, p_server);
END;
$$;

COMMIT;
