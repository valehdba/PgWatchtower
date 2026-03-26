require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());

// ─── Database Connection ─────────────────────────────────────────────────────

const pool = new Pool({
    host:     process.env.DB_HOST     || 'localhost',
    port:     process.env.DB_PORT     || 5432,
    database: process.env.DB_NAME     || 'metrics_db',
    user:     process.env.DB_USER     || 'metrics_user',
    password: process.env.DB_PASSWORD || '',
});

const SCHEMA = process.env.DB_SCHEMA || 'watchtower';

// Helper to query with schema
async function query(sql, params = []) {
    const client = await pool.connect();
    try {
        await client.query(`SET search_path TO ${SCHEMA}, public`);
        const result = await client.query(sql, params);
        return result.rows;
    } finally {
        client.release();
    }
}

// ─── API Routes ──────────────────────────────────────────────────────────────

// List all tracked servers
app.get('/api/servers', async (req, res) => {
    try {
        const rows = await query(`
            SELECT DISTINCT source_server,
                   COUNT(DISTINCT database_name) AS db_count,
                   MIN(collected_at) AS first_seen,
                   MAX(collected_at) AS last_seen
            FROM db_size_history
            GROUP BY source_server
            ORDER BY source_server
        `);
        res.json(rows);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// List databases for a server
app.get('/api/servers/:server/databases', async (req, res) => {
    try {
        const rows = await query(`
            SELECT DISTINCT database_name,
                   MAX(size_bytes) AS latest_size,
                   MAX(size_pretty) AS latest_pretty
            FROM db_size_history
            WHERE source_server = $1
              AND collected_at = (SELECT MAX(collected_at) FROM db_size_history WHERE source_server = $1)
            GROUP BY database_name
            ORDER BY latest_size DESC
        `, [req.params.server]);
        res.json(rows);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Database size history
app.get('/api/db-sizes', async (req, res) => {
    try {
        const { server, database, days = 90 } = req.query;
        let sql = `
            SELECT collected_at, source_server, database_name, size_bytes, size_pretty
            FROM db_size_history
            WHERE collected_at >= now() - $1::integer * interval '1 day'
        `;
        const params = [days];
        let idx = 2;

        if (server) {
            sql += ` AND source_server = $${idx}`;
            params.push(server);
            idx++;
        }
        if (database) {
            sql += ` AND database_name = $${idx}`;
            params.push(database);
            idx++;
        }
        sql += ` ORDER BY collected_at ASC`;

        const rows = await query(sql, params);
        res.json(rows);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Database growth analytics
app.get('/api/db-growth', async (req, res) => {
    try {
        const { server, database, days = 90 } = req.query;
        let sql = `
            SELECT *
            FROM v_db_daily_growth
            WHERE collected_at >= now() - $1::integer * interval '1 day'
        `;
        const params = [days];
        let idx = 2;

        if (server) {
            sql += ` AND source_server = $${idx}`;
            params.push(server);
            idx++;
        }
        if (database) {
            sql += ` AND database_name = $${idx}`;
            params.push(database);
            idx++;
        }
        sql += ` ORDER BY collected_at ASC`;

        const rows = await query(sql, params);
        res.json(rows);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Table size history
app.get('/api/table-sizes', async (req, res) => {
    try {
        const { server, database, schema, table, days = 90 } = req.query;
        let sql = `
            SELECT collected_at, source_server, database_name, schema_name, table_name,
                   total_bytes, table_bytes, index_bytes, toast_bytes, row_estimate, total_pretty
            FROM table_size_history
            WHERE collected_at >= now() - $1::integer * interval '1 day'
        `;
        const params = [days];
        let idx = 2;

        if (server)   { sql += ` AND source_server = $${idx}`; params.push(server); idx++; }
        if (database) { sql += ` AND database_name = $${idx}`; params.push(database); idx++; }
        if (schema)   { sql += ` AND schema_name = $${idx}`;   params.push(schema); idx++; }
        if (table)    { sql += ` AND table_name = $${idx}`;    params.push(table); idx++; }

        sql += ` ORDER BY collected_at ASC, total_bytes DESC`;

        const rows = await query(sql, params);
        res.json(rows);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Table growth analytics
app.get('/api/table-growth', async (req, res) => {
    try {
        const { server, database, schema, table, days = 90 } = req.query;
        let sql = `
            SELECT *
            FROM v_table_daily_growth
            WHERE collected_at >= now() - $1::integer * interval '1 day'
        `;
        const params = [days];
        let idx = 2;

        if (server)   { sql += ` AND source_server = $${idx}`; params.push(server); idx++; }
        if (database) { sql += ` AND database_name = $${idx}`; params.push(database); idx++; }
        if (schema)   { sql += ` AND schema_name = $${idx}`;   params.push(schema); idx++; }
        if (table)    { sql += ` AND table_name = $${idx}`;    params.push(table); idx++; }

        sql += ` ORDER BY collected_at ASC`;

        const rows = await query(sql, params);
        res.json(rows);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Top growing tables
app.get('/api/top-growers', async (req, res) => {
    try {
        const { server, limit = 20 } = req.query;
        let sql = `SELECT * FROM v_top_growing_tables`;
        const params = [];

        if (server) {
            sql += ` WHERE source_server = $1`;
            params.push(server);
        }
        sql += ` LIMIT ${parseInt(limit)}`;

        const rows = await query(sql, params);
        res.json(rows);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Latest snapshot: current sizes for all tables
app.get('/api/latest-snapshot', async (req, res) => {
    try {
        const { server, database } = req.query;
        let sql = `
            SELECT DISTINCT ON (source_server, database_name, schema_name, table_name)
                   source_server, database_name, schema_name, table_name,
                   total_bytes, table_bytes, index_bytes, toast_bytes,
                   row_estimate, total_pretty, collected_at
            FROM table_size_history
            WHERE 1=1
        `;
        const params = [];
        let idx = 1;

        if (server)   { sql += ` AND source_server = $${idx}`; params.push(server); idx++; }
        if (database) { sql += ` AND database_name = $${idx}`; params.push(database); idx++; }

        sql += ` ORDER BY source_server, database_name, schema_name, table_name, collected_at DESC`;

        const rows = await query(sql, params);
        res.json(rows);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Collection health
app.get('/api/collection-health', async (req, res) => {
    try {
        const rows = await query(`
            SELECT source_server, database_name,
                   COUNT(*) FILTER (WHERE status = 'success') AS success_count,
                   COUNT(*) FILTER (WHERE status != 'success') AS fail_count,
                   MAX(collected_at) AS last_collection,
                   ROUND(AVG(duration_ms)) AS avg_duration_ms
            FROM collection_log
            WHERE collected_at >= now() - interval '30 days'
            GROUP BY source_server, database_name
            ORDER BY source_server
        `);
        res.json(rows);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Summary stats
app.get('/api/summary', async (req, res) => {
    try {
        const [servers] = await query(`SELECT COUNT(DISTINCT source_server) AS count FROM db_size_history`);
        const [databases] = await query(`SELECT COUNT(DISTINCT database_name) AS count FROM db_size_history`);
        const [tables] = await query(`SELECT COUNT(DISTINCT schema_name || '.' || table_name) AS count FROM table_size_history`);
        const [dataPoints] = await query(`SELECT COUNT(*) AS count FROM table_size_history`);
        const [lastCollection] = await query(`SELECT MAX(collected_at) AS ts FROM collection_log WHERE status = 'success'`);

        res.json({
            servers: parseInt(servers.count),
            databases: parseInt(databases.count),
            tables: parseInt(tables.count),
            data_points: parseInt(dataPoints.count),
            last_collection: lastCollection.ts
        });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ─── Serve Static Frontend ───────────────────────────────────────────────────

app.use(express.static(path.join(__dirname, 'public')));

app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ─── Start ───────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`pgwatchtower dashboard running on http://localhost:${PORT}`);
});
