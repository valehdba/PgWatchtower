# PgWatchtower — PostgreSQL Database & Table Size Analytics

A complete system for collecting, shipping, and visualizing PostgreSQL database and table size metrics over time.

## Architecture

```
┌──────────────────────┐       ┌─────────────────────────┐       ┌──────────────────────┐
│  Source PostgreSQL    │       │  Remote Metrics DB       │       │  Web Dashboard        │
│  ──────────────────   │       │  ──────────────────────  │       │  ────────────────────  │
│  pgwatchtower ext  │──────▶│  watchtower schema     │◀──────│  React + Chart.js     │
│  - daily cron job     │ dblink│  - db_size_history       │  API  │  - growth trends      │
│  - config table       │       │  - table_size_history    │       │  - analytics          │
│  - collector function │       │  - collection_log        │       │  - forecasting        │
└──────────────────────┘       └─────────────────────────┘       └──────────────────────┘
```

## Components

### 1. PostgreSQL Extension (`extension/`)
- Installs into any PostgreSQL 12+ instance
- Creates a configuration table for remote DB connection
- Provides `collect_and_ship_sizes()` function
- Gathers database and per-table sizes daily
- Ships data via `dblink` to the configured remote database

### 2. Remote Schema (`remote-schema/`)
- SQL migration to set up the metrics storage database
- Tables: `db_size_history`, `table_size_history`, `collection_log`
- Indexes optimized for time-range queries
- Views for common analytics (growth rates, top growers)

### 3. Web Dashboard (`webapp/`)
- Single-page React application
- Connects to the remote metrics database via a Node.js API
- Interactive charts for size trends, growth rates, and forecasting
- Filter by database, schema, table, and date range

## Quick Start

### Step 1: Set up the Remote Metrics Database

```bash
psql -h metrics-server -U postgres -f remote-schema/001_create_schema.sql
```

### Step 2: Install the Extension on Source Databases

```bash
cd extension/
make install
# Then in psql:
CREATE EXTENSION pgwatchtower;
```

### Step 3: Configure the Remote Connection

```sql
SELECT watchtower.set_remote_config(
    'metrics-server',    -- host
    5432,                -- port
    'metrics_db',        -- database
    'metrics_user',      -- username
    'secure_password'    -- password
);
```

### Step 4: Test Collection

```sql
SELECT watchtower.collect_and_ship_sizes();
```

### Step 5: Schedule Daily Collection (pg_cron)

```sql
SELECT cron.schedule(
    'daily-size-collection',
    '0 2 * * *',  -- 2 AM daily
    $$SELECT watchtower.collect_and_ship_sizes()$$
);
```

### Step 6: Launch the Dashboard

```bash
cd webapp/
cp .env.example .env  # Edit with your metrics DB credentials
npm install
npm start
```

## Requirements

- PostgreSQL 12+ (source databases)
- `dblink` extension (ships with PostgreSQL)
- `pg_cron` (optional, for scheduling)
- Node.js 18+ (for the web dashboard)
