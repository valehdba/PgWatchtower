# Changelog

All notable changes to PgWatchtower are documented here.

## [1.2.0] — Parallel Cloning, Resume Support & Conflict Resolution

### Added
- **Parallel cloning** — sync metrics to multiple remote destinations simultaneously
  - `watchtower.clone_targets` table for managing multiple remote databases
  - `watchtower.add_clone_target()` / `watchtower.remove_clone_target()` for target management
  - `watchtower.parallel_clone()` — sync to all active targets with parallel connectivity
  - `watchtower.sync_to_target()` — sync to a specific target with full resume support
- **Resume support** — interrupted syncs pick up where they left off
  - `watchtower.sync_checkpoints` table tracks last synced position per target per data type
  - Configurable batch sizes via `clone_batch_size` setting
  - `resume_enabled` toggle in configuration
- **Conflict resolution** — configurable policies for handling data conflicts
  - `watchtower.conflict_log` — full audit trail of every resolved conflict
  - Policies: `local_wins`, `remote_wins`, `newest_wins`, `manual`
  - `watchtower.v_conflict_summary` view for conflict analytics
- **Sync status monitoring**
  - `watchtower.v_sync_status` view — at-a-glance status of all clone targets
  - Per-target last sync time, record counts, and 24h conflict counts

### Changed
- `watchtower.collect_and_ship_sizes()` now automatically uses parallel clone when targets are configured, falls back to original single-target behavior otherwise

### Upgrade
```sql
ALTER EXTENSION pgwatchtower UPDATE TO '1.2.0';
```

---

## [1.1.0] — Background Worker & Progress Tracking

### Added
- **Async job queue** — submit collection jobs that run in the background
  - `watchtower.job_queue` table with priority, retry, and timeout support
  - `watchtower.submit_collection_job()` — queue a new collection
  - `watchtower.execute_job()` — run a specific queued job
  - `watchtower.poll_and_execute()` — background worker entry point
- **Progress tracking** — monitor each step of a collection job
  - `watchtower.job_progress` table with per-step status, item counts, and timing
  - `watchtower.get_job_status()` — detailed job status with step-by-step progress as JSONB
  - `watchtower.list_jobs()` — list recent jobs filtered by status
- **Job management**
  - `watchtower.cancel_job()` — cancel pending or running jobs
  - `watchtower.cleanup_jobs()` — purge old completed/failed jobs
  - Automatic timeout detection and retry for stale jobs
- **Worker configuration** — new settings in `remote_config`
  - `worker_enabled`, `worker_interval_sec`, `worker_max_parallel`
  - `worker_retry_delay_sec`, `worker_timeout_sec`
- **pg_cron integration**
  - `watchtower.setup_cron_worker()` — one-call setup for worker polling, daily submission, and weekly cleanup

### Upgrade
```sql
ALTER EXTENSION pgwatchtower UPDATE TO '1.1.0';
```

---

## [1.0.0] — Initial Release

### Added
- Core extension with `watchtower` schema
- `watchtower.remote_config` — singleton configuration for remote metrics DB
- `watchtower.collect_and_ship_sizes()` — collect DB and table sizes, ship via dblink
- `watchtower._collect_db_sizes()` / `watchtower._collect_table_sizes()` — size collectors
- `watchtower.set_remote_config()` — friendly configuration API
- `watchtower.test_connection()` — verify remote connectivity
- `watchtower.show_db_sizes()` / `watchtower.show_table_sizes()` — local size views
- Remote schema with analytical views (`v_db_daily_growth`, `v_table_daily_growth`, `v_top_growing_tables`)
- Web dashboard with Chart.js visualizations (5 pages)
- Demo data generator for testing
