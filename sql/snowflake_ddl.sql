-- =============================================================
-- TelcoStream — Snowflake DDL
-- All objects created idempotently (CREATE OR REPLACE / IF NOT EXISTS)
-- Run order matters: database → schema → warehouse → tables → views
-- =============================================================

-- ------------------------------------------------------------
-- 1. DATABASE & SCHEMA
-- ------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS TELCOSTREAM;
USE DATABASE TELCOSTREAM;

CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;
CREATE SCHEMA IF NOT EXISTS CONTROL;   -- pipeline metadata, dead letter

-- ------------------------------------------------------------
-- 2. VIRTUAL WAREHOUSE
-- Sized XS — sufficient for near real-time micro-batch loads.
-- Auto-suspend after 60s to protect trial credits.
-- ------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS TELCOSTREAM_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    COMMENT        = 'TelcoStream pipeline warehouse';

USE WAREHOUSE TELCOSTREAM_WH;

-- ------------------------------------------------------------
-- 3. BRONZE LAYER — raw ingest, schema-on-read
-- Append-only. No transforms. Exactly what arrived.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS BRONZE.RAW_NETWORK_EVENTS (
    event_id        VARCHAR(64),
    event_type      VARCHAR(32),        -- DROPPED_CALL | LATENCY_ALERT | TOWER_STATUS
    tower_id        VARCHAR(16),
    region          VARCHAR(32),
    severity        VARCHAR(8),         -- LOW | MEDIUM | HIGH | CRITICAL
    metric_value    FLOAT,              -- latency ms, signal strength dBm, etc.
    event_timestamp TIMESTAMP_NTZ,      -- original event time (device clock)
    ingest_timestamp TIMESTAMP_NTZ      -- when Spark received it
        DEFAULT CURRENT_TIMESTAMP(),
    payload         VARIANT,            -- full raw JSON preserved
    _source_file    VARCHAR(512),       -- Auto Loader source file path
    _batch_id       VARCHAR(64)         -- Spark streaming batch ID
);

-- Dead letter table — malformed or schema-violating records
CREATE TABLE IF NOT EXISTS CONTROL.DEAD_LETTER (
    raw_record      VARIANT,
    error_message   VARCHAR(2048),
    source_file     VARCHAR(512),
    failed_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ------------------------------------------------------------
-- 4. SILVER LAYER — cleaned, deduplicated, enriched
-- ------------------------------------------------------------

-- Tower dimension — SCD Type 2
-- Tracks history of tower status changes over time.
-- current_flag = TRUE means this is the active record.
CREATE TABLE IF NOT EXISTS SILVER.DIM_TOWER (
    tower_sk            NUMBER AUTOINCREMENT PRIMARY KEY,  -- surrogate key
    tower_id            VARCHAR(16),
    tower_name          VARCHAR(64),
    region              VARCHAR(32),
    latitude            FLOAT,
    longitude           FLOAT,
    status              VARCHAR(16),    -- ACTIVE | DEGRADED | OFFLINE
    capacity_channels   INTEGER,
    effective_from      TIMESTAMP_NTZ,
    effective_to        TIMESTAMP_NTZ,  -- NULL = current record
    current_flag        BOOLEAN DEFAULT TRUE,
    _created_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _updated_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Cleaned fact events — deduplicated, watermarked, tower SK resolved
CREATE TABLE IF NOT EXISTS SILVER.FACT_NETWORK_EVENTS (
    event_id            VARCHAR(64) PRIMARY KEY,
    event_type          VARCHAR(32),
    tower_sk            NUMBER,         -- FK to DIM_TOWER.tower_sk
    tower_id            VARCHAR(16),
    region              VARCHAR(32),
    severity            VARCHAR(8),
    metric_value        FLOAT,
    severity_score      INTEGER,        -- numeric: LOW=1, MEDIUM=2, HIGH=3, CRITICAL=4
    event_timestamp     TIMESTAMP_NTZ,
    event_date          DATE,           -- partition key for downstream queries
    ingest_timestamp    TIMESTAMP_NTZ,
    processing_latency_ms FLOAT,        -- ingest_timestamp - event_timestamp
    _batch_id           VARCHAR(64),
    _processed_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ------------------------------------------------------------
-- 5. GOLD LAYER — aggregated, serving-ready
-- ------------------------------------------------------------

-- Hourly KPIs per tower
CREATE TABLE IF NOT EXISTS GOLD.TOWER_HOURLY_KPI (
    tower_id            VARCHAR(16),
    region              VARCHAR(32),
    event_hour          TIMESTAMP_NTZ,
    total_events        INTEGER,
    dropped_calls       INTEGER,
    latency_alerts      INTEGER,
    tower_status_changes INTEGER,
    critical_events     INTEGER,
    avg_metric_value    FLOAT,
    p95_metric_value    FLOAT,
    max_metric_value    FLOAT,
    _updated_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (tower_id, event_hour)
);

-- Regional drop rate rolling summary
CREATE TABLE IF NOT EXISTS GOLD.REGIONAL_DROP_SUMMARY (
    region              VARCHAR(32),
    summary_date        DATE,
    total_events        INTEGER,
    drop_rate_pct       FLOAT,          -- dropped_calls / total_events * 100
    avg_latency_ms      FLOAT,
    critical_towers     INTEGER,        -- distinct towers with CRITICAL events
    _updated_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (region, summary_date)
);

-- Pipeline run audit log
CREATE TABLE IF NOT EXISTS CONTROL.PIPELINE_RUN_LOG (
    run_id              VARCHAR(64),
    layer               VARCHAR(8),     -- BRONZE | SILVER | GOLD
    batch_id            VARCHAR(64),
    records_processed   INTEGER,
    records_rejected    INTEGER,
    run_start           TIMESTAMP_NTZ,
    run_end             TIMESTAMP_NTZ,
    status              VARCHAR(16),    -- SUCCESS | FAILED | PARTIAL
    error_message       VARCHAR(2048),
    _logged_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ------------------------------------------------------------
-- 6. SNOWPIPE — auto-ingest trigger for Bronze
-- NOTE: The STAGE and PIPE are created separately after
-- Databricks storage integration is configured.
-- Placeholder commented out — Phase 5 will complete this.
-- ------------------------------------------------------------
-- CREATE STAGE IF NOT EXISTS BRONZE.TELCO_STAGE ...
-- CREATE PIPE IF NOT EXISTS BRONZE.TELCO_PIPE ...

-- ------------------------------------------------------------
-- 7. ROLE & ACCESS (good hygiene, even on a trial account)
-- ------------------------------------------------------------
CREATE ROLE IF NOT EXISTS TELCOSTREAM_PIPELINE;
GRANT USAGE ON DATABASE TELCOSTREAM TO ROLE TELCOSTREAM_PIPELINE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE TELCOSTREAM TO ROLE TELCOSTREAM_PIPELINE;
GRANT INSERT, SELECT ON ALL TABLES IN DATABASE TELCOSTREAM TO ROLE TELCOSTREAM_PIPELINE;
GRANT USAGE ON WAREHOUSE TELCOSTREAM_WH TO ROLE TELCOSTREAM_PIPELINE;