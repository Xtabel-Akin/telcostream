-- =============================================================
-- TelcoStream — Gold Analytical Views
-- These sit on top of Gold tables and are the serving layer.
-- Rebuilt on every schema deploy — safe to DROP and recreate.
-- =============================================================

USE DATABASE TELCOSTREAM;
USE SCHEMA GOLD;

-- ------------------------------------------------------------
-- 1. Current tower health status
-- Joins latest Silver events with current tower dimension record
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW GOLD.V_TOWER_CURRENT_HEALTH AS
WITH latest_events AS (
    SELECT
        tower_id,
        COUNT(*)                                        AS events_last_24h,
        SUM(CASE WHEN event_type = 'DROPPED_CALL'
                 THEN 1 ELSE 0 END)                    AS dropped_calls_24h,
        SUM(CASE WHEN severity = 'CRITICAL'
                 THEN 1 ELSE 0 END)                    AS critical_events_24h,
        AVG(metric_value)                              AS avg_metric_value,
        MAX(event_timestamp)                           AS last_event_time
    FROM SILVER.FACT_NETWORK_EVENTS
    WHERE event_timestamp >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
    GROUP BY tower_id
)
SELECT
    t.tower_id,
    t.tower_name,
    t.region,
    t.status                                           AS tower_status,
    t.latitude,
    t.longitude,
    COALESCE(e.events_last_24h, 0)                    AS events_last_24h,
    COALESCE(e.dropped_calls_24h, 0)                  AS dropped_calls_24h,
    COALESCE(e.critical_events_24h, 0)                AS critical_events_24h,
    ROUND(e.avg_metric_value, 2)                      AS avg_metric_value,
    e.last_event_time,
    CASE
        WHEN e.critical_events_24h > 10 THEN 'RED'
        WHEN e.critical_events_24h > 3  THEN 'AMBER'
        ELSE                                 'GREEN'
    END                                               AS health_indicator
FROM SILVER.DIM_TOWER t
LEFT JOIN latest_events e ON t.tower_id = e.tower_id
WHERE t.current_flag = TRUE;

-- ------------------------------------------------------------
-- 2. Drop rate trend — last 7 days by region, daily
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW GOLD.V_DROP_RATE_TREND AS
SELECT
    region,
    event_date,
    COUNT(*)                                          AS total_events,
    SUM(CASE WHEN event_type = 'DROPPED_CALL'
             THEN 1 ELSE 0 END)                       AS dropped_calls,
    ROUND(
        SUM(CASE WHEN event_type = 'DROPPED_CALL'
                 THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                                 AS drop_rate_pct,
    AVG(CASE WHEN event_type = 'LATENCY_ALERT'
             THEN metric_value END)                   AS avg_latency_ms
FROM SILVER.FACT_NETWORK_EVENTS
WHERE event_date >= DATEADD(day, -7, CURRENT_DATE())
GROUP BY region, event_date
ORDER BY region, event_date;

-- ------------------------------------------------------------
-- 3. P95 latency by tower, last hour
-- Useful for near real-time NOC monitoring
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW GOLD.V_LATENCY_P95_LAST_HOUR AS
SELECT
    tower_id,
    region,
    COUNT(*)                                          AS latency_events,
    ROUND(AVG(metric_value), 2)                       AS avg_latency_ms,
    ROUND(PERCENTILE_CONT(0.95)
          WITHIN GROUP (ORDER BY metric_value), 2)    AS p95_latency_ms,
    ROUND(MAX(metric_value), 2)                       AS max_latency_ms,
    MIN(event_timestamp)                              AS window_start,
    MAX(event_timestamp)                              AS window_end
FROM SILVER.FACT_NETWORK_EVENTS
WHERE event_type = 'LATENCY_ALERT'
  AND event_timestamp >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
GROUP BY tower_id, region
ORDER BY p95_latency_ms DESC;

-- ------------------------------------------------------------
-- 4. Pipeline health — recent run audit
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW GOLD.V_PIPELINE_HEALTH AS
SELECT
    layer,
    status,
    COUNT(*)                                          AS run_count,
    SUM(records_processed)                            AS total_records,
    SUM(records_rejected)                             AS total_rejected,
    ROUND(AVG(DATEDIFF('second', run_start, run_end)), 1) AS avg_duration_sec,
    MAX(run_end)                                      AS last_run
FROM CONTROL.PIPELINE_RUN_LOG
WHERE run_start >= DATEADD(day, -1, CURRENT_TIMESTAMP())
GROUP BY layer, status
ORDER BY layer, status;