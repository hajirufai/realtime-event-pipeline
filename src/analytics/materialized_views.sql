-- Materialized views for the analytics engine
-- Run these periodically to pre-compute common aggregations

-- Hourly event summary
CREATE OR REPLACE VIEW hourly_summary AS
SELECT
    DATE_TRUNC('hour', timestamp) as hour,
    event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT session_id) as unique_sessions
FROM events
GROUP BY 1, 2;

-- User session summary
CREATE OR REPLACE VIEW session_summary AS
SELECT
    session_id,
    user_id,
    user_segment,
    MIN(timestamp) as session_start,
    MAX(timestamp) as session_end,
    COUNT(*) as event_count,
    COUNT(DISTINCT event_type) as unique_event_types,
    MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as converted
FROM events
GROUP BY 1, 2, 3;

-- Daily conversion rates
CREATE OR REPLACE VIEW daily_conversions AS
SELECT
    DATE_TRUNC('day', session_start) as day,
    COUNT(*) as total_sessions,
    SUM(converted) as converted_sessions,
    ROUND(100.0 * SUM(converted) / COUNT(*), 2) as conversion_rate
FROM session_summary
GROUP BY 1
ORDER BY 1 DESC;
