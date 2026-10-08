/*
  Query Store: where is the time going, and what got worse?
  Requires Query Store enabled (it is on by default in Azure SQL Database):
    ALTER DATABASE CURRENT SET QUERY_STORE = ON;

  Part 1  top queries by total CPU over the last 7 days
  Part 2  queries whose average duration regressed against the previous 7 days (candidates for plan
          forcing or a missing statistic, after root cause analysis, not before)
*/

-- Part 1
SELECT TOP (20)
    q.query_id,
    LEFT(qt.query_sql_text, 200) AS query_text,
    SUM(rs.count_executions) AS executions,
    CAST(SUM(rs.avg_cpu_time * rs.count_executions) / 1000000.0 AS DECIMAL(12, 2)) AS total_cpu_seconds,
    CAST(SUM(rs.avg_duration * rs.count_executions) / NULLIF(SUM(rs.count_executions), 0) / 1000.0 AS DECIMAL(12, 2)) AS avg_duration_ms,
    CAST(SUM(rs.avg_logical_io_reads * rs.count_executions) / NULLIF(SUM(rs.count_executions), 0) AS DECIMAL(14, 0)) AS avg_logical_reads,
    COUNT(DISTINCT p.plan_id) AS plans
FROM sys.query_store_query AS q
INNER JOIN sys.query_store_query_text AS qt
    ON qt.query_text_id = q.query_text_id
INNER JOIN sys.query_store_plan AS p
    ON p.query_id = q.query_id
INNER JOIN sys.query_store_runtime_stats AS rs
    ON rs.plan_id = p.plan_id
INNER JOIN sys.query_store_runtime_stats_interval AS rsi
    ON rsi.runtime_stats_interval_id = rs.runtime_stats_interval_id
WHERE rsi.start_time >= DATEADD(DAY, -7, SYSUTCDATETIME())
GROUP BY q.query_id, LEFT(qt.query_sql_text, 200)
ORDER BY total_cpu_seconds DESC;

-- Part 2
WITH per_window AS
(
    SELECT
        p.query_id,
        CASE WHEN rsi.start_time >= DATEADD(DAY, -7, SYSUTCDATETIME()) THEN 'recent' ELSE 'previous' END AS time_window,
        SUM(rs.avg_duration * rs.count_executions) / NULLIF(SUM(rs.count_executions), 0) AS avg_duration_us,
        SUM(rs.count_executions) AS executions
    FROM sys.query_store_plan AS p
    INNER JOIN sys.query_store_runtime_stats AS rs
        ON rs.plan_id = p.plan_id
    INNER JOIN sys.query_store_runtime_stats_interval AS rsi
        ON rsi.runtime_stats_interval_id = rs.runtime_stats_interval_id
    WHERE rsi.start_time >= DATEADD(DAY, -14, SYSUTCDATETIME())
    GROUP BY
        p.query_id,
        CASE WHEN rsi.start_time >= DATEADD(DAY, -7, SYSUTCDATETIME()) THEN 'recent' ELSE 'previous' END
)

SELECT TOP (20)
    r.query_id,
    LEFT(qt.query_sql_text, 200) AS query_text,
    CAST(o.avg_duration_us / 1000.0 AS DECIMAL(12, 2)) AS previous_avg_ms,
    CAST(r.avg_duration_us / 1000.0 AS DECIMAL(12, 2)) AS recent_avg_ms,
    CAST(r.avg_duration_us / NULLIF(o.avg_duration_us, 0) AS DECIMAL(8, 2)) AS slowdown_factor,
    r.executions AS recent_executions
FROM per_window AS r
INNER JOIN per_window AS o
    ON o.query_id = r.query_id
    AND o.time_window = 'previous'
INNER JOIN sys.query_store_query AS q
    ON q.query_id = r.query_id
INNER JOIN sys.query_store_query_text AS qt
    ON qt.query_text_id = q.query_text_id
WHERE r.time_window = 'recent'
    AND r.executions >= 10
    AND r.avg_duration_us > 1.5 * o.avg_duration_us
ORDER BY slowdown_factor DESC;
