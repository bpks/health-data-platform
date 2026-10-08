/*
  Live triage. Run during an incident when someone says "the database is slow".

  Part 1  who is blocking whom right now, and what they are running
  Part 2  what the database has been waiting on (Azure SQL Database: sys.dm_db_wait_stats;
          SQL Server: use sys.dm_os_wait_stats instead)

  Read-only. Killing a session is a decision for the incident manager, never part of this script.
*/

-- Part 1: current blocking chain
SELECT
    r.session_id,
    r.blocking_session_id,
    r.wait_type,
    r.wait_time AS wait_ms,
    r.status,
    r.command,
    DB_NAME(r.database_id) AS database_name,
    s.login_name,
    s.host_name,
    s.program_name,
    SUBSTRING(
        t.[text],
        (r.statement_start_offset / 2) + 1,
        ((CASE r.statement_end_offset WHEN -1 THEN DATALENGTH(t.[text]) ELSE r.statement_end_offset END - r.statement_start_offset) / 2) + 1
    ) AS running_statement
FROM sys.dm_exec_requests AS r
INNER JOIN sys.dm_exec_sessions AS s
    ON s.session_id = r.session_id
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS t
WHERE r.blocking_session_id <> 0
    OR r.session_id IN (SELECT blocking_session_id FROM sys.dm_exec_requests WHERE blocking_session_id <> 0)
ORDER BY r.blocking_session_id, r.session_id;

-- Part 2: top waits, ignoring benign background waits
SELECT TOP (15)
    ws.wait_type,
    ws.waiting_tasks_count,
    CAST(ws.wait_time_ms / 1000.0 AS DECIMAL(14, 1)) AS wait_seconds,
    CAST(ws.signal_wait_time_ms / 1000.0 AS DECIMAL(14, 1)) AS signal_seconds,
    CAST(100.0 * ws.wait_time_ms / SUM(ws.wait_time_ms) OVER () AS DECIMAL(5, 1)) AS pct_of_total_wait
FROM sys.dm_db_wait_stats AS ws
WHERE ws.wait_type NOT IN
    ('SLEEP_TASK', 'LAZYWRITER_SLEEP', 'XE_TIMER_EVENT', 'XE_DISPATCHER_WAIT', 'BROKER_TASK_STOP',
     'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', 'CLR_AUTO_EVENT', 'CLR_MANUAL_EVENT', 'DIRTY_PAGE_POLL', 'HADR_FILESTREAM_IOMGR_IOCOMPLETION')
    AND ws.wait_time_ms > 0
ORDER BY ws.wait_time_ms DESC;
