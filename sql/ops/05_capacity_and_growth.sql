/*
  Capacity management: what is big, how full is the log, and how fast are we growing?

  Part 1  size per table (reserved space and rows), largest first
  Part 2  transaction log usage: a log that sits near full usually means a long-running open
          transaction or a stalled log backup/replication, find the cause before growing it
  Part 3  month-on-month load volume, taken from ops.load_log: cheap capacity-trend evidence
*/

-- Part 1: table sizes
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    SUM(CASE WHEN ps.index_id IN (0, 1) THEN ps.row_count ELSE 0 END) AS row_count,
    CAST(SUM(ps.reserved_page_count) * 8 / 1024.0 AS DECIMAL(14, 1)) AS reserved_mb,
    CAST(SUM(ps.used_page_count) * 8 / 1024.0 AS DECIMAL(14, 1)) AS used_mb
FROM sys.dm_db_partition_stats AS ps
INNER JOIN sys.tables AS t
    ON t.object_id = ps.object_id
INNER JOIN sys.schemas AS s
    ON s.schema_id = t.schema_id
GROUP BY s.name, t.name
ORDER BY reserved_mb DESC;

-- Part 2: log space
SELECT
    CAST(total_log_size_in_bytes / 1048576.0 AS DECIMAL(14, 1)) AS log_size_mb,
    CAST(used_log_space_in_bytes / 1048576.0 AS DECIMAL(14, 1)) AS used_mb,
    CAST(used_log_space_in_percent AS DECIMAL(5, 1)) AS used_pct
FROM sys.dm_db_log_space_usage;

-- Part 3: rows loaded per month by procedure
SELECT
    DATEFROMPARTS(YEAR(started_at), MONTH(started_at), 1) AS [month],
    procedure_name,
    COUNT(*) AS runs,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_runs,
    SUM(ISNULL(rows_inserted, 0)) AS rows_inserted,
    SUM(ISNULL(rows_updated, 0)) AS rows_updated
FROM ops.load_log
GROUP BY DATEFROMPARTS(YEAR(started_at), MONTH(started_at), 1), procedure_name
ORDER BY [month] DESC, procedure_name ASC;
