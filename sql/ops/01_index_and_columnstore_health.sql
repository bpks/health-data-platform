/*
  Index health check. Run weekly, or when a query regresses.

  Part 1  rowstore fragmentation with a recommended action (the usual guidance: below 5% do nothing,
          5-30% REORGANIZE, above 30% REBUILD; ignore tiny indexes, below ~1000 pages)
  Part 2  columnstore health: deleted rows and trimmed row groups are what slow scans down

  Read-only. It recommends; a person (or a maintenance job with change approval) decides.
*/

-- Part 1: rowstore fragmentation
SELECT
    OBJECT_SCHEMA_NAME(ips.object_id) AS schema_name,
    OBJECT_NAME(ips.object_id) AS table_name,
    i.name AS index_name,
    ips.index_type_desc,
    ips.page_count,
    CAST(ips.avg_fragmentation_in_percent AS DECIMAL(5, 1)) AS fragmentation_pct,
    CASE
        WHEN ips.avg_fragmentation_in_percent >= 30 THEN 'REBUILD'
        WHEN ips.avg_fragmentation_in_percent >= 5 THEN 'REORGANIZE'
        ELSE 'none'
    END AS recommended_action
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') AS ips
INNER JOIN sys.indexes AS i
    ON i.object_id = ips.object_id
    AND i.index_id = ips.index_id
WHERE ips.index_id > 0
    AND ips.page_count >= 1000
    AND ips.index_type_desc NOT LIKE '%COLUMNSTORE%'
ORDER BY ips.avg_fragmentation_in_percent DESC;

-- Part 2: columnstore row group health
SELECT
    OBJECT_SCHEMA_NAME(rg.object_id) AS schema_name,
    OBJECT_NAME(rg.object_id) AS table_name,
    COUNT(*) AS row_groups,
    SUM(rg.total_rows) AS total_rows,
    SUM(rg.deleted_rows) AS deleted_rows,
    CAST(100.0 * SUM(rg.deleted_rows) / NULLIF(SUM(rg.total_rows), 0) AS DECIMAL(5, 1)) AS deleted_pct,
    SUM(CASE WHEN rg.state_desc = 'OPEN' THEN 1 ELSE 0 END) AS open_row_groups,
    SUM(CASE WHEN rg.trim_reason_desc IN ('MEMORY_LIMITATION', 'DICTIONARY_SIZE') THEN 1 ELSE 0 END) AS trimmed_row_groups,
    CASE
        WHEN 100.0 * SUM(rg.deleted_rows) / NULLIF(SUM(rg.total_rows), 0) >= 20 THEN 'REORGANIZE (compress deleted rows away)'
        ELSE 'none'
    END AS recommended_action
FROM sys.dm_db_column_store_row_group_physical_stats AS rg
GROUP BY rg.object_id
ORDER BY deleted_pct DESC;
