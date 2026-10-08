/*
  Missing-index suggestions from the optimiser, ranked by estimated benefit.

  Treat these as hints, not instructions: the optimiser suggests per query and ignores the write cost
  of every extra index and overlap with indexes you already have. Review against the real workload
  (Query Store, 03_query_store_top_queries.sql) and consolidate before creating anything.
  The suggestions reset when the instance restarts, so a quiet list after a restart means little.
*/

SELECT TOP (20)
    OBJECT_SCHEMA_NAME(mid.object_id) AS schema_name,
    OBJECT_NAME(mid.object_id) AS table_name,
    migs.user_seeks,
    migs.user_scans,
    CAST(migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) * (migs.user_seeks + migs.user_scans) AS DECIMAL(18, 1)) AS estimated_benefit,
    mid.equality_columns,
    mid.inequality_columns,
    mid.included_columns
FROM sys.dm_db_missing_index_groups AS mig
INNER JOIN sys.dm_db_missing_index_group_stats AS migs
    ON migs.group_handle = mig.index_group_handle
INNER JOIN sys.dm_db_missing_index_details AS mid
    ON mid.index_handle = mig.index_handle
WHERE mid.database_id = DB_ID()
ORDER BY estimated_benefit DESC;
