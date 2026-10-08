/*
  Statistics freshness. Stale statistics make the optimiser pick bad plans, and that is a common cause
  of "the same query was fast last month". Lists statistics where a meaningful share of rows changed
  since the last update.

  Rule of thumb used here: modification_counter above 10% of rows (and above 1,000 rows).
*/

SELECT
    OBJECT_SCHEMA_NAME(s.object_id) AS schema_name,
    OBJECT_NAME(s.object_id) AS table_name,
    s.name AS statistics_name,
    sp.last_updated,
    sp.[rows],
    sp.rows_sampled,
    sp.modification_counter,
    CAST(100.0 * sp.modification_counter / NULLIF(sp.[rows], 0) AS DECIMAL(6, 1)) AS modified_pct,
    DATEDIFF(DAY, sp.last_updated, SYSUTCDATETIME()) AS days_since_update
FROM sys.stats AS s
CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) AS sp
WHERE OBJECTPROPERTY(s.object_id, 'IsUserTable') = 1
    AND sp.modification_counter > 1000
    AND sp.modification_counter > 0.10 * sp.[rows]
ORDER BY modified_pct DESC;

-- To act on one table after review (a change, so raise it through change management):
--   UPDATE STATISTICS dw.fact_enrolment WITH FULLSCAN;
