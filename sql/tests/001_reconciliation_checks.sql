/*
  Post-load reconciliation. Run after every load; any THROW means the load must not be signed off.
  Run as a member of role_all_regions: row-level security would otherwise hide rows and make the
  counts meaningless.
*/
SET NOCOUNT ON;

-- 1. Every dimension has exactly one unknown member (key -1)
IF EXISTS (
    SELECT 1
    FROM (
        SELECT COUNT(*) AS n FROM dw.dim_study WHERE study_key = -1
        UNION ALL SELECT COUNT(*) FROM dw.dim_site WHERE site_key = -1
        UNION ALL SELECT COUNT(*) FROM dw.dim_participant WHERE participant_key = -1
        UNION ALL SELECT COUNT(*) FROM dw.dim_date WHERE date_key = -1
    ) AS c
    WHERE c.n <> 1
)
    THROW 50101, 'A dimension is missing its unknown member, or has more than one.', 1;

-- 2. No fact points at the unknown member: that would mean a reference failed to resolve upstream
IF EXISTS (SELECT 1 FROM dw.fact_application WHERE study_key = -1 OR site_key = -1)
    THROW 50102, 'fact_application has rows that resolved to the unknown study or site.', 1;
IF EXISTS (SELECT 1 FROM dw.fact_enrolment WHERE study_key = -1 OR site_key = -1 OR participant_key = -1)
    THROW 50103, 'fact_enrolment has rows that resolved to an unknown dimension member.', 1;

-- 3. At most one current version per site
IF EXISTS (SELECT 1 FROM dw.dim_site WHERE is_current = 1 GROUP BY site_id HAVING COUNT(*) > 1)
    THROW 50104, 'dim_site has more than one current version for a site.', 1;

-- 4. Versions of a site must not overlap (half-open intervals)
IF EXISTS (
    SELECT 1
    FROM dw.dim_site AS a
    INNER JOIN dw.dim_site AS b
        ON a.site_id = b.site_id
        AND a.site_key <> b.site_key
        AND a.valid_from < b.valid_to
        AND b.valid_from < a.valid_to
)
    THROW 50105, 'dim_site has overlapping validity intervals.', 1;

-- 5. Each fact sits inside the validity window of the site version it points to
IF EXISTS (
    SELECT 1
    FROM dw.fact_enrolment AS f
    INNER JOIN dw.dim_site AS s ON s.site_key = f.site_key
    INNER JOIN dw.dim_date AS d ON d.date_key = f.enrolled_date_key
    WHERE d.[date] < s.valid_from OR d.[date] >= s.valid_to
)
    THROW 50106, 'fact_enrolment points at a site version that was not valid on the enrolment date.', 1;

-- 6. The mart reconciles to the facts
IF (SELECT SUM(applications_submitted) FROM dw.mart_monthly_performance) <> (SELECT COUNT(*) FROM dw.fact_application)
    THROW 50107, 'mart applications_submitted does not equal the fact_application row count.', 1;
IF (SELECT SUM(enrolments) FROM dw.mart_monthly_performance) <> (SELECT COUNT(*) FROM dw.fact_enrolment)
    THROW 50108, 'mart enrolments does not equal the fact_enrolment row count.', 1;

-- 7. The last load of each procedure succeeded
IF EXISTS (
    SELECT 1
    FROM ops.load_log AS l
    WHERE l.load_id = (SELECT MAX(l2.load_id) FROM ops.load_log AS l2 WHERE l2.procedure_name = l.procedure_name)
        AND l.status <> 'success'
)
    THROW 50109, 'The most recent run of a load procedure did not succeed. See ops.load_log.', 1;

PRINT 'All reconciliation checks passed.';
