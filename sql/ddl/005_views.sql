/*
  Reporting views. They only join and rename: every measure is computed once in the lake
  (src/hdp/measures.py), so Power BI, Excel and ad-hoc SQL all report the same numbers.
  Row-level security and masking apply automatically because the views read the protected tables.
*/

CREATE OR ALTER VIEW dw.vw_study_performance
AS
SELECT
    s.study_id,
    s.title,
    s.study_type,
    s.risk_level,
    m.site_region AS region,
    m.month_key,
    d.[year],
    d.month_name,
    d.financial_year,
    m.applications_submitted,
    m.applications_decided,
    m.applications_approved,
    m.avg_days_to_decision,
    m.pct_decided_within_target,
    m.enrolments
FROM dw.mart_monthly_performance AS m
INNER JOIN dw.dim_study AS s
    ON s.study_key = m.study_key
LEFT JOIN dw.dim_date AS d
    ON d.date_key = (m.month_key * 100) + 1;   -- first day of the month
GO

CREATE OR ALTER VIEW dw.vw_enrolment_detail
AS
SELECT
    f.enrolment_id,
    s.study_id,
    s.study_type,
    si.site_name,
    f.site_region AS region,
    d.[date] AS enrolled_on,
    d.financial_year,
    f.status,
    f.age_band_at_enrolment,
    p.sex,
    p.ethnicity,           -- masked unless the caller holds UNMASK
    p.outward_postcode     -- masked unless the caller holds UNMASK
FROM dw.fact_enrolment AS f
INNER JOIN dw.dim_study AS s
    ON s.study_key = f.study_key
INNER JOIN dw.dim_site AS si
    ON si.site_key = f.site_key
INNER JOIN dw.dim_participant AS p
    ON p.participant_key = f.participant_key
INNER JOIN dw.dim_date AS d
    ON d.date_key = f.enrolled_date_key;
GO

GRANT SELECT ON dw.vw_study_performance TO role_analyst;
GRANT SELECT ON dw.vw_enrolment_detail TO role_analyst;
GO
