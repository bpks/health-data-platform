/*
  Fact tables and the monthly mart.

  * Clustered columnstore indexes: analytic scans, compression, batch-mode execution.
  * The primary key is NONCLUSTERED and enforced, so key lookups for MERGE stay cheap.
  * site_region is deliberately repeated on every fact. The row-level security predicate reads it,
    and a predicate that has to join to dim_site on every query would be slow.
*/

IF OBJECT_ID('dw.fact_application') IS NULL
CREATE TABLE dw.fact_application
(
    application_key    BIGINT        NOT NULL,
    application_id     NVARCHAR(12)  NOT NULL,
    study_key          BIGINT        NOT NULL,
    site_key           BIGINT        NOT NULL,
    site_region        NVARCHAR(40)  NOT NULL,
    submitted_date_key INT           NOT NULL,
    decision_date_key  INT           NOT NULL,   -- -1 while the application is still open
    status             NVARCHAR(20)  NOT NULL,
    is_decided         BIT           NOT NULL,
    is_approved        BIT           NOT NULL,
    days_to_decision   INT           NULL,       -- NULL until decided
    met_sla            BIT           NULL,       -- NULL until decided (NULL means "not yet", not "no")
    updated_at         DATETIME2(0)  NOT NULL,
    CONSTRAINT pk_fact_application PRIMARY KEY NONCLUSTERED (application_key),
    CONSTRAINT fk_fact_application_study FOREIGN KEY (study_key) REFERENCES dw.dim_study (study_key),
    CONSTRAINT fk_fact_application_site FOREIGN KEY (site_key) REFERENCES dw.dim_site (site_key),
    CONSTRAINT fk_fact_application_submitted FOREIGN KEY (submitted_date_key) REFERENCES dw.dim_date (date_key),
    CONSTRAINT fk_fact_application_decision FOREIGN KEY (decision_date_key) REFERENCES dw.dim_date (date_key)
);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'cci_fact_application' AND object_id = OBJECT_ID('dw.fact_application'))
    CREATE CLUSTERED COLUMNSTORE INDEX cci_fact_application ON dw.fact_application;
GO

IF OBJECT_ID('dw.fact_enrolment') IS NULL
CREATE TABLE dw.fact_enrolment
(
    enrolment_key         BIGINT        NOT NULL,
    enrolment_id          NVARCHAR(12)  NOT NULL,
    participant_key       BIGINT        NOT NULL,
    study_key             BIGINT        NOT NULL,
    site_key              BIGINT        NOT NULL,
    site_region           NVARCHAR(40)  NOT NULL,
    enrolled_date_key     INT           NOT NULL,
    status                NVARCHAR(20)  NOT NULL,
    age_band_at_enrolment NVARCHAR(10)  NOT NULL,   -- approximate: derived from birth year only
    enrolment_count       INT           NOT NULL,   -- always 1; lets BI tools SUM() the grain
    updated_at            DATETIME2(0)  NOT NULL,
    CONSTRAINT pk_fact_enrolment PRIMARY KEY NONCLUSTERED (enrolment_key),
    CONSTRAINT fk_fact_enrolment_participant FOREIGN KEY (participant_key) REFERENCES dw.dim_participant (participant_key),
    CONSTRAINT fk_fact_enrolment_study FOREIGN KEY (study_key) REFERENCES dw.dim_study (study_key),
    CONSTRAINT fk_fact_enrolment_site FOREIGN KEY (site_key) REFERENCES dw.dim_site (site_key),
    CONSTRAINT fk_fact_enrolment_date FOREIGN KEY (enrolled_date_key) REFERENCES dw.dim_date (date_key)
);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'cci_fact_enrolment' AND object_id = OBJECT_ID('dw.fact_enrolment'))
    CREATE CLUSTERED COLUMNSTORE INDEX cci_fact_enrolment ON dw.fact_enrolment;
GO

-- Study x region x month. Measures are computed once in the Spark gold layer
-- (src/hdp/measures.py) and loaded as-is; the SQL layer does not redefine them.
IF OBJECT_ID('dw.mart_monthly_performance') IS NULL
CREATE TABLE dw.mart_monthly_performance
(
    study_key                 BIGINT         NOT NULL,
    site_region               NVARCHAR(40)   NOT NULL,
    month_key                 INT            NOT NULL,   -- yyyymm
    applications_submitted    BIGINT         NOT NULL,
    applications_decided      BIGINT         NOT NULL,
    applications_approved     BIGINT         NOT NULL,
    decided_within_target     BIGINT         NOT NULL,
    avg_days_to_decision      DECIMAL(9, 1)  NULL,
    enrolments                BIGINT         NOT NULL,
    pct_decided_within_target DECIMAL(5, 1)  NULL,
    CONSTRAINT pk_mart_monthly PRIMARY KEY NONCLUSTERED (study_key, site_region, month_key)
);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'cci_mart_monthly' AND object_id = OBJECT_ID('dw.mart_monthly_performance'))
    CREATE CLUSTERED COLUMNSTORE INDEX cci_mart_monthly ON dw.mart_monthly_performance;
GO
