/*
  Staging tables mirror the gold tables in the lake one-to-one. The pipeline (Data Factory copy
  activity, a Fabric pipeline or a notebook) truncates and reloads them; the load procedures in
  sql/procs/ then merge them into the dw schema. Keeping staging identical to gold keeps the SQL
  free of business logic.
*/

IF OBJECT_ID('stg.dim_site') IS NULL
CREATE TABLE stg.dim_site
(
    site_key   BIGINT        NOT NULL,
    site_id    NVARCHAR(10)  NOT NULL,
    site_name  NVARCHAR(200) NULL,
    region     NVARCHAR(40)  NULL,
    trust_type NVARCHAR(30)  NULL,
    valid_from DATE          NOT NULL,
    valid_to   DATE          NOT NULL,
    is_current BIT           NOT NULL,
    row_hash   CHAR(64)      NULL
);
GO

IF OBJECT_ID('stg.fact_enrolment') IS NULL
CREATE TABLE stg.fact_enrolment
(
    enrolment_key         BIGINT        NOT NULL,
    enrolment_id          NVARCHAR(12)  NOT NULL,
    participant_key       BIGINT        NOT NULL,
    study_key             BIGINT        NOT NULL,
    site_key              BIGINT        NOT NULL,
    site_region           NVARCHAR(40)  NOT NULL,
    enrolled_date_key     INT           NOT NULL,
    status                NVARCHAR(20)  NOT NULL,
    age_band_at_enrolment NVARCHAR(10)  NOT NULL,
    enrolment_count       INT           NOT NULL,
    updated_at            DATETIME2(0)  NOT NULL
);
GO

-- One row per load attempt: the data source for "did last night's load run, and how long did it take?"
IF OBJECT_ID('ops.load_log') IS NULL
CREATE TABLE ops.load_log
(
    load_id        BIGINT IDENTITY(1, 1) NOT NULL,
    procedure_name SYSNAME               NOT NULL,
    started_at     DATETIME2(0)          NOT NULL,
    finished_at    DATETIME2(0)          NULL,
    status         VARCHAR(10)           NOT NULL,   -- running | success | failed
    rows_inserted  BIGINT                NULL,
    rows_updated   BIGINT                NULL,
    message        NVARCHAR(2000)        NULL,
    CONSTRAINT pk_load_log PRIMARY KEY CLUSTERED (load_id),
    CONSTRAINT ck_load_log_status CHECK (status IN ('running', 'success', 'failed'))
);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_load_log_proc_started' AND object_id = OBJECT_ID('ops.load_log'))
    CREATE NONCLUSTERED INDEX ix_load_log_proc_started ON ops.load_log (procedure_name, started_at DESC);
GO
