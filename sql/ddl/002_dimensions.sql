/*
  Conformed dimensions. Surrogate keys are BIGINT hashes produced by the Spark gold layer, so the
  lake and the serving layer agree on keys without sharing a sequence. Every dimension has an
  unknown member (key -1), loaded with the data, so facts never carry a dangling key.
*/

IF OBJECT_ID('dw.dim_date') IS NULL
CREATE TABLE dw.dim_date
(
    date_key       INT           NOT NULL,
    [date]         DATE          NULL,
    [year]         SMALLINT      NULL,
    [quarter]      TINYINT       NULL,
    [month]        TINYINT       NULL,
    month_name     NVARCHAR(20)  NULL,
    year_month     NVARCHAR(10)  NULL,
    day_name       NVARCHAR(20)  NULL,
    is_weekend     BIT           NULL,
    financial_year NVARCHAR(10)  NULL,   -- UK financial year, April to March, e.g. 2025/26
    CONSTRAINT pk_dim_date PRIMARY KEY CLUSTERED (date_key)
);
GO

IF OBJECT_ID('dw.dim_study') IS NULL
CREATE TABLE dw.dim_study
(
    study_key  BIGINT         NOT NULL,
    study_id   NVARCHAR(10)   NOT NULL,
    title      NVARCHAR(200)  NULL,
    study_type NVARCHAR(50)   NULL,
    sponsor    NVARCHAR(100)  NULL,
    risk_level NVARCHAR(10)   NULL,
    start_date DATE           NULL,
    CONSTRAINT pk_dim_study PRIMARY KEY CLUSTERED (study_key),
    CONSTRAINT uq_dim_study_id UNIQUE (study_id)
);
GO

-- Type 2 slowly changing dimension. Versions are half-open: valid_from <= date < valid_to.
IF OBJECT_ID('dw.dim_site') IS NULL
CREATE TABLE dw.dim_site
(
    site_key   BIGINT         NOT NULL,
    site_id    NVARCHAR(10)   NOT NULL,
    site_name  NVARCHAR(200)  NULL,
    region     NVARCHAR(40)   NULL,
    trust_type NVARCHAR(30)   NULL,
    valid_from DATE           NOT NULL,
    valid_to   DATE           NOT NULL,
    is_current BIT            NOT NULL,
    row_hash   CHAR(64)       NULL,
    CONSTRAINT pk_dim_site PRIMARY KEY CLUSTERED (site_key),
    CONSTRAINT uq_dim_site_version UNIQUE (site_id, valid_from),
    CONSTRAINT ck_dim_site_validity CHECK (valid_from < valid_to)
);
GO

-- Pseudonymous participants. The four sensitive columns are masked for everyone except
-- members of role_data_steward (see sql/security/021_dynamic_data_masking.sql).
IF OBJECT_ID('dw.dim_participant') IS NULL
CREATE TABLE dw.dim_participant
(
    participant_key  BIGINT        NOT NULL,
    participant_pk   CHAR(64)      NOT NULL,                         -- salted SHA-256 pseudonym
    birth_year       SMALLINT      NULL,
    sex              NVARCHAR(10)  NULL,
    ethnicity        NVARCHAR(60)  NULL,
    outward_postcode NVARCHAR(4)   NULL,                             -- outward code only, e.g. MK9
    updated_at       DATETIME2(0)  NULL,
    CONSTRAINT pk_dim_participant PRIMARY KEY CLUSTERED (participant_key)
);
GO
