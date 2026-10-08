/*
  Prove the security controls work, rather than assume they do. Run in a NON-PRODUCTION copy after
  deploying sql/security/. Creates throw-away users without logins, impersonates them, and reverts.

  Expectations
    test_analyst_london      sees only London rows; participant columns are masked
    test_analyst_nowhere     has no region assignment: sees no fact rows at all
    test_steward             sees all regions and the unmasked values
*/
SET NOCOUNT ON;

IF DATABASE_PRINCIPAL_ID('test_analyst_london') IS NULL CREATE USER test_analyst_london WITHOUT LOGIN;
IF DATABASE_PRINCIPAL_ID('test_analyst_nowhere') IS NULL CREATE USER test_analyst_nowhere WITHOUT LOGIN;
IF DATABASE_PRINCIPAL_ID('test_steward') IS NULL CREATE USER test_steward WITHOUT LOGIN;

ALTER ROLE role_analyst ADD MEMBER test_analyst_london;
ALTER ROLE role_analyst ADD MEMBER test_analyst_nowhere;
ALTER ROLE role_data_steward ADD MEMBER test_steward;

DELETE FROM security.region_access WHERE principal_name IN ('test_analyst_london', 'test_analyst_nowhere');
INSERT INTO security.region_access (principal_name, region) VALUES ('test_analyst_london', 'London');

DECLARE @total_rows INT = (SELECT COUNT(*) FROM dw.fact_enrolment);
DECLARE @london_rows INT = (SELECT COUNT(*) FROM dw.fact_enrolment WHERE site_region = 'London');
DECLARE @seen INT;
DECLARE @other_regions INT;
DECLARE @masked_postcode NVARCHAR(10);
DECLARE @steward_postcode NVARCHAR(10);

-- Row-level security
EXECUTE AS USER = 'test_analyst_london';
SELECT @seen = COUNT(*), @other_regions = SUM(CASE WHEN site_region <> 'London' THEN 1 ELSE 0 END) FROM dw.fact_enrolment;
SELECT TOP (1) @masked_postcode = outward_postcode FROM dw.dim_participant WHERE participant_key <> -1;
REVERT;

IF @seen <> @london_rows OR ISNULL(@other_regions, 0) <> 0
    THROW 50201, 'RLS failure: the London analyst did not see exactly the London rows.', 1;

EXECUTE AS USER = 'test_analyst_nowhere';
SELECT @seen = COUNT(*) FROM dw.fact_enrolment;
REVERT;

IF @seen <> 0
    THROW 50202, 'RLS failure: an analyst with no region assignment can see fact rows.', 1;

-- Dynamic data masking: analyst sees "<first char>XX", steward sees the real outward code
IF @masked_postcode IS NOT NULL AND @masked_postcode NOT LIKE '_XX'
    THROW 50203, 'Masking failure: an analyst can see an unmasked outward postcode.', 1;

EXECUTE AS USER = 'test_steward';
SELECT @seen = COUNT(*) FROM dw.fact_enrolment;
SELECT TOP (1) @steward_postcode = outward_postcode FROM dw.dim_participant WHERE participant_key <> -1;
REVERT;

IF @seen <> @total_rows
    THROW 50204, 'RLS failure: the data steward does not see every row.', 1;
IF @steward_postcode IS NOT NULL AND @steward_postcode LIKE '_XX'
    THROW 50205, 'Masking failure: the data steward is seeing masked values despite holding UNMASK.', 1;

-- Clean up
DELETE FROM security.region_access WHERE principal_name IN ('test_analyst_london', 'test_analyst_nowhere');
ALTER ROLE role_analyst DROP MEMBER test_analyst_london;
ALTER ROLE role_analyst DROP MEMBER test_analyst_nowhere;
ALTER ROLE role_data_steward DROP MEMBER test_steward;
DROP USER test_analyst_london;
DROP USER test_analyst_nowhere;
DROP USER test_steward;

PRINT 'RLS and masking checks passed.';
