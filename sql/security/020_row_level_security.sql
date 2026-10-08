/*
  Row-level security: a user sees only the rows for the regions they are assigned.

  How access is decided (security.fn_region_predicate):
    1. members of role_all_regions (data stewards, the ETL identity, ops) see everything
    2. otherwise a user sees a row when security.region_access maps them, or an Entra ID group
       they belong to, to the row's region

  Fact rows carry site_region so the predicate needs no join. The filter predicate is applied by the
  engine to every query, view and BI tool; an analyst cannot opt out by writing a different query.
  Access is granted by inserting into security.region_access (ideally by group, not by person).
*/

IF OBJECT_ID('security.region_access') IS NULL
CREATE TABLE security.region_access
(
    principal_name SYSNAME      NOT NULL,   -- a database user name or an Entra ID group name
    region         NVARCHAR(40) NOT NULL,
    CONSTRAINT pk_region_access PRIMARY KEY CLUSTERED (principal_name, region)
);
GO

-- The policy is schema-bound to the function, so drop it before changing the function.
IF EXISTS (SELECT 1 FROM sys.security_policies WHERE name = 'region_policy')
    DROP SECURITY POLICY security.region_policy;
GO

CREATE OR ALTER FUNCTION security.fn_region_predicate (@site_region AS NVARCHAR(40))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
    SELECT 1 AS is_allowed
    WHERE IS_ROLEMEMBER('role_all_regions') = 1
        OR EXISTS (
            SELECT 1
            FROM security.region_access AS ra
            WHERE ra.region = @site_region
                AND (ra.principal_name = USER_NAME() OR IS_MEMBER(ra.principal_name) = 1)
        );
GO

CREATE SECURITY POLICY security.region_policy
    ADD FILTER PREDICATE security.fn_region_predicate(site_region) ON dw.fact_application,
    ADD FILTER PREDICATE security.fn_region_predicate(site_region) ON dw.fact_enrolment,
    ADD FILTER PREDICATE security.fn_region_predicate(site_region) ON dw.mart_monthly_performance
WITH (STATE = ON, SCHEMABINDING = ON);
GO

-- security.region_access decides who sees what, so only the ETL/admin identity may change it.
DENY INSERT, UPDATE, DELETE ON security.region_access TO role_analyst;
GO
