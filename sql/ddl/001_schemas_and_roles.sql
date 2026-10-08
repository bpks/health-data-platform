/*
  Serving layer for the health data platform.
  Target: SQL Server 2019+ or Azure SQL Database.

  Schemas
    stg       landing area for copies of the gold tables from the lake (loaded by the pipeline)
    dw        the star schema that analysts query
    ops       load logging and operational objects
    security  row-level security objects

  Roles (least privilege; membership is managed through Entra ID groups, never individual grants)
    role_analyst        read the dw schema: rows filtered by region, sensitive columns masked
    role_data_steward   as analyst, plus UNMASK, for the people who investigate data issues
    role_all_regions    marker role: members see every region (data stewards, ETL, ops)
    role_etl            the load service principal: write dw, read stg, run the load procedures
    role_ops_readonly   performance and capacity monitoring, no access to data
*/

IF SCHEMA_ID('stg') IS NULL EXEC ('CREATE SCHEMA stg AUTHORIZATION dbo');
GO
IF SCHEMA_ID('dw') IS NULL EXEC ('CREATE SCHEMA dw AUTHORIZATION dbo');
GO
IF SCHEMA_ID('ops') IS NULL EXEC ('CREATE SCHEMA ops AUTHORIZATION dbo');
GO
IF SCHEMA_ID('security') IS NULL EXEC ('CREATE SCHEMA security AUTHORIZATION dbo');
GO

IF DATABASE_PRINCIPAL_ID('role_analyst') IS NULL CREATE ROLE role_analyst;
IF DATABASE_PRINCIPAL_ID('role_data_steward') IS NULL CREATE ROLE role_data_steward;
IF DATABASE_PRINCIPAL_ID('role_all_regions') IS NULL CREATE ROLE role_all_regions;
IF DATABASE_PRINCIPAL_ID('role_etl') IS NULL CREATE ROLE role_etl;
IF DATABASE_PRINCIPAL_ID('role_ops_readonly') IS NULL CREATE ROLE role_ops_readonly;
GO

-- Analysts: the star schema only. Never the staging copies, which are not masked.
GRANT SELECT ON SCHEMA::dw TO role_analyst;
DENY SELECT ON SCHEMA::stg TO role_analyst;

-- Stewards inherit analyst access and may see unmasked values (rows are still region-filtered
-- unless they are also in role_all_regions).
ALTER ROLE role_analyst ADD MEMBER role_data_steward;
GRANT UNMASK TO role_data_steward;
ALTER ROLE role_all_regions ADD MEMBER role_data_steward;

-- ETL: write the star schema, read staging, execute the load procedures, see all regions.
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::dw TO role_etl;
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::stg TO role_etl;
GRANT SELECT, INSERT ON SCHEMA::ops TO role_etl;
GRANT EXECUTE ON SCHEMA::dw TO role_etl;
GRANT EXECUTE ON SCHEMA::ops TO role_etl;
ALTER ROLE role_all_regions ADD MEMBER role_etl;

-- Operations: performance state only (DMVs, Query Store). No SELECT on any schema.
GRANT VIEW DATABASE STATE TO role_ops_readonly;
ALTER ROLE role_all_regions ADD MEMBER role_ops_readonly;
GO
