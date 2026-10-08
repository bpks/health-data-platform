/*
  Dynamic data masking on the participant dimension.

  Everyone except members of role_data_steward (which holds UNMASK) sees masked values.

  Be honest about what this is: masking limits accidental exposure in query results. It is not
  encryption and not an access boundary: a determined user with SELECT can infer masked values by
  filtering (WHERE outward_postcode = 'MK9'). The real controls are the pseudonymisation and
  generalisation done in the lake before the data gets here, plus RBAC and row-level security.
  Masking is a belt-and-braces layer on top of those.
*/

IF NOT EXISTS (SELECT 1 FROM sys.masked_columns WHERE object_id = OBJECT_ID('dw.dim_participant') AND name = 'participant_pk')
    ALTER TABLE dw.dim_participant ALTER COLUMN participant_pk ADD MASKED WITH (FUNCTION = 'partial(4, "XXXXXXXX", 0)');

IF NOT EXISTS (SELECT 1 FROM sys.masked_columns WHERE object_id = OBJECT_ID('dw.dim_participant') AND name = 'birth_year')
    ALTER TABLE dw.dim_participant ALTER COLUMN birth_year ADD MASKED WITH (FUNCTION = 'default()');

IF NOT EXISTS (SELECT 1 FROM sys.masked_columns WHERE object_id = OBJECT_ID('dw.dim_participant') AND name = 'outward_postcode')
    ALTER TABLE dw.dim_participant ALTER COLUMN outward_postcode ADD MASKED WITH (FUNCTION = 'partial(1, "XX", 0)');

IF NOT EXISTS (SELECT 1 FROM sys.masked_columns WHERE object_id = OBJECT_ID('dw.dim_participant') AND name = 'ethnicity')
    ALTER TABLE dw.dim_participant ALTER COLUMN ethnicity ADD MASKED WITH (FUNCTION = 'default()');
GO
