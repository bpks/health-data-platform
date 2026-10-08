/*
  Merge changed enrolments from staging into dw.fact_enrolment.

  Staging holds only the rows the lake changed in the latest run (the gold layer is incremental).
  A staged row only overwrites the target when it is NEWER (updated_at), so reloading an old extract
  can never roll data back. Dimensions must be loaded first because the foreign keys are enforced.

  At much larger volumes, prefer loading into an empty partition and switching it in, or batching
  this MERGE: a MERGE into a columnstore index is fine for thousands of rows, not hundreds of millions.
*/
CREATE OR ALTER PROCEDURE dw.usp_merge_fact_enrolment
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @load_id BIGINT;
    DECLARE @changes TABLE (change_type NVARCHAR(10) NOT NULL);

    INSERT INTO ops.load_log (procedure_name, started_at, status)
    VALUES (OBJECT_NAME(@@PROCID), SYSUTCDATETIME(), 'running');
    SET @load_id = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRANSACTION;

        MERGE dw.fact_enrolment AS t
        USING stg.fact_enrolment AS s
            ON t.enrolment_key = s.enrolment_key
        WHEN MATCHED AND t.updated_at < s.updated_at
            THEN UPDATE SET
                t.participant_key = s.participant_key,
                t.study_key = s.study_key,
                t.site_key = s.site_key,
                t.site_region = s.site_region,
                t.enrolled_date_key = s.enrolled_date_key,
                t.status = s.status,
                t.age_band_at_enrolment = s.age_band_at_enrolment,
                t.enrolment_count = s.enrolment_count,
                t.updated_at = s.updated_at
        WHEN NOT MATCHED BY TARGET
            THEN INSERT (
                enrolment_key, enrolment_id, participant_key, study_key, site_key, site_region,
                enrolled_date_key, status, age_band_at_enrolment, enrolment_count, updated_at
            )
            VALUES (
                s.enrolment_key, s.enrolment_id, s.participant_key, s.study_key, s.site_key, s.site_region,
                s.enrolled_date_key, s.status, s.age_band_at_enrolment, s.enrolment_count, s.updated_at
            )
        OUTPUT $action INTO @changes (change_type);

        COMMIT TRANSACTION;

        UPDATE ops.load_log
        SET finished_at = SYSUTCDATETIME(),
            status = 'success',
            rows_inserted = (SELECT COUNT(*) FROM @changes WHERE change_type = 'INSERT'),
            rows_updated = (SELECT COUNT(*) FROM @changes WHERE change_type = 'UPDATE')
        WHERE load_id = @load_id;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;

        UPDATE ops.load_log
        SET finished_at = SYSUTCDATETIME(),
            status = 'failed',
            message = LEFT(ERROR_MESSAGE(), 2000)
        WHERE load_id = @load_id;

        THROW;
    END CATCH;
END;
GO
