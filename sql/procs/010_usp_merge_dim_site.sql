/*
  Merge the gold dim_site (type 2 history, built in the lake) from staging into dw.dim_site.

  The SCD2 logic lives in the Spark gold layer (src/hdp/gold.py: scd2_merge). Keys are
  deterministic hashes, so this procedure is a plain keyed upsert: closed versions arrive as
  updates (valid_to, is_current), new versions arrive as inserts. Nothing is ever deleted:
  history is the point.

  Every run writes one row to ops.load_log, success or failure. Failures are re-thrown so the
  calling pipeline run fails and the alert in monitoring/ fires.
*/
CREATE OR ALTER PROCEDURE dw.usp_merge_dim_site
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

        MERGE dw.dim_site AS t
        USING stg.dim_site AS s
            ON t.site_key = s.site_key
        WHEN MATCHED AND (
                t.valid_to <> s.valid_to
                OR t.is_current <> s.is_current
                OR ISNULL(t.row_hash, '') <> ISNULL(s.row_hash, '')
            )
            THEN UPDATE SET
                t.site_name = s.site_name,
                t.region = s.region,
                t.trust_type = s.trust_type,
                t.valid_from = s.valid_from,
                t.valid_to = s.valid_to,
                t.is_current = s.is_current,
                t.row_hash = s.row_hash
        WHEN NOT MATCHED BY TARGET
            THEN INSERT (site_key, site_id, site_name, region, trust_type, valid_from, valid_to, is_current, row_hash)
                VALUES (s.site_key, s.site_id, s.site_name, s.region, s.trust_type, s.valid_from, s.valid_to, s.is_current, s.row_hash)
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
