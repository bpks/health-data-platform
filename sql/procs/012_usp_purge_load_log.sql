/*
  Housekeeping: keep ops.load_log to a rolling window. Deletes in small batches so the transaction
  log and lock footprint stay small, and so a long purge never blocks a nightly load for long.
*/
CREATE OR ALTER PROCEDURE ops.usp_purge_load_log
    @retain_days INT = 180
AS
BEGIN
    SET NOCOUNT ON;

    IF @retain_days < 30
        THROW 50001, 'Refusing to keep less than 30 days of load history.', 1;

    DECLARE @cutoff DATETIME2(0) = DATEADD(DAY, -@retain_days, SYSUTCDATETIME());
    DECLARE @deleted INT = 1;

    WHILE @deleted > 0
    BEGIN
        DELETE TOP (5000) FROM ops.load_log
        WHERE started_at < @cutoff;

        SET @deleted = @@ROWCOUNT;
    END;
END;
GO
