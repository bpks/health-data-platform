from datetime import datetime, timezone

from pyspark.sql import functions as F

from hdp.lake import (
    apply_watermark,
    ingested_batches,
    latest_per_key,
    read_table,
    record_batches,
    replace_table,
    set_watermark,
    table_exists,
    upsert,
    write_table,
)

SCHEMA = "id int, status string, updated_at timestamp"


def _df(spark, rows):
    return spark.createDataFrame([(i, st, datetime.fromisoformat(ts)) for i, st, ts in rows], SCHEMA)


def _state(spark, s, path):
    return {r["id"]: r["status"] for r in read_table(spark, s, path).collect()}


def test_latest_per_key_keeps_newest(spark):
    df = _df(
        spark,
        [(1, "old", "2025-01-01T00:00:00"), (1, "new", "2025-01-02T00:00:00"), (2, "only", "2025-01-01T00:00:00")],
    )
    got = {r["id"]: r["status"] for r in latest_per_key(df, ["id"], ["updated_at"]).collect()}
    assert got == {1: "new", 2: "only"}


def test_upsert_inserts_then_updates(spark, settings):
    path = settings.silver("t")
    upsert(
        spark,
        settings,
        _df(spark, [(1, "a", "2025-01-01T00:00:00"), (2, "b", "2025-01-01T00:00:00")]),
        path,
        ["id"],
        ["updated_at"],
    )
    assert not table_exists(spark, settings, settings.silver("missing"))
    upsert(
        spark,
        settings,
        _df(spark, [(2, "b2", "2025-01-02T00:00:00"), (3, "c", "2025-01-02T00:00:00")]),
        path,
        ["id"],
        ["updated_at"],
    )
    assert _state(spark, settings, path) == {1: "a", 2: "b2", 3: "c"}


def test_upsert_ignores_stale_records(spark, settings):
    path = settings.silver("t")
    upsert(spark, settings, _df(spark, [(1, "current", "2025-06-01T12:00:00")]), path, ["id"], ["updated_at"])
    upsert(spark, settings, _df(spark, [(1, "stale", "2000-01-01T00:00:00")]), path, ["id"], ["updated_at"])
    assert _state(spark, settings, path) == {1: "current"}


def test_upsert_is_idempotent(spark, settings):
    path = settings.silver("t")
    batch = _df(spark, [(1, "a", "2025-01-01T00:00:00"), (2, "b", "2025-01-01T00:00:00")])
    for _ in range(3):
        upsert(spark, settings, batch, path, ["id"], ["updated_at"])
    assert read_table(spark, settings, path).count() == 2


def test_upsert_dedupes_within_the_incoming_batch(spark, settings):
    path = settings.silver("t")
    batch = _df(spark, [(1, "first", "2025-01-01T00:00:00"), (1, "second", "2025-01-01T09:00:00")])
    upsert(spark, settings, batch, path, ["id"], ["updated_at"])
    assert _state(spark, settings, path) == {1: "second"}


def test_replace_table_can_overwrite_a_table_derived_from_itself(spark, settings):
    path = settings.gold("t")
    write_table(_df(spark, [(1, "a", "2025-01-01T00:00:00")]), settings, path, "overwrite")
    derived = read_table(spark, settings, path).withColumn("status", F.upper("status"))
    replace_table(spark, settings, derived, path)
    assert _state(spark, settings, path) == {1: "A"}


def test_watermark_filters_already_processed_rows(spark, settings):
    ts = lambda s: datetime.fromisoformat(s).replace(tzinfo=timezone.utc)  # noqa: E731
    df = spark.createDataFrame(
        [(1, ts("2025-01-01T00:00:00")), (2, ts("2025-01-02T00:00:00"))], "id int, _ingested_at timestamp"
    )
    # nothing recorded yet: everything passes
    assert apply_watermark(spark, settings, df, "things").count() == 2
    set_watermark(spark, settings, "things", df.filter("id = 1"))
    assert [r["id"] for r in apply_watermark(spark, settings, df, "things").collect()] == [2]
    # watermarks are per entity
    assert apply_watermark(spark, settings, df, "other").count() == 2


def test_ingested_batches_are_remembered(spark, settings):
    assert ingested_batches(spark, settings, "applications") == set()
    record_batches(spark, settings, "applications", ["2025-06-01"], "run1")
    record_batches(spark, settings, "applications", ["2025-06-01", "2025-06-02"], "run2")
    assert ingested_batches(spark, settings, "applications") == {"2025-06-01", "2025-06-02"}
    assert ingested_batches(spark, settings, "studies") == set()
