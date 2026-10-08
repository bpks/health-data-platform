"""Table I/O helpers: read, write, upsert, plus the small control tables.

Delta is the intended table format (Fabric and Databricks both default to it).
A plain-Parquet fallback exists so the whole pipeline can be run and tested on a
laptop without Delta jars. The Parquet fallback only works on a local filesystem.
"""

from __future__ import annotations

import os
import shutil

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from hdp.config import Settings

WATERMARK_SCHEMA = "entity string, watermark timestamp, updated_at timestamp"
INGESTED_SCHEMA_COLUMNS = ["entity", "batch_date", "run_id", "ingested_at"]


def table_exists(spark: SparkSession, s: Settings, path: str) -> bool:
    try:
        spark.read.format(s.table_format).load(path).schema  # noqa: B018 - schema access forces path resolution
        return True
    except AnalysisException:
        return False


def read_table(spark: SparkSession, s: Settings, path: str) -> DataFrame:
    return spark.read.format(s.table_format).load(path)


def write_table(
    df: DataFrame, s: Settings, path: str, mode: str = "append", partition_by: list[str] | None = None
) -> None:
    writer = df.write.format(s.table_format).mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def _require_local(s: Settings) -> None:
    if not s.is_local:
        raise NotImplementedError(
            "HDP_FORMAT=parquet is a local-development fallback (it swaps directories on a local "
            "filesystem). Use HDP_FORMAT=delta for cloud storage."
        )


def replace_table(
    spark: SparkSession, s: Settings, df: DataFrame, path: str, partition_by: list[str] | None = None
) -> None:
    """Overwrite a table with `df`, even when `df` was derived by reading that same table."""
    if s.table_format == "delta":
        write_table(df, s, path, "overwrite", partition_by)  # Delta overwrite is transactional
        return
    _require_local(s)
    tmp = path.rstrip("/") + "__tmp"
    shutil.rmtree(tmp, ignore_errors=True)
    write_table(df, s, tmp, "overwrite", partition_by)  # fully materialised before we touch `path`
    shutil.rmtree(path, ignore_errors=True)
    os.rename(tmp, path)


def latest_per_key(df: DataFrame, keys: list[str], order_cols: list[str]) -> DataFrame:
    """Keep one row per key: the one that sorts first by `order_cols` descending."""
    window = Window.partitionBy(*keys).orderBy(*[F.col(c).desc_nulls_last() for c in order_cols])
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


def upsert(
    spark: SparkSession, s: Settings, new_df: DataFrame, path: str, keys: list[str], order_cols: list[str]
) -> None:
    """Insert-or-update by key. The row with the latest `order_cols` wins, so late or
    out-of-order records never overwrite newer data and re-running a batch is harmless."""
    deduped = latest_per_key(new_df, keys, order_cols)
    if not table_exists(spark, s, path):
        write_table(deduped, s, path, "overwrite")
        return

    if s.table_format == "delta":
        from delta.tables import DeltaTable

        condition = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
        (
            DeltaTable.forPath(spark, path)
            .alias("t")
            .merge(deduped.alias("s"), condition)
            .whenMatchedUpdateAll(condition=f"s.`{order_cols[0]}` >= t.`{order_cols[0]}`")
            .whenNotMatchedInsertAll()
            .execute()
        )
        return

    existing = read_table(spark, s, path)
    combined = existing.unionByName(deduped, allowMissingColumns=True)
    replace_table(spark, s, latest_per_key(combined, keys, order_cols), path)


# --- control tables: watermarks and ingested batches -------------------------------------------------


def apply_watermark(
    spark: SparkSession, s: Settings, df: DataFrame, entity: str, ts_col: str = "_ingested_at"
) -> DataFrame:
    """Keep only rows newer than the last processed watermark for `entity`.

    Done in Spark SQL on purpose: pulling the watermark into Python would round-trip a
    timestamp through the driver's local timezone."""
    path = s.control("watermarks")
    wm = (
        read_table(spark, s, path).filter(F.col("entity") == entity)
        if table_exists(spark, s, path)
        else spark.createDataFrame([], WATERMARK_SCHEMA)
    )
    view = f"_wm_{entity}"
    wm.createOrReplaceTempView(view)
    return df.filter(
        F.expr(f"{ts_col} > coalesce((SELECT max(watermark) FROM {view}), TIMESTAMP '1970-01-01 00:00:00')")
    )


def set_watermark(
    spark: SparkSession, s: Settings, entity: str, processed: DataFrame, ts_col: str = "_ingested_at"
) -> None:
    wm = (
        processed.agg(F.max(ts_col).alias("watermark"))
        .withColumn("entity", F.lit(entity))
        .withColumn("updated_at", F.current_timestamp())
        .select("entity", "watermark", "updated_at")
    )
    upsert(spark, s, wm, s.control("watermarks"), ["entity"], ["updated_at"])


def ingested_batches(spark: SparkSession, s: Settings, entity: str) -> set[str]:
    path = s.control("ingested_batches")
    if not table_exists(spark, s, path):
        return set()
    rows = read_table(spark, s, path).filter(F.col("entity") == entity).select("batch_date").collect()
    return {r["batch_date"] for r in rows}


def record_batches(spark: SparkSession, s: Settings, entity: str, batch_dates: list[str], run_id: str) -> None:
    rows = spark.createDataFrame(
        [(entity, b, run_id) for b in batch_dates], "entity string, batch_date string, run_id string"
    )
    rows = rows.withColumn("ingested_at", F.current_timestamp()).select(*INGESTED_SCHEMA_COLUMNS)
    upsert(spark, s, rows, s.control("ingested_batches"), ["entity", "batch_date"], ["ingested_at"])
