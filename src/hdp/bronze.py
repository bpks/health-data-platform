"""Bronze: land the raw files, unchanged, with audit columns.

* Everything is read as strings (schema-on-read), so a bad value can never fail the load.
* Bronze is append-only and is the only layer that holds raw identifiers, so it is the
  restricted zone: access is limited to the platform service principal and data engineers.
* Batches already ingested are skipped, so re-running a day is safe. Delivery into bronze
  is at-least-once and everything downstream is idempotent (see silver.py).
"""

from __future__ import annotations

import logging

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from hdp.config import Settings
from hdp.lake import ingested_batches, record_batches, write_table
from hdp.model import ENTITIES, RAW_COLUMNS
from hdp.observability import RunContext

LOG = logging.getLogger("hdp")


def _raw_schema(entity: str) -> StructType:
    # `batch_date` is the Hive-style partition column of the landing folders; including it in
    # the schema keeps it a string instead of letting Spark infer a date.
    return StructType(
        [StructField(c, StringType()) for c in RAW_COLUMNS[entity]] + [StructField("batch_date", StringType())]
    )


def read_landing(spark: SparkSession, s: Settings, entity: str) -> DataFrame | None:
    """All landing batches of an entity, or None if no file exists yet."""
    try:
        return (
            spark.read.schema(_raw_schema(entity))
            .option("header", True)
            .option("basePath", s.landing())
            .csv(f"{s.landing()}/batch_date=*/{entity}.csv")
        )
    except AnalysisException:
        return None


def run(spark: SparkSession, s: Settings, ctx: RunContext) -> None:
    for entity in ENTITIES:
        with ctx.stage("bronze", entity) as info:
            landing = read_landing(spark, s, entity)
            if landing is None:
                info.update(status="skipped", message="no landing files", rows_in=0, rows_out=0)
                continue

            seen = ingested_batches(spark, s, entity)
            available = sorted(r["batch_date"] for r in landing.select("batch_date").distinct().collect())
            new_batches = [b for b in available if b not in seen]
            if not new_batches:
                info.update(status="skipped", message="all batches already ingested", rows_in=0, rows_out=0)
                continue

            raw = landing.filter(F.col("batch_date").isin(new_batches))
            bronze = raw.select(
                *[F.col(c) for c in RAW_COLUMNS[entity]],
                F.col("batch_date").alias("_batch_date"),
                F.lit(ctx.run_id).alias("_run_id"),
                F.input_file_name().alias("_source_file"),
                F.lit(ctx.run_ts).cast("timestamp").alias("_ingested_at"),
            )
            bronze.persist()
            try:
                rows = bronze.count()
                write_table(bronze, s, s.bronze(entity), "append", partition_by=["_batch_date"])
            finally:
                bronze.unpersist()
            record_batches(spark, s, entity, new_batches, ctx.run_id)
            info.update(rows_in=rows, rows_out=rows, message=f"batches: {', '.join(new_batches)}")
