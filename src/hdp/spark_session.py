"""Spark session factory with settings that behave the same on Spark 3.5 and 4.x."""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

from hdp.config import Settings


def get_spark(settings: Settings, app_name: str = "hdp") -> SparkSession:
    # Local mode: make the Python workers use the same interpreter as the driver,
    # otherwise Spark picks `python3` from PATH and can fail with a version mismatch.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    builder = SparkSession.builder.appName(app_name)
    if settings.is_local:
        builder = (
            builder.master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            # Local mode only: advertise localhost so executors fetch large broadcast blocks from an
            # address that exists. Without this, a machine whose hostname resolves to an unreachable
            # interface fails once a broadcast outgrows the inline limit (a join against ~60k keys).
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
        )

    if settings.table_format == "delta":
        try:
            from delta import configure_spark_with_delta_pip

            builder = configure_spark_with_delta_pip(builder)
        except ImportError:
            # Fabric and Databricks sessions already have Delta registered.
            pass
        builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )

    spark = builder.getOrCreate()
    # Runtime settings (safe to set on an existing Fabric/Databricks session):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    # Spark 4 turns ANSI mode on by default. The cleaning code relies on
    # "unparseable value -> NULL, then a data quality rule catches it", so pin the
    # behaviour instead of depending on the Spark version.
    spark.conf.set("spark.sql.ansi.enabled", "false")
    spark.sparkContext.setLogLevel("ERROR")
    return spark
