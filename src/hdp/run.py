"""Pipeline entry point: `hdp-run --stage all` (or bronze | silver | gold)."""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import SparkSession

from hdp import bronze, gold, silver
from hdp.config import Settings
from hdp.observability import RunContext, configure_logging, flush_run_log
from hdp.spark_session import get_spark

LOG = logging.getLogger("hdp")


def run_pipeline(spark: SparkSession, s: Settings, stage: str = "all", full_refresh: bool = False) -> RunContext:
    """Run one or more stages and always flush the run log, even when a stage fails."""
    ctx = RunContext.new()
    try:
        if stage in ("all", "bronze"):
            bronze.run(spark, s, ctx)
        if stage in ("all", "silver"):
            silver.run(spark, s, ctx, full_refresh=full_refresh)
        if stage in ("all", "gold"):
            gold.run(spark, s, ctx, full_refresh=full_refresh)
    finally:
        flush_run_log(spark, s, ctx)
    return ctx


def summarise(ctx: RunContext) -> str:
    lines = [
        f"run {ctx.run_id}",
        f"{'stage':<8}{'entity':<26}{'status':<9}{'in':>9}{'out':>9}{'quarantined':>13}{'secs':>7}",
    ]
    for r in ctx.records:

        def n(v: int | float | None) -> str:
            return "-" if v is None else str(v)

        lines.append(
            f"{r['stage']:<8}{r['entity']:<26}{r['status']:<9}{n(r['rows_in']):>9}{n(r['rows_out']):>9}"
            f"{n(r['rows_quarantined']):>13}{n(r['duration_s']):>7}"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the bronze -> silver -> gold pipeline.")
    parser.add_argument("--stage", choices=["all", "bronze", "silver", "gold"], default="all")
    parser.add_argument("--full-refresh", action="store_true", help="ignore watermarks and rebuild from all of bronze")
    parser.add_argument("--base-path", default=None, help="lake root (default: $HDP_BASE_PATH or data/lake)")
    parser.add_argument("--format", default=None, choices=["parquet", "delta"], dest="table_format")
    args = parser.parse_args(argv)

    configure_logging()
    settings = Settings.from_env(base_path=args.base_path, table_format=args.table_format)
    spark = get_spark(settings)
    try:
        ctx = run_pipeline(spark, settings, args.stage, args.full_refresh)
        print("\n" + summarise(ctx))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
