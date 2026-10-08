"""Structured run logging.

Every stage writes one JSON log line per entity (what an Azure Monitor / Log Analytics
agent would ingest) and the same records are appended to a `_control/run_log` table so
freshness, volume and quarantine-rate checks can be queried later.
See monitoring/kql/ for the matching alert queries.
"""

from __future__ import annotations

import json
import logging
import sys
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType

from hdp.config import Settings
from hdp.lake import write_table

LOG = logging.getLogger("hdp")

RUN_LOG_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("run_ts", TimestampType()),
        StructField("stage", StringType()),
        StructField("entity", StringType()),
        StructField("status", StringType()),
        StructField("rows_in", LongType()),
        StructField("rows_out", LongType()),
        StructField("rows_quarantined", LongType()),
        StructField("duration_s", DoubleType()),
        StructField("message", StringType()),
    ]
)


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout, force=True)


@dataclass
class RunContext:
    run_id: str
    run_ts: datetime
    records: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def new(cls) -> RunContext:
        return cls(run_id=uuid.uuid4().hex[:12], run_ts=datetime.now(timezone.utc))

    def record(
        self,
        stage: str,
        entity: str,
        status: str,
        rows_in: int | None = None,
        rows_out: int | None = None,
        rows_quarantined: int | None = None,
        duration_s: float | None = None,
        message: str = "",
    ) -> None:
        rec = {
            "run_id": self.run_id,
            "run_ts": self.run_ts,
            "stage": stage,
            "entity": entity,
            "status": status,
            "rows_in": rows_in,
            "rows_out": rows_out,
            "rows_quarantined": rows_quarantined,
            "duration_s": None if duration_s is None else round(duration_s, 2),
            "message": message,
        }
        self.records.append(rec)
        LOG.info(json.dumps(rec, default=str))

    @contextmanager
    def stage(self, stage: str, entity: str) -> Iterator[dict[str, Any]]:
        """Time a unit of work. The caller fills the yielded dict with rows_in/rows_out/..."""
        started = time.perf_counter()
        info: dict[str, Any] = {}
        try:
            yield info
        except Exception as exc:
            self.record(stage, entity, "failed", duration_s=time.perf_counter() - started, message=str(exc)[:500])
            raise
        else:
            self.record(
                stage,
                entity,
                info.pop("status", "success"),
                rows_in=info.get("rows_in"),
                rows_out=info.get("rows_out"),
                rows_quarantined=info.get("rows_quarantined"),
                duration_s=time.perf_counter() - started,
                message=info.get("message", ""),
            )

    def rows(self, stage: str, entity: str | None = None) -> list[dict[str, Any]]:
        return [r for r in self.records if r["stage"] == stage and (entity is None or r["entity"] == entity)]


def flush_run_log(spark: SparkSession, s: Settings, ctx: RunContext) -> None:
    if not ctx.records:
        return
    df = spark.createDataFrame(
        [tuple(r[f.name] for f in RUN_LOG_SCHEMA.fields) for r in ctx.records], schema=RUN_LOG_SCHEMA
    )
    write_table(df, s, s.control("run_log"), "append")
