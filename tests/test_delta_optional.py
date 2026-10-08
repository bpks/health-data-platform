"""Delta Lake code path. Skipped unless HDP_TEST_DELTA=1 (needs delta-spark and Delta jars).

The default test suite uses Parquet so it runs anywhere. This file checks the same upsert
semantics against real Delta tables, including MERGE and the stale-record guard.
"""

import os
from datetime import datetime

import pytest

pytestmark = pytest.mark.skipif(os.getenv("HDP_TEST_DELTA") != "1", reason="set HDP_TEST_DELTA=1 to run Delta tests")


@pytest.fixture(scope="module")
def delta_spark():
    from hdp.config import Settings
    from hdp.spark_session import get_spark

    session = get_spark(Settings(table_format="delta"), "hdp-delta-tests")
    yield session
    session.stop()


def _df(spark, rows):
    return spark.createDataFrame(
        [(i, st, datetime.fromisoformat(ts)) for i, st, ts in rows], "id int, status string, updated_at timestamp"
    )


def test_delta_upsert_inserts_updates_and_ignores_stale(delta_spark, tmp_path):
    from hdp.config import Settings
    from hdp.lake import read_table, upsert

    s = Settings(base_path=str(tmp_path / "lake"), table_format="delta")
    path = s.silver("t")
    upsert(
        delta_spark,
        s,
        _df(delta_spark, [(1, "a", "2025-06-01T10:00:00"), (2, "b", "2025-06-01T10:00:00")]),
        path,
        ["id"],
        ["updated_at"],
    )
    upsert(
        delta_spark,
        s,
        _df(
            delta_spark,
            [(1, "stale", "2000-01-01T00:00:00"), (2, "b2", "2025-06-02T10:00:00"), (3, "c", "2025-06-02T10:00:00")],
        ),
        path,
        ["id"],
        ["updated_at"],
    )
    got = {r["id"]: r["status"] for r in read_table(delta_spark, s, path).collect()}
    assert got == {1: "a", 2: "b2", 3: "c"}


def test_delta_pipeline_runs_end_to_end(delta_spark, tmp_path):
    from pathlib import Path

    from hdp.config import Settings
    from hdp.generate import Volumes, generate_day1
    from hdp.lake import read_table
    from hdp.run import run_pipeline

    s = Settings(base_path=str(tmp_path / "lake"), table_format="delta", pseudo_salt="delta-test", env="local")
    generate_day1(Path(s.landing()), "2025-06-01", seed=5, volumes=Volumes.small())
    ctx = run_pipeline(delta_spark, s)
    assert all(r["status"] in {"success", "skipped"} for r in ctx.records)
    assert read_table(delta_spark, s, s.gold("fact_application")).count() > 0
