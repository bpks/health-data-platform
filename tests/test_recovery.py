"""Recovery from partial failure: a run that stops part-way must be fixable by simply running again.

The stages are run as separate invocations (each gets its own run id), which is what happens when a pipeline
fails after bronze or silver and is re-triggered. Gold must still catch up with everything silver holds.
"""

from pathlib import Path

from pyspark.sql import functions as F

from hdp.config import Settings
from hdp.generate import Volumes, generate_day1, generate_day2
from hdp.lake import read_table
from hdp.run import run_pipeline


def _counts(spark, s):
    return {
        "silver_apps": read_table(spark, s, s.silver("applications")).count(),
        "silver_enrols": read_table(spark, s, s.silver("enrolments")).count(),
        "silver_people": read_table(spark, s, s.silver("participants")).count(),
        "fact_apps": read_table(spark, s, s.gold("fact_application")).count(),
        "fact_enrols": read_table(spark, s, s.gold("fact_enrolment")).count(),
        # the dimension holds one extra row: the unknown member
        "dim_people": read_table(spark, s, s.gold("dim_participant")).count() - 1,
    }


def test_gold_catches_up_after_a_run_that_stopped_before_it(spark, tmp_path):
    s = Settings(base_path=str(tmp_path / "lake"), pseudo_salt="recovery-test-salt", env="local")
    landing = Path(s.landing())

    # Day 1: bronze and silver complete in their own runs; gold "never ran".
    generate_day1(landing, "2025-06-01", seed=21, volumes=Volumes.small())
    run_pipeline(spark, s, stage="bronze")
    run_pipeline(spark, s, stage="silver")
    # Recovery is just running again. This is a new run id, and bronze and silver have nothing to do.
    recovery = run_pipeline(spark, s)
    assert all(r["status"] == "skipped" for r in recovery.rows("bronze"))
    c = _counts(spark, s)
    assert c["fact_apps"] == c["silver_apps"] > 0
    assert c["fact_enrols"] == c["silver_enrols"] > 0
    assert c["dim_people"] == c["silver_people"] > 0

    # Day 2: silver finishes, gold is missed again, and the next run still brings gold level.
    m2 = generate_day2(landing, "2025-06-02", seed=22)
    run_pipeline(spark, s, stage="bronze")
    run_pipeline(spark, s, stage="silver")
    before = _counts(spark, s)
    assert before["fact_enrols"] < before["silver_enrols"] or before["fact_apps"] <= before["silver_apps"]
    recovery2 = run_pipeline(spark, s, stage="gold")
    c2 = _counts(spark, s)
    assert c2["fact_apps"] == c2["silver_apps"]
    assert c2["fact_enrols"] == c2["silver_enrols"]
    assert c2["dim_people"] == c2["silver_people"]

    # ...and it only did the work that was missing, not a full rebuild.
    gold_in = recovery2.rows("gold", "fact_enrolment")[0]["rows_in"]
    assert 0 < gold_in < c2["silver_enrols"] / 5

    # Status changes made on day 2 reached the facts.
    facts = {r["application_id"]: r["status"] for r in read_table(spark, s, s.gold("fact_application")).collect()}
    assert m2["progressed_applications"]
    for app_id, status in m2["progressed_applications"].items():
        assert facts[app_id] == status

    # A further run changes nothing.
    again = run_pipeline(spark, s)
    assert again.rows("gold", "fact_application")[0]["rows_in"] == 0
    assert again.rows("gold", "fact_enrolment")[0]["rows_in"] == 0
    assert _counts(spark, s) == c2
    assert read_table(spark, s, s.gold("fact_application")).filter(F.col("application_key").isNull()).count() == 0
