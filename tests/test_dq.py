import json

import pytest
from pyspark.sql import functions as F

from hdp.dq import Rule, apply_rules, duplicate_keys


@pytest.fixture()
def rows(spark):
    return spark.createDataFrame(
        [
            (1, "A", 10, True, "b1"),  # clean
            (2, None, 10, True, "b1"),  # fails name_present (error)
            (3, "C", -5, True, "b1"),  # fails age_positive (error)
            (4, "D", 10, False, "b1"),  # fails flag_set (warning only: still loads)
            (5, None, -1, True, "b2"),  # fails two errors
            (6, "F", None, True, "b2"),  # NULL age counts as a failure
        ],
        "id int, name string, age int, _chk_flag boolean, _batch_date string",
    )


def make_rules():
    # built inside a function: Spark columns need an active Spark session
    return [
        Rule("name_present", F.col("name").isNotNull()),
        Rule("age_positive", F.col("age") > 0),
        Rule("flag_set", F.col("_chk_flag"), severity="warning"),
    ]


def test_error_rows_are_quarantined_and_warning_rows_still_load(rows):
    valid, quarantined, stats = apply_rules(rows, make_rules())
    assert sorted(r["id"] for r in valid.collect()) == [1, 4]
    assert quarantined.count() == 4
    warned = {r["id"]: r["dq_warnings"] for r in valid.collect()}
    assert warned == {1: [], 4: ["flag_set"]}


def test_quarantine_records_which_rules_failed(rows):
    _, quarantined, _ = apply_rules(rows, make_rules())
    failed = {json.loads(r["payload"])["id"]: sorted(r["failed_rules"]) for r in quarantined.collect()}
    assert failed[2] == ["name_present"]
    assert failed[3] == ["age_positive"]
    assert failed[5] == ["age_positive", "name_present"]
    assert failed[6] == ["age_positive"]  # NULL fails


def test_stats_count_failures_per_rule(rows):
    _, _, stats = apply_rules(rows, make_rules())
    assert stats["_total"] == 6
    assert stats["_quarantined"] == 4
    assert stats["name_present"] == 2
    assert stats["age_positive"] == 3
    assert stats["flag_set"] == 1


def test_helper_and_audit_columns_stay_out_of_the_loaded_and_quarantined_data(rows):
    valid, quarantined, _ = apply_rules(rows, make_rules())
    assert "_chk_flag" not in valid.columns and "_dq_errors" not in valid.columns
    payload = json.loads(quarantined.first()["payload"])
    assert set(payload) == {"id", "name", "age"}  # no audit (_batch_date) or helper (_chk_) columns
    assert "_batch_date" in quarantined.columns  # but the audit column travels alongside


def test_no_rules_means_everything_loads(rows):
    valid, quarantined, stats = apply_rules(rows, [])
    assert valid.count() == 6 and quarantined.count() == 0 and stats["_quarantined"] == 0


def test_invalid_severity_is_rejected():
    with pytest.raises(ValueError):
        Rule("x", F.lit(True), severity="fatal")


def test_duplicate_keys(spark):
    df = spark.createDataFrame([(1,), (1,), (2,), (3,), (3,), (3,)], "k int")
    assert duplicate_keys(df, ["k"]) == 2
    assert duplicate_keys(df.dropDuplicates(["k"]), ["k"]) == 0
