from datetime import date

import pytest

from hdp.gold import _as_of_site, scd2_merge
from hdp.model import BEGINNING_OF_TIME, OPEN_ENDED, UNKNOWN_KEY

COLS = "site_id string, site_name string, region string, trust_type string"


@pytest.fixture()
def first_load(spark):
    snapshot = spark.createDataFrame(
        [
            ("SITE001", "Ashford Health Partnership", "London", "Acute"),
            ("SITE002", "Brookvale Health Partnership", "North East", "Community"),
        ],
        COLS,
    )
    return scd2_merge(spark, None, snapshot, "2025-06-01")


def _rows(df):
    return sorted((r.asDict() for r in df.collect()), key=lambda r: (r["site_id"], r["valid_from"]))


def test_first_load_opens_every_site_from_the_beginning_of_time_plus_unknown_member(first_load):
    rows = _rows(first_load)
    by_id = {r["site_id"]: r for r in rows}
    assert set(by_id) == {"SITE001", "SITE002", "UNKNOWN"}
    assert by_id["SITE001"]["valid_from"] == date.fromisoformat(BEGINNING_OF_TIME)
    assert by_id["SITE001"]["valid_to"] == date.fromisoformat(OPEN_ENDED)
    assert all(r["is_current"] for r in rows)
    assert by_id["UNKNOWN"]["site_key"] == UNKNOWN_KEY


def test_a_changed_site_gets_a_new_version_and_the_old_one_is_closed(spark, first_load):
    renamed = spark.createDataFrame(
        [
            ("SITE001", "Ashford Integrated Care Partnership", "London", "Acute"),
            ("SITE002", "Brookvale Health Partnership", "North East", "Community"),
        ],
        COLS,
    )
    merged = scd2_merge(spark, first_load, renamed, "2025-06-02")
    site1 = [r for r in _rows(merged) if r["site_id"] == "SITE001"]
    assert len(site1) == 2
    old, new = site1
    assert old["site_name"] == "Ashford Health Partnership" and old["is_current"] is False
    assert old["valid_to"] == date(2025, 6, 2)  # closed on the effective date (half-open interval)
    assert new["site_name"] == "Ashford Integrated Care Partnership" and new["is_current"] is True
    assert new["valid_from"] == date(2025, 6, 2) and new["valid_to"] == date.fromisoformat(OPEN_ENDED)
    assert old["site_key"] != new["site_key"]


def test_an_unchanged_site_is_left_alone(spark, first_load):
    renamed = spark.createDataFrame(
        [
            ("SITE001", "Renamed", "London", "Acute"),
            ("SITE002", "Brookvale Health Partnership", "North East", "Community"),
        ],
        COLS,
    )
    before = {r["site_id"]: r["site_key"] for r in first_load.collect()}
    merged = scd2_merge(spark, first_load, renamed, "2025-06-02")
    site2 = [r for r in _rows(merged) if r["site_id"] == "SITE002"]
    assert len(site2) == 1 and site2[0]["site_key"] == before["SITE002"] and site2[0]["is_current"]


def test_a_brand_new_site_starts_at_the_beginning_of_time(spark, first_load):
    snapshot = spark.createDataFrame(
        [
            ("SITE001", "Ashford Health Partnership", "London", "Acute"),
            ("SITE002", "Brookvale Health Partnership", "North East", "Community"),
            ("SITE003", "Cedarton Health Partnership", "South West", "Specialist"),
        ],
        COLS,
    )
    merged = scd2_merge(spark, first_load, snapshot, "2025-06-02")
    site3 = [r for r in _rows(merged) if r["site_id"] == "SITE003"]
    assert len(site3) == 1 and site3[0]["valid_from"] == date.fromisoformat(BEGINNING_OF_TIME)


def test_rerunning_the_same_snapshot_changes_nothing(spark, first_load):
    snapshot = spark.createDataFrame(
        [
            ("SITE001", "Ashford Health Partnership", "London", "Acute"),
            ("SITE002", "Brookvale Health Partnership", "North East", "Community"),
        ],
        COLS,
    )
    again = scd2_merge(spark, first_load, snapshot, "2025-06-09")
    assert again.count() == first_load.count()
    assert all(r["is_current"] for r in again.collect())


def test_facts_resolve_to_the_site_version_current_on_their_date(spark, first_load):
    renamed = spark.createDataFrame(
        [
            ("SITE001", "Ashford Integrated Care Partnership", "London", "Acute"),
            ("SITE002", "Brookvale Health Partnership", "North East", "Community"),
        ],
        COLS,
    )
    dim = scd2_merge(spark, first_load, renamed, "2025-06-02")
    facts = spark.createDataFrame(
        [("a", "SITE001", date(2025, 6, 1)), ("b", "SITE001", date(2025, 6, 2)), ("c", "SITE001", date(2024, 1, 1))],
        "id string, site_id string, d date",
    )
    resolved = {r["id"]: r["site_key"] for r in _as_of_site(facts, "site_id", "d", dim).collect()}
    keys = {(r["valid_from"], r["is_current"]): r["site_key"] for r in dim.filter("site_id = 'SITE001'").collect()}
    old_key = keys[(date.fromisoformat(BEGINNING_OF_TIME), False)]
    new_key = keys[(date(2025, 6, 2), True)]
    assert resolved == {"a": old_key, "b": new_key, "c": old_key}
