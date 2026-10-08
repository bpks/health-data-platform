import re

from pyspark.sql import functions as F

from hdp import measures
from hdp.privacy import outward_postcode, pseudonymise


def _one(spark, expr, value):
    df = spark.createDataFrame([(value,)], "v string")
    return df.select(expr(F.col("v")).alias("out")).first()["out"]


def test_pseudonym_is_deterministic_and_normalises_whitespace_and_case(spark):
    a = _one(spark, lambda c: pseudonymise(c, "salt"), "1234567890")
    b = _one(spark, lambda c: pseudonymise(c, "salt"), " 1234567890 ")
    assert a == b
    assert re.fullmatch(r"[0-9a-f]{64}", a)
    assert "1234567890" not in a


def test_pseudonym_depends_on_the_secret_salt(spark):
    a = _one(spark, lambda c: pseudonymise(c, "salt-one"), "1234567890")
    b = _one(spark, lambda c: pseudonymise(c, "salt-two"), "1234567890")
    assert a != b


def test_different_identifiers_get_different_pseudonyms(spark):
    a = _one(spark, lambda c: pseudonymise(c, "salt"), "1234567890")
    b = _one(spark, lambda c: pseudonymise(c, "salt"), "1234567891")
    assert a != b


def test_outward_postcode_generalises_well_formed_postcodes(spark):
    cases = {
        "MK9 3AB": "MK9",
        "mk93ab": "MK9",
        " SW1A 1AA ": "SW1A",
        "M1 1AE": "M1",
        "LS6 2UE": "LS6",
        "ZZ99": None,
        "": None,
        "not a postcode": None,
        None: None,
    }
    for raw, expected in cases.items():
        assert _one(spark, outward_postcode, raw) == expected, raw


def test_age_band_boundaries(spark):
    df = spark.createDataFrame(
        [(None,), (0,), (17,), (18,), (29,), (30,), (44,), (45,), (59,), (60,), (74,), (75,), (101,)], "age int"
    )
    got = {r["age"]: r["band"] for r in df.select("age", measures.age_band(F.col("age")).alias("band")).collect()}
    assert got[None] == "Unknown"
    assert [got[a] for a in (0, 17)] == ["0-17", "0-17"]
    assert [got[a] for a in (18, 29)] == ["18-29", "18-29"]
    assert [got[a] for a in (30, 44)] == ["30-44", "30-44"]
    assert [got[a] for a in (45, 59)] == ["45-59", "45-59"]
    assert [got[a] for a in (60, 74)] == ["60-74", "60-74"]
    assert [got[a] for a in (75, 101)] == ["75+", "75+"]


def test_decision_measures(spark):
    df = spark.createDataFrame(
        [
            ("Approved", "2025-01-01", "2025-02-15"),
            ("Rejected", "2025-01-01", "2025-04-01"),
            ("In Review", "2025-01-01", None),
        ],
        "status string, submitted string, decided string",
    ).select(
        "status", F.col("submitted").cast("date").alias("submitted"), F.col("decided").cast("date").alias("decided")
    )
    out = (
        df.select(
            "status",
            measures.is_decided(F.col("status")).alias("decided_flag"),
            measures.is_approved(F.col("status")).alias("approved_flag"),
            measures.days_to_decision(F.col("submitted"), F.col("decided")).alias("days"),
        )
        .withColumn("met", measures.met_sla(F.col("days"), 60))
        .collect()
    )
    by_status = {r["status"]: r for r in out}
    assert by_status["Approved"]["days"] == 45 and by_status["Approved"]["met"] is True
    assert by_status["Rejected"]["days"] == 90 and by_status["Rejected"]["met"] is False
    assert by_status["Rejected"]["approved_flag"] is False and by_status["Rejected"]["decided_flag"] is True
    # an open application has no days and no SLA verdict yet (NULL, not False)
    assert by_status["In Review"]["days"] is None and by_status["In Review"]["met"] is None
    assert by_status["In Review"]["decided_flag"] is False
