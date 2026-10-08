"""End-to-end test: generate day 1, run the pipeline, generate day 2, run again, then check
counts, incremental behaviour, history, idempotency and privacy against the generator's manifests."""

import csv
import json
import re
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from hdp.config import Settings
from hdp.generate import Volumes, generate_day1, generate_day2
from hdp.lake import read_table
from hdp.run import run_pipeline

DAY1, DAY2 = "2025-06-01", "2025-06-02"
ENTITIES = ["studies", "sites", "applications", "participants", "enrolments"]


def _count(spark, s, path):
    return read_table(spark, s, path).count()


def _quarantine_counts(spark, s):
    rows = read_table(spark, s, s.quarantine()).groupBy("entity").count().collect()
    return {r["entity"]: r["count"] for r in rows}


@pytest.fixture(scope="module")
def lake(spark, tmp_path_factory):
    s = Settings(base_path=str(tmp_path_factory.mktemp("lake")), pseudo_salt="e2e-test-salt", env="local")
    landing = Path(s.landing())
    out: dict = {"s": s, "landing": landing}

    out["m1"] = generate_day1(landing, DAY1, seed=11, volumes=Volumes.small())
    out["ctx1"] = run_pipeline(spark, s)
    out["after1"] = {
        "silver": {e: _count(spark, s, s.silver(e)) for e in ENTITIES},
        "quarantine": _quarantine_counts(spark, s),
        "dim_site": _count(spark, s, s.gold("dim_site")),
        "fact_application": _count(spark, s, s.gold("fact_application")),
        "fact_enrolment": _count(spark, s, s.gold("fact_enrolment")),
    }

    out["m2"] = generate_day2(landing, DAY2, seed=12)
    out["ctx2"] = run_pipeline(spark, s)
    out["after2"] = {
        "silver": {e: _count(spark, s, s.silver(e)) for e in ENTITIES},
        "quarantine": _quarantine_counts(spark, s),
    }

    out["ctx3"] = run_pipeline(spark, s)  # nothing new: must be a no-op
    out["after3"] = {
        "silver": {e: _count(spark, s, s.silver(e)) for e in ENTITIES},
        "quarantine": _quarantine_counts(spark, s),
        "fact_application": _count(spark, s, s.gold("fact_application")),
        "fact_enrolment": _count(spark, s, s.gold("fact_enrolment")),
    }
    return out


# ---- day 1 ------------------------------------------------------------------------------------------------


def test_day1_silver_row_counts_match_the_generator_manifest(lake):
    m = lake["m1"]
    silver = lake["after1"]["silver"]
    for entity in ["studies", "sites", "applications", "participants", "enrolments"]:
        assert silver[entity] == m["valid_keys_total"][entity], entity


def test_day1_every_injected_error_is_quarantined_and_nothing_else(lake):
    assert lake["after1"]["quarantine"] == {k: v for k, v in lake["m1"]["expected_quarantined"].items() if v}


def test_day1_facts_match_silver_and_have_no_dangling_keys(spark, lake):
    s = lake["s"]
    assert lake["after1"]["fact_application"] == lake["after1"]["silver"]["applications"]
    assert lake["after1"]["fact_enrolment"] == lake["after1"]["silver"]["enrolments"]
    for table, keys in [
        ("fact_application", ["study_key", "site_key"]),
        ("fact_enrolment", ["study_key", "site_key", "participant_key"]),
    ]:
        df = read_table(spark, s, s.gold(table))
        for k in keys:
            assert df.filter(F.col(k) == -1).count() == 0, f"{table}.{k} has unknown-member rows"


def test_dimension_surrogate_keys_are_unique(spark, lake):
    s = lake["s"]
    for table, key in [
        ("dim_study", "study_key"),
        ("dim_site", "site_key"),
        ("dim_participant", "participant_key"),
        ("dim_date", "date_key"),
    ]:
        df = read_table(spark, s, s.gold(table))
        assert df.count() == df.select(key).distinct().count(), table
        assert df.filter(F.col(key) == -1).count() == 1, f"{table} must have exactly one unknown member"


# ---- day 2: incremental behaviour ---------------------------------------------------------------------------


def test_day2_only_processes_the_new_extract_not_the_whole_history(lake):
    m2 = lake["m2"]
    for entity in ["applications", "enrolments", "participants", "studies"]:
        bronze = lake["ctx2"].rows("bronze", entity)[0]
        silver = lake["ctx2"].rows("silver", entity)[0]
        assert bronze["rows_in"] == m2["rows_written"][entity], entity
        assert silver["rows_in"] == m2["rows_written"][entity], entity
    total_apps = lake["after2"]["silver"]["applications"]
    assert lake["ctx2"].rows("silver", "applications")[0]["rows_in"] < total_apps / 10


def test_day2_gold_facts_only_touch_changed_rows(lake):
    ctx2 = lake["ctx2"]
    silver_changed = ctx2.rows("silver", "applications")[0]["rows_out"]
    gold_in = ctx2.rows("gold", "fact_application")[0]["rows_in"]
    assert 0 < gold_in <= silver_changed
    assert gold_in < lake["after2"]["silver"]["applications"] / 10


def test_progressed_applications_have_their_new_status_in_silver_and_gold(spark, lake):
    s, m2 = lake["s"], lake["m2"]
    silver = {r["application_id"]: r["status"] for r in read_table(spark, s, s.silver("applications")).collect()}
    gold = {r["application_id"]: r for r in read_table(spark, s, s.gold("fact_application")).collect()}
    assert m2["progressed_applications"]
    for app_id, status in m2["progressed_applications"].items():
        assert silver[app_id] == status
        assert gold[app_id]["status"] == status
        assert gold[app_id]["is_decided"] is True
        assert gold[app_id]["days_to_decision"] is not None


def test_a_stale_record_does_not_overwrite_newer_data(spark, lake):
    s = lake["s"]
    stale = lake["m2"]["stale_application"]
    row = read_table(spark, s, s.silver("applications")).filter(F.col("application_id") == stale["id"]).first()
    assert row["status"] == stale["expected_status"]


def test_changed_enrolments_and_amended_studies_are_updated(spark, lake):
    s, m2 = lake["s"], lake["m2"]
    enr = {r["enrolment_id"]: r["status"] for r in read_table(spark, s, s.silver("enrolments")).collect()}
    assert m2["changed_enrolments"]
    for enr_id, status in m2["changed_enrolments"].items():
        assert enr[enr_id] == status
    titles = {r["study_id"]: r["title"] for r in read_table(spark, s, s.silver("studies")).collect()}
    assert all(titles[sid].endswith("(amended)") for sid in m2["amended_studies"])


def test_day2_quarantine_only_grew_by_the_injected_errors(lake):
    expected = {}
    for m in (lake["m1"], lake["m2"]):
        for entity, n in m["expected_quarantined"].items():
            expected[entity] = expected.get(entity, 0) + n
    assert lake["after2"]["quarantine"] == {k: v for k, v in expected.items() if v}


# ---- history (SCD type 2) -------------------------------------------------------------------------------------


def test_renamed_sites_keep_their_history(spark, lake):
    s, m2 = lake["s"], lake["m2"]
    dim = read_table(spark, s, s.gold("dim_site"))
    n_renamed = len(m2["renamed_sites"])
    assert dim.count() == lake["after1"]["dim_site"] + n_renamed
    for site_id in m2["renamed_sites"]:
        versions = dim.filter(F.col("site_id") == site_id).orderBy("valid_from").collect()
        assert len(versions) == 2
        old, new = versions
        assert old["is_current"] is False and new["is_current"] is True
        assert old["valid_to"] == new["valid_from"]
        assert "Integrated Care Partnership" in new["site_name"] and "Health Partnership" in old["site_name"]
    assert dim.filter(F.col("is_current")).groupBy("site_id").count().filter("count > 1").count() == 0


def test_every_fact_points_at_the_site_version_valid_on_its_date(spark, lake):
    s = lake["s"]
    dim = read_table(spark, s, s.gold("dim_site")).select("site_key", "valid_from", "valid_to")
    for table, date_col in [("fact_application", "submitted_date_key"), ("fact_enrolment", "enrolled_date_key")]:
        fact = read_table(spark, s, s.gold(table)).withColumn(
            "d", F.to_date(F.col(date_col).cast("string"), "yyyyMMdd")
        )
        joined = fact.join(dim, "site_key")
        outside = joined.filter(~((F.col("d") >= F.col("valid_from")) & (F.col("d") < F.col("valid_to"))))
        assert outside.count() == 0, table
        assert joined.count() == fact.count()


# ---- idempotency ------------------------------------------------------------------------------------------------


def test_rerunning_with_nothing_new_changes_nothing(lake):
    assert lake["after3"]["silver"] == lake["after2"]["silver"]
    assert lake["after3"]["quarantine"] == lake["after2"]["quarantine"]
    assert all(r["status"] == "skipped" for r in lake["ctx3"].rows("bronze"))
    assert all(r["status"] == "skipped" for r in lake["ctx3"].rows("silver"))


def test_silver_keys_are_unique(spark, lake):
    from hdp.dq import duplicate_keys
    from hdp.model import SILVER_KEYS

    s = lake["s"]
    for entity, keys in SILVER_KEYS.items():
        assert duplicate_keys(read_table(spark, s, s.silver(entity)), keys) == 0, entity


# ---- measures and marts -------------------------------------------------------------------------------------------


def test_mart_totals_reconcile_to_the_facts(spark, lake):
    s = lake["s"]
    mart = read_table(spark, s, s.gold("mart_monthly_performance"))
    totals = mart.agg(
        F.sum("applications_submitted").alias("a"),
        F.sum("enrolments").alias("e"),
        F.sum("applications_decided").alias("d"),
    ).first()
    assert totals["a"] == lake["after3"]["fact_application"]
    assert totals["e"] == lake["after3"]["fact_enrolment"]
    facts = read_table(spark, s, s.gold("fact_application"))
    assert totals["d"] == facts.filter("is_decided").count()
    bad = mart.filter((F.col("pct_decided_within_target") < 0) | (F.col("pct_decided_within_target") > 100))
    assert bad.count() == 0


# ---- privacy ------------------------------------------------------------------------------------------------------


FORBIDDEN_COLUMNS = {"participant_ref", "date_of_birth", "dob", "postcode"}


def test_no_raw_identifiers_or_full_postcodes_beyond_bronze(spark, lake):
    s = lake["s"]
    tables = [s.silver(e) for e in ENTITIES] + [
        s.gold(t) for t in ["dim_participant", "dim_study", "dim_site", "fact_application", "fact_enrolment"]
    ]
    for path in tables:
        assert not FORBIDDEN_COLUMNS & set(read_table(spark, s, path).columns), path


def test_pseudonyms_are_hashes_and_never_the_raw_ten_digit_reference(spark, lake):
    s = lake["s"]
    pks = [r["participant_pk"] for r in read_table(spark, s, s.silver("participants")).collect()]
    assert pks and all(re.fullmatch(r"[0-9a-f]{64}", p) for p in pks)
    raw_refs = {
        r["participant_ref"]
        for p in lake["landing"].glob("batch_date=*/participants.csv")
        for r in csv.DictReader(p.open())
    }
    assert not raw_refs & set(pks)


def test_quarantined_participants_do_not_leak_raw_identifiers(spark, lake):
    s = lake["s"]
    payloads = [r["payload"] for r in read_table(spark, s, s.quarantine()).filter("entity = 'participants'").collect()]
    assert payloads
    raw_refs = {
        r["participant_ref"]
        for p in lake["landing"].glob("batch_date=*/participants.csv")
        for r in csv.DictReader(p.open())
    }
    for payload in payloads:
        doc = json.loads(payload)
        assert "participant_ref" not in doc and "date_of_birth" not in doc and "postcode" not in doc
        assert not any(ref in payload for ref in raw_refs if len(ref) == 10)


def test_bronze_is_the_only_layer_that_keeps_raw_values(spark, lake):
    s = lake["s"]
    assert "participant_ref" in read_table(spark, s, s.bronze("participants")).columns  # restricted zone


# ---- observability ------------------------------------------------------------------------------------------------


def test_every_run_is_recorded_in_the_run_log(spark, lake):
    s = lake["s"]
    log = read_table(spark, s, s.control("run_log"))
    run_ids = {r["run_id"] for r in log.select("run_id").distinct().collect()}
    assert run_ids == {lake[k].run_id for k in ("ctx1", "ctx2", "ctx3")}
    assert log.filter(F.col("status") == "failed").count() == 0
    assert log.filter(F.col("duration_s").isNull()).count() == 0
