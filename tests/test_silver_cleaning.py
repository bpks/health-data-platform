from datetime import date

from hdp.config import Settings
from hdp.silver import clean_applications, clean_enrolments, clean_participants, clean_studies

S = Settings(pseudo_salt="unit-test-salt")
AUDIT = "_batch_date string, _run_id string, _source_file string, _ingested_at timestamp"


def _frame(spark, cols, rows):
    return spark.createDataFrame([(*r, "2025-06-01", "run", "file.csv", None) for r in rows], f"{cols}, {AUDIT}")


def test_studies_are_trimmed_uppercased_and_typed(spark):
    df = _frame(
        spark,
        "study_id string, title string, study_type string, sponsor string, risk_level string, start_date string",
        [(" s00007 ", " A title ", "Observational", "  ", "HIGH", "2024-03-05")],
    )
    row = clean_studies(df, S).first()
    assert row["study_id"] == "S00007" and row["title"] == "A title"
    assert row["sponsor"] is None  # blank becomes NULL
    assert row["risk_level"] == "High"
    assert row["start_date"] == date(2024, 3, 5)


def test_applications_status_is_standardised_and_bad_values_become_null(spark):
    cols = (
        "application_id string, study_id string, lead_site_id string, submitted_date string, "
        "status string, decision_date string, updated_at string"
    )
    df = _frame(
        spark,
        cols,
        [
            ("APP000001", " s00001", "site001 ", "2025-01-05", " in REVIEW ", "", "2025-01-06T10:00:00"),
            ("APP000002", "S00001", "SITE001", "2025-02-30", "approved", "garbage", "not a time"),
        ],
    )
    rows = {r["application_id"]: r for r in clean_applications(df, S).collect()}
    a, b = rows["APP000001"], rows["APP000002"]
    assert a["status"] == "In Review" and a["study_id"] == "S00001" and a["lead_site_id"] == "SITE001"
    assert a["decision_date"] is None and a["_chk_decision_date_parsed"] is True
    assert b["submitted_date"] is None  # 30 February does not exist
    assert b["updated_at"] is None
    # an unparseable decision date must be flagged, not mistaken for "no decision yet"
    assert b["decision_date"] is None and b["_chk_decision_date_parsed"] is False


def test_participants_never_expose_raw_identifier_dob_or_full_postcode(spark):
    df = _frame(
        spark,
        "participant_ref string, date_of_birth string, sex string, ethnicity string, postcode string",
        [
            ("1234567890", "1980-07-14", " female ", "White", "mk9 3ab"),
            ("12345", "31/12/1980", "x", None, "ZZ99"),
        ],
    )
    out = clean_participants(df, S)
    assert not {"participant_ref", "date_of_birth", "postcode"} & set(out.columns)
    good, bad = (r for r in sorted(out.collect(), key=lambda r: r["participant_pk"] is None, reverse=False))
    assert good["participant_pk"] is not None and len(good["participant_pk"]) == 64
    assert "1234567890" not in str(good.asDict().values())
    assert good["birth_year"] == 1980 and good["sex"] == "F" and good["outward_postcode"] == "MK9"
    assert bad["participant_pk"] is None  # 5-digit identifier is invalid
    assert bad["birth_year"] is None  # unparseable date of birth
    assert bad["sex"] == "Unknown" and bad["outward_postcode"] is None and bad["_chk_postcode_ok"] is False


def test_enrolments_use_the_same_pseudonym_as_participants(spark):
    people = _frame(
        spark,
        "participant_ref string, date_of_birth string, sex string, ethnicity string, postcode string",
        [("1234567890", "1980-07-14", "F", "White", "MK9 3AB")],
    )
    enrol = _frame(
        spark,
        "enrolment_id string, participant_ref string, study_id string, site_id string, "
        "enrolled_date string, status string, updated_at string",
        [("enr0000001", "1234567890", "S00001", "SITE001", "2025-05-01", "enrolled", "2025-05-01T09:00:00")],
    )
    pk_person = clean_participants(people, S).first()["participant_pk"]
    row = clean_enrolments(enrol, S).first()
    assert row["participant_pk"] == pk_person
    assert row["enrolment_id"] == "ENR0000001" and row["status"] == "Enrolled"
