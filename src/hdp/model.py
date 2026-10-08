"""Entity definitions and reference domains shared by generator, pipeline and tests."""

from __future__ import annotations

ENTITIES = ["studies", "sites", "applications", "participants", "enrolments"]

# Columns exactly as they arrive in the landing CSVs.
RAW_COLUMNS: dict[str, list[str]] = {
    "studies": ["study_id", "title", "study_type", "sponsor", "risk_level", "start_date"],
    "sites": ["site_id", "site_name", "region", "trust_type"],
    "applications": [
        "application_id",
        "study_id",
        "lead_site_id",
        "submitted_date",
        "status",
        "decision_date",
        "updated_at",
    ],
    "participants": ["participant_ref", "date_of_birth", "sex", "ethnicity", "postcode"],
    "enrolments": [
        "enrolment_id",
        "participant_ref",
        "study_id",
        "site_id",
        "enrolled_date",
        "status",
        "updated_at",
    ],
}

# Business key of each entity once it reaches silver. Participants are keyed by
# a pseudonym; the raw identifier never leaves bronze.
SILVER_KEYS: dict[str, list[str]] = {
    "studies": ["study_id"],
    "sites": ["site_id"],
    "applications": ["application_id"],
    "participants": ["participant_pk"],
    "enrolments": ["enrolment_id"],
}

REGIONS = [
    "East Midlands",
    "East of England",
    "London",
    "North East",
    "North West",
    "South East",
    "South West",
    "West Midlands",
    "Yorkshire and The Humber",
]
TRUST_TYPES = ["Acute", "Community", "Mental Health", "Primary Care", "Specialist"]
STUDY_TYPES = ["Clinical trial", "Observational", "Qualitative", "Device study", "Health services research"]
RISK_LEVELS = ["Low", "Medium", "High"]

APPLICATION_STATUSES = ["Submitted", "In Review", "Approved", "Rejected", "Withdrawn"]
DECIDED_STATUSES = ["Approved", "Rejected"]
ENROLMENT_STATUSES = ["Enrolled", "Completed", "Withdrawn"]
SEX_VALUES = ["F", "M", "Other", "Unknown"]
ETHNICITIES = [
    "Asian or Asian British",
    "Black, African, Caribbean or Black British",
    "Mixed or multiple ethnic groups",
    "White",
    "Other ethnic group",
    "Not stated",
]

# Surrogate key used for the "unknown member" row in every dimension.
UNKNOWN_KEY = -1
# Valid-to date of the current version of a type 2 dimension row.
OPEN_ENDED = "9999-12-31"
# Valid-from of the first version of a dimension member, so history always joins.
BEGINNING_OF_TIME = "1900-01-01"
