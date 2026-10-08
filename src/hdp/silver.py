"""Silver: typed, cleaned, de-duplicated, privacy-safe current state of each entity.

For each entity the stage:

1. reads only bronze rows newer than the entity's watermark (incremental load),
2. cleans and types them, applying the privacy controls (pseudonymise, generalise),
3. runs the data quality rules: error rows go to quarantine, warning rows load with a flag,
4. keeps the latest version of each key and upserts it into the silver table,
5. moves the watermark forward.

Entities run in dependency order so referential checks see the reference data loaded in
this same run. A row that references something that does not exist yet is quarantined, not
silently dropped or loaded with a dangling key.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from hdp import dq
from hdp.config import Settings
from hdp.dq import Rule
from hdp.lake import apply_watermark, latest_per_key, read_table, set_watermark, table_exists, upsert, write_table
from hdp.model import (
    APPLICATION_STATUSES,
    ENROLMENT_STATUSES,
    ENTITIES,
    ETHNICITIES,
    REGIONS,
    RISK_LEVELS,
    SILVER_KEYS,
    TRUST_TYPES,
)
from hdp.observability import RunContext
from hdp.privacy import outward_postcode, pseudonymise

AUDIT = ["_batch_date", "_run_id", "_source_file", "_ingested_at"]


def _text(name: str) -> Column:
    """Trimmed string, with blank treated as NULL."""
    trimmed = F.trim(F.col(name))
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def _audit() -> list[Column]:
    return [F.col(c) for c in AUDIT]


def _ten_digits(name: str) -> Column:
    return _text(name).rlike(r"^[0-9]{10}$")


# --- cleaning: raw strings in, typed and standardised columns out --------------------------------------


def clean_studies(df: DataFrame, s: Settings) -> DataFrame:
    return df.select(
        F.upper(_text("study_id")).alias("study_id"),
        _text("title").alias("title"),
        _text("study_type").alias("study_type"),
        _text("sponsor").alias("sponsor"),
        F.initcap(_text("risk_level")).alias("risk_level"),
        F.col("start_date").cast("date").alias("start_date"),
        F.col("_ingested_at").alias("updated_at"),  # the source sends no change timestamp
        *_audit(),
    )


def clean_sites(df: DataFrame, s: Settings) -> DataFrame:
    return df.select(
        F.upper(_text("site_id")).alias("site_id"),
        _text("site_name").alias("site_name"),
        _text("region").alias("region"),
        _text("trust_type").alias("trust_type"),
        F.col("_ingested_at").alias("updated_at"),
        *_audit(),
    )


def clean_applications(df: DataFrame, s: Settings) -> DataFrame:
    decision_raw = _text("decision_date")
    decision = decision_raw.cast("date")
    return df.select(
        F.upper(_text("application_id")).alias("application_id"),
        F.upper(_text("study_id")).alias("study_id"),
        F.upper(_text("lead_site_id")).alias("lead_site_id"),
        F.col("submitted_date").cast("date").alias("submitted_date"),
        F.initcap(F.lower(_text("status"))).alias("status"),
        decision.alias("decision_date"),
        F.col("updated_at").cast("timestamp").alias("updated_at"),
        # a non-blank decision date that failed to parse would otherwise look like "no decision yet"
        (decision_raw.isNull() | decision.isNotNull()).alias("_chk_decision_date_parsed"),
        *_audit(),
    )


def clean_participants(df: DataFrame, s: Settings) -> DataFrame:
    sex_raw = F.upper(F.coalesce(_text("sex"), F.lit("")))
    sex = (
        F.when(sex_raw.isin("F", "FEMALE"), "F")
        .when(sex_raw.isin("M", "MALE"), "M")
        .when(sex_raw == "OTHER", "Other")
        .otherwise("Unknown")
    )
    return df.select(
        # raw identifier -> pseudonym; NULL if the identifier is malformed
        F.when(_ten_digits("participant_ref"), pseudonymise(_text("participant_ref"), s.pseudo_salt)).alias(
            "participant_pk"
        ),
        F.year(F.col("date_of_birth").cast("date")).alias("birth_year"),  # date of birth is not kept
        sex.alias("sex"),
        _text("ethnicity").alias("ethnicity"),
        outward_postcode(F.col("postcode")).alias("outward_postcode"),
        F.col("_ingested_at").alias("updated_at"),
        (_text("postcode").isNull() | outward_postcode(F.col("postcode")).isNotNull()).alias("_chk_postcode_ok"),
        *_audit(),
    )


def clean_enrolments(df: DataFrame, s: Settings) -> DataFrame:
    return df.select(
        F.upper(_text("enrolment_id")).alias("enrolment_id"),
        F.when(_ten_digits("participant_ref"), pseudonymise(_text("participant_ref"), s.pseudo_salt)).alias(
            "participant_pk"
        ),
        F.upper(_text("study_id")).alias("study_id"),
        F.upper(_text("site_id")).alias("site_id"),
        F.col("enrolled_date").cast("date").alias("enrolled_date"),
        F.initcap(F.lower(_text("status"))).alias("status"),
        F.col("updated_at").cast("timestamp").alias("updated_at"),
        *_audit(),
    )


CLEANERS = {
    "studies": clean_studies,
    "sites": clean_sites,
    "applications": clean_applications,
    "participants": clean_participants,
    "enrolments": clean_enrolments,
}


# --- referential checks -------------------------------------------------------------------------------


def with_ref_flag(
    spark: SparkSession, s: Settings, df: DataFrame, column: str, ref_entity: str, flag: str
) -> DataFrame:
    """Add a boolean helper column: does `df[column]` exist in the silver table of `ref_entity`?"""
    path = s.silver(ref_entity)
    key = SILVER_KEYS[ref_entity][0]
    if not table_exists(spark, s, path):
        return df.withColumn(flag, F.lit(False))
    ref = read_table(spark, s, path).select(F.col(key).alias("_ref_key")).distinct().withColumn(flag, F.lit(True))
    return (
        df.join(ref, df[column] == ref["_ref_key"], "left")
        .drop("_ref_key")
        .withColumn(flag, F.coalesce(F.col(flag), F.lit(False)))
    )


# --- data quality rules -------------------------------------------------------------------------------


def _batch_date() -> Column:
    return F.col("_batch_date").cast("date")


def rules_for(entity: str) -> list[Rule]:
    c = F.col
    if entity == "studies":
        return [
            Rule("study_id_format", c("study_id").rlike(r"^S[0-9]{5}$")),
            Rule("title_present", c("title").isNotNull()),
            Rule("risk_level_valid", c("risk_level").isin(*RISK_LEVELS)),
            Rule("start_date_valid", c("start_date").isNotNull()),
        ]
    if entity == "sites":
        return [
            Rule("site_id_format", c("site_id").rlike(r"^SITE[0-9]{3}$")),
            Rule("site_name_present", c("site_name").isNotNull()),
            Rule("region_valid", c("region").isin(*REGIONS)),
            Rule("trust_type_valid", c("trust_type").isin(*TRUST_TYPES), severity="warning"),
        ]
    if entity == "applications":
        return [
            Rule("application_id_format", c("application_id").rlike(r"^APP[0-9]{6}$")),
            Rule("study_exists", c("_chk_study_exists")),
            Rule("site_exists", c("_chk_site_exists")),
            Rule("submitted_date_valid", c("submitted_date").isNotNull() & (c("submitted_date") <= _batch_date())),
            Rule("status_valid", c("status").isin(*APPLICATION_STATUSES)),
            Rule("decision_date_parsed", c("_chk_decision_date_parsed")),
            Rule(
                "decision_date_plausible",
                c("decision_date").isNull()
                | ((c("decision_date") >= c("submitted_date")) & (c("decision_date") <= _batch_date())),
            ),
            Rule(
                "decided_has_decision_date", ~c("status").isin("Approved", "Rejected") | c("decision_date").isNotNull()
            ),
            Rule("updated_at_valid", c("updated_at").isNotNull()),
        ]
    if entity == "participants":
        return [
            Rule("identifier_valid", c("participant_pk").isNotNull()),
            Rule("birth_year_plausible", c("birth_year").between(1900, F.year(_batch_date()))),
            Rule("postcode_well_formed", c("_chk_postcode_ok"), severity="warning"),
            Rule(
                "ethnicity_recognised", c("ethnicity").isin(*ETHNICITIES) | c("ethnicity").isNull(), severity="warning"
            ),
        ]
    if entity == "enrolments":
        return [
            Rule("enrolment_id_format", c("enrolment_id").rlike(r"^ENR[0-9]{7}$")),
            Rule("participant_exists", c("participant_pk").isNotNull() & c("_chk_participant_exists")),
            Rule("study_exists", c("_chk_study_exists")),
            Rule("site_exists", c("_chk_site_exists")),
            Rule("enrolled_date_valid", c("enrolled_date").isNotNull() & (c("enrolled_date") <= _batch_date())),
            Rule("status_valid", c("status").isin(*ENROLMENT_STATUSES)),
            Rule("updated_at_valid", c("updated_at").isNotNull()),
        ]
    raise KeyError(entity)


def _add_reference_flags(spark: SparkSession, s: Settings, entity: str, df: DataFrame) -> DataFrame:
    if entity == "applications":
        df = with_ref_flag(spark, s, df, "study_id", "studies", "_chk_study_exists")
        df = with_ref_flag(spark, s, df, "lead_site_id", "sites", "_chk_site_exists")
    elif entity == "enrolments":
        df = with_ref_flag(spark, s, df, "participant_pk", "participants", "_chk_participant_exists")
        df = with_ref_flag(spark, s, df, "study_id", "studies", "_chk_study_exists")
        df = with_ref_flag(spark, s, df, "site_id", "sites", "_chk_site_exists")
    return df


# --- stage --------------------------------------------------------------------------------------------


def run(spark: SparkSession, s: Settings, ctx: RunContext, full_refresh: bool = False) -> None:
    for entity in ENTITIES:
        with ctx.stage("silver", entity) as info:
            bronze_path = s.bronze(entity)
            if not table_exists(spark, s, bronze_path):
                info.update(status="skipped", message="no bronze table", rows_in=0, rows_out=0, rows_quarantined=0)
                continue

            incoming = read_table(spark, s, bronze_path)
            if not full_refresh:
                incoming = apply_watermark(spark, s, incoming, entity)
            incoming.persist()
            try:
                rows_in = incoming.count()
                if rows_in == 0:
                    info.update(
                        status="skipped",
                        message="nothing new since watermark",
                        rows_in=0,
                        rows_out=0,
                        rows_quarantined=0,
                    )
                    continue

                cleaned = _add_reference_flags(spark, s, entity, CLEANERS[entity](incoming, s))
                valid, quarantined, stats = dq.apply_rules(cleaned, rules_for(entity))

                keys = SILVER_KEYS[entity]
                latest = latest_per_key(valid, keys, ["updated_at", "_batch_date", "_ingested_at"])
                latest.persist()
                try:
                    rows_out = latest.count()
                    upsert(spark, s, latest, s.silver(entity), keys, ["updated_at", "_batch_date", "_ingested_at"])
                finally:
                    latest.unpersist()

                if stats["_quarantined"]:
                    write_table(
                        quarantined.withColumn("entity", F.lit(entity)).withColumn(
                            "quarantined_at", F.current_timestamp()
                        ),
                        s,
                        s.quarantine(),
                        "append",
                    )
                set_watermark(spark, s, entity, incoming)
                failing = {k: v for k, v in stats.items() if not k.startswith("_") and v}
                info.update(
                    rows_in=rows_in,
                    rows_out=rows_out,
                    rows_quarantined=stats["_quarantined"],
                    message=f"rule failures: {failing}" if failing else "",
                )
            finally:
                incoming.unpersist()
