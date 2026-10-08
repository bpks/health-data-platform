"""Gold: Kimball star schema built from silver.

* Conformed dimensions (`dim_date`, `dim_study`, `dim_site`) are shared by both fact tables,
  so "study" and "region" mean the same thing wherever they are used.
* `dim_site` is a type 2 slowly changing dimension: a renamed site gets a new version row and
  facts keep pointing at the version that was current on the fact's date.
* Every dimension has an "unknown member" row (key -1) so a fact never has a dangling key.
* Surrogate keys are deterministic hashes of the business key, so re-running never reshuffles
  keys and no sequence generator is needed.
* Facts and the participant dimension are loaded incrementally: only silver rows that gold has not
  caught up with (a missing key, or a newer `updated_at`), so a re-run after any partial failure
  recovers by itself. The study and site dimensions are small and rebuilt or merged each run; the mart
  is an aggregate rebuilt from the facts.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType

from hdp import measures
from hdp.config import Settings
from hdp.lake import read_table, replace_table, table_exists, upsert
from hdp.model import BEGINNING_OF_TIME, OPEN_ENDED, UNKNOWN_KEY
from hdp.observability import RunContext

# --- helpers ---------------------------------------------------------------------------------------------


def surrogate_key(*cols: Column) -> Column:
    return F.xxhash64(*cols)


def date_key(col: Column) -> Column:
    """yyyymmdd as an int, or -1 when the date is missing."""
    return F.coalesce(F.date_format(col, "yyyyMMdd").cast("int"), F.lit(UNKNOWN_KEY))


def unknown_member(spark: SparkSession, schema: StructType, **values: object) -> DataFrame:
    row = tuple(values.get(f.name) for f in schema.fields)
    # Every column is made nullable: the unknown member legitimately has no date, name, etc.
    nullable = StructType([StructField(f.name, f.dataType, True) for f in schema.fields])
    return spark.createDataFrame([row], nullable)


def rows_gold_has_not_seen(
    spark: SparkSession, s: Settings, silver_df: DataFrame, gold_path: str, key: str, full_refresh: bool
) -> DataFrame:
    """Silver rows that `gold_path` has not caught up with: no gold row for the key, or an older `updated_at`.

    This is stateless on purpose. Selecting "rows from this run" would silently skip a batch whenever a
    previous run failed part-way (bronze done, gold not), whereas comparing against what gold already holds
    picks up exactly what is missing, however the last run ended.
    """
    if full_refresh or not table_exists(spark, s, gold_path):
        return silver_df
    held = read_table(spark, s, gold_path).select(key, F.col("updated_at").alias("_gold_updated_at"))
    return (
        silver_df.join(held, key, "left")
        .filter(F.col("_gold_updated_at").isNull() | (F.col("updated_at") > F.col("_gold_updated_at")))
        .drop("_gold_updated_at")
    )


# --- dimensions ------------------------------------------------------------------------------------------


def build_dim_date(spark: SparkSession, start: str = "2018-01-01", end: str = "2032-12-31") -> DataFrame:
    days = spark.sql(f"SELECT explode(sequence(DATE '{start}', DATE '{end}', INTERVAL 1 DAY)) AS date")
    d = F.col("date")
    # UK financial year runs April to March, e.g. "2025/26"
    fy_start = F.when(F.month(d) >= 4, F.year(d)).otherwise(F.year(d) - 1)
    dim = days.select(
        date_key(d).alias("date_key"),
        d.alias("date"),
        F.year(d).alias("year"),
        F.quarter(d).alias("quarter"),
        F.month(d).alias("month"),
        F.date_format(d, "MMMM").alias("month_name"),
        F.date_format(d, "yyyy-MM").alias("year_month"),
        F.date_format(d, "EEEE").alias("day_name"),
        F.dayofweek(d).isin(1, 7).alias("is_weekend"),
        F.concat(fy_start.cast("string"), F.lit("/"), F.lpad(((fy_start + 1) % 100).cast("string"), 2, "0")).alias(
            "financial_year"
        ),
    )
    return dim.unionByName(
        unknown_member(
            spark,
            dim.schema,
            date_key=UNKNOWN_KEY,
            month_name="Unknown",
            year_month="Unknown",
            day_name="Unknown",
            financial_year="Unknown",
        )
    )


def build_dim_study(spark: SparkSession, silver_studies: DataFrame) -> DataFrame:
    dim = silver_studies.select(
        surrogate_key(F.col("study_id")).alias("study_key"),
        "study_id",
        "title",
        "study_type",
        "sponsor",
        "risk_level",
        "start_date",
    )
    return dim.unionByName(
        unknown_member(spark, dim.schema, study_key=UNKNOWN_KEY, study_id="UNKNOWN", title="Unknown")
    )


def _row_hash(*cols: Column) -> Column:
    return F.sha2(F.concat_ws("||", *[F.coalesce(c.cast("string"), F.lit("")) for c in cols]), 256)


def scd2_merge(spark: SparkSession, existing: DataFrame | None, incoming: DataFrame, effective_date: str) -> DataFrame:
    """Type 2 merge for dim_site.

    incoming   current snapshot of sites: site_id, site_name, region, trust_type
    existing   current dim_site (or None on the first load)
    effective_date  the date a detected change takes effect (the batch date)

    Versions are half-open: valid_from <= date < valid_to. The current version has
    valid_to = 9999-12-31. A brand new site starts at 1900-01-01 so facts of any date can
    resolve to it. A changed site has its current version closed on `effective_date` and a
    new version opened on that date.
    """
    tracked = ["site_name", "region", "trust_type"]
    snap = incoming.select("site_id", *tracked).withColumn("row_hash", _row_hash(*[F.col(c) for c in tracked]))

    def versioned(df: DataFrame, valid_from: str) -> DataFrame:
        vf = F.lit(valid_from).cast("date")
        return df.select(
            surrogate_key(F.col("site_id"), vf.cast("string")).alias("site_key"),
            "site_id",
            *tracked,
            vf.alias("valid_from"),
            F.lit(OPEN_ENDED).cast("date").alias("valid_to"),
            F.lit(True).alias("is_current"),
            "row_hash",
        )

    if existing is None:
        first = versioned(snap, BEGINNING_OF_TIME)
        return first.unionByName(
            unknown_member(
                spark,
                first.schema,
                site_key=UNKNOWN_KEY,
                site_id="UNKNOWN",
                site_name="Unknown",
                valid_from=None,
                valid_to=None,
                is_current=True,
            )
            .withColumn("valid_from", F.lit(BEGINNING_OF_TIME).cast("date"))
            .withColumn("valid_to", F.lit(OPEN_ENDED).cast("date"))
        )

    current = existing.filter(F.col("is_current") & (F.col("site_id") != "UNKNOWN")).select(
        F.col("site_id").alias("cur_site_id"), F.col("row_hash").alias("cur_hash")
    )
    joined = snap.join(current, snap.site_id == current.cur_site_id, "left")
    brand_new = joined.filter(F.col("cur_site_id").isNull()).drop("cur_site_id", "cur_hash")
    changed = joined.filter(F.col("cur_site_id").isNotNull() & (F.col("cur_hash") != F.col("row_hash"))).drop(
        "cur_site_id", "cur_hash"
    )

    changed_ids = changed.select(F.col("site_id").alias("chg_site_id"))
    to_close = existing.join(changed_ids, existing.site_id == changed_ids.chg_site_id, "left_semi").filter(
        F.col("is_current")
    )
    untouched = existing.join(
        to_close.select(F.col("site_key").alias("closed_key")), existing.site_key == F.col("closed_key"), "left_anti"
    )
    closed = to_close.withColumn("valid_to", F.lit(effective_date).cast("date")).withColumn("is_current", F.lit(False))

    return (
        untouched.unionByName(closed)
        .unionByName(versioned(changed, effective_date))
        .unionByName(versioned(brand_new, BEGINNING_OF_TIME))
    )


def build_dim_participant(spark: SparkSession, silver_participants: DataFrame) -> DataFrame:
    return silver_participants.select(
        surrogate_key(F.col("participant_pk")).alias("participant_key"),
        "participant_pk",
        "birth_year",
        "sex",
        "ethnicity",
        "outward_postcode",
        "updated_at",
    )


# --- facts -----------------------------------------------------------------------------------------------


def _as_of_site(df: DataFrame, site_col: str, date_col: str, dim_site: DataFrame) -> DataFrame:
    """Attach the dim_site version that was current on `date_col`."""
    versions = dim_site.select(
        F.col("site_id").alias("_v_site_id"), "site_key", F.col("region").alias("site_region"), "valid_from", "valid_to"
    )
    cond = (
        (df[site_col] == versions["_v_site_id"])
        & (df[date_col] >= versions["valid_from"])
        & (df[date_col] < versions["valid_to"])
    )
    return df.join(versions, cond, "left").drop("_v_site_id", "valid_from", "valid_to")


def build_fact_application(apps: DataFrame, dim_study: DataFrame, dim_site: DataFrame, sla_days: int) -> DataFrame:
    studies = dim_study.select("study_id", "study_key")
    f = _as_of_site(apps, "lead_site_id", "submitted_date", dim_site).join(studies, "study_id", "left")
    days = measures.days_to_decision(F.col("submitted_date"), F.col("decision_date"))
    return f.select(
        surrogate_key(F.col("application_id")).alias("application_key"),
        "application_id",
        F.coalesce(F.col("study_key"), F.lit(UNKNOWN_KEY)).alias("study_key"),
        F.coalesce(F.col("site_key"), F.lit(UNKNOWN_KEY)).alias("site_key"),
        F.coalesce(F.col("site_region"), F.lit("Unknown")).alias("site_region"),
        date_key(F.col("submitted_date")).alias("submitted_date_key"),
        date_key(F.col("decision_date")).alias("decision_date_key"),
        "status",
        measures.is_decided(F.col("status")).alias("is_decided"),
        measures.is_approved(F.col("status")).alias("is_approved"),
        days.alias("days_to_decision"),
        measures.met_sla(days, sla_days).alias("met_sla"),
        "updated_at",
        F.col("_run_id").alias("_silver_run_id"),
    )


def build_fact_enrolment(
    enrolments: DataFrame, dim_study: DataFrame, dim_site: DataFrame, dim_participant: DataFrame
) -> DataFrame:
    studies = dim_study.select("study_id", "study_key")
    people = dim_participant.select("participant_pk", "participant_key", "birth_year")
    f = (
        _as_of_site(enrolments, "site_id", "enrolled_date", dim_site)
        .join(studies, "study_id", "left")
        .join(people, "participant_pk", "left")
    )
    # Age is approximate (birth year only): date of birth is deliberately not kept.
    approx_age = F.year(F.col("enrolled_date")) - F.col("birth_year")
    return f.select(
        surrogate_key(F.col("enrolment_id")).alias("enrolment_key"),
        "enrolment_id",
        F.coalesce(F.col("participant_key"), F.lit(UNKNOWN_KEY)).alias("participant_key"),
        F.coalesce(F.col("study_key"), F.lit(UNKNOWN_KEY)).alias("study_key"),
        F.coalesce(F.col("site_key"), F.lit(UNKNOWN_KEY)).alias("site_key"),
        F.coalesce(F.col("site_region"), F.lit("Unknown")).alias("site_region"),
        date_key(F.col("enrolled_date")).alias("enrolled_date_key"),
        "status",
        measures.age_band(approx_age).alias("age_band_at_enrolment"),
        F.lit(1).alias("enrolment_count"),
        "updated_at",
        F.col("_run_id").alias("_silver_run_id"),
    )


def build_mart_monthly(fact_application: DataFrame, fact_enrolment: DataFrame) -> DataFrame:
    """Study x region x month. Applications are attributed to the lead site's region and
    enrolments to the enrolling site's region; both use the same month key (yyyymm)."""
    apps = fact_application.groupBy(
        "study_key", "site_region", (F.col("submitted_date_key") / 100).cast("int").alias("month_key")
    ).agg(
        F.count(F.lit(1)).alias("applications_submitted"),
        F.sum(F.col("is_decided").cast("int")).alias("applications_decided"),
        F.sum(F.col("is_approved").cast("int")).alias("applications_approved"),
        F.sum(F.col("met_sla").cast("int")).alias("decided_within_target"),
        F.round(F.avg("days_to_decision"), 1).alias("avg_days_to_decision"),
    )
    enr = fact_enrolment.groupBy(
        "study_key", "site_region", (F.col("enrolled_date_key") / 100).cast("int").alias("month_key")
    ).agg(F.sum("enrolment_count").alias("enrolments"))
    mart = apps.join(enr, ["study_key", "site_region", "month_key"], "full_outer")
    counts = [
        "applications_submitted",
        "applications_decided",
        "applications_approved",
        "decided_within_target",
        "enrolments",
    ]
    for c in counts:
        mart = mart.withColumn(c, F.coalesce(F.col(c), F.lit(0)).cast("long"))
    return mart.withColumn(
        "pct_decided_within_target",
        F.when(
            F.col("applications_decided") > 0,
            F.round(F.col("decided_within_target") / F.col("applications_decided") * 100, 1),
        ),
    )


# --- stage -----------------------------------------------------------------------------------------------


def run(spark: SparkSession, s: Settings, ctx: RunContext, full_refresh: bool = False) -> None:
    def silver(entity: str) -> DataFrame | None:
        return read_table(spark, s, s.silver(entity)) if table_exists(spark, s, s.silver(entity)) else None

    studies, sites, apps, people, enrolments = (
        silver(e) for e in ["studies", "sites", "applications", "participants", "enrolments"]
    )

    with ctx.stage("gold", "dim_date") as info:
        if table_exists(spark, s, s.gold("dim_date")):
            info.update(status="skipped", message="already built")
        else:
            dim = build_dim_date(spark)
            replace_table(spark, s, dim, s.gold("dim_date"))
            info.update(rows_out=dim.count())

    with ctx.stage("gold", "dim_study") as info:
        if studies is None:
            info.update(status="skipped", message="no silver studies")
        else:
            dim = build_dim_study(spark, studies)
            replace_table(spark, s, dim, s.gold("dim_study"))
            info.update(rows_in=studies.count(), rows_out=dim.count())

    with ctx.stage("gold", "dim_site") as info:
        if sites is None:
            info.update(status="skipped", message="no silver sites")
        else:
            existing = read_table(spark, s, s.gold("dim_site")) if table_exists(spark, s, s.gold("dim_site")) else None
            effective = sites.agg(F.max("_batch_date")).first()[0]
            merged = scd2_merge(spark, existing, sites, effective)
            replace_table(spark, s, merged, s.gold("dim_site"))
            # Count from the table that was just written. `merged` is lazy and reads the OLD
            # dim_site files, which replace_table has just swapped out; evaluating it again fails.
            info.update(rows_in=sites.count(), rows_out=read_table(spark, s, s.gold("dim_site")).count())

    with ctx.stage("gold", "dim_participant") as info:
        if people is None:
            info.update(status="skipped", message="no silver participants")
        else:
            changed = rows_gold_has_not_seen(
                spark, s, people, s.gold("dim_participant"), "participant_pk", full_refresh
            )
            new_rows = build_dim_participant(spark, changed)
            if not table_exists(spark, s, s.gold("dim_participant")):
                new_rows = new_rows.unionByName(
                    unknown_member(
                        spark, new_rows.schema, participant_key=UNKNOWN_KEY, participant_pk="UNKNOWN", sex="Unknown"
                    )
                )
            rows = new_rows.count()
            if rows:
                upsert(spark, s, new_rows, s.gold("dim_participant"), ["participant_key"], ["updated_at"])
            info.update(rows_in=rows, rows_out=rows)

    dim_study = read_table(spark, s, s.gold("dim_study")) if table_exists(spark, s, s.gold("dim_study")) else None
    dim_site = read_table(spark, s, s.gold("dim_site")) if table_exists(spark, s, s.gold("dim_site")) else None
    dim_participant = (
        read_table(spark, s, s.gold("dim_participant")) if table_exists(spark, s, s.gold("dim_participant")) else None
    )

    with ctx.stage("gold", "fact_application") as info:
        if apps is None or dim_study is None or dim_site is None:
            info.update(status="skipped", message="missing inputs")
        else:
            changed = rows_gold_has_not_seen(spark, s, apps, s.gold("fact_application"), "application_id", full_refresh)
            fact = build_fact_application(changed, dim_study, dim_site, s.sla_days)
            # Count before writing: `changed` reads the gold table, whose files the upsert swaps out.
            rows_in = changed.count()
            rows = fact.count()
            if rows:
                upsert(spark, s, fact, s.gold("fact_application"), ["application_key"], ["updated_at"])
            info.update(rows_in=rows_in, rows_out=rows)

    with ctx.stage("gold", "fact_enrolment") as info:
        if enrolments is None or dim_study is None or dim_site is None or dim_participant is None:
            info.update(status="skipped", message="missing inputs")
        else:
            changed = rows_gold_has_not_seen(
                spark, s, enrolments, s.gold("fact_enrolment"), "enrolment_id", full_refresh
            )
            fact = build_fact_enrolment(changed, dim_study, dim_site, dim_participant)
            rows_in = changed.count()
            rows = fact.count()
            if rows:
                upsert(spark, s, fact, s.gold("fact_enrolment"), ["enrolment_key"], ["updated_at"])
            info.update(rows_in=rows_in, rows_out=rows)

    with ctx.stage("gold", "mart_monthly_performance") as info:
        if all(table_exists(spark, s, s.gold(t)) for t in ["fact_application", "fact_enrolment"]):
            mart = build_mart_monthly(
                read_table(spark, s, s.gold("fact_application")), read_table(spark, s, s.gold("fact_enrolment"))
            )
            replace_table(spark, s, mart, s.gold("mart_monthly_performance"))
            info.update(rows_out=mart.count())
        else:
            info.update(status="skipped", message="facts not built yet")
