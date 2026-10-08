"""Business measure definitions, written once.

Both fact tables and the monthly mart use these functions, so every consumer
(however many teams read the data) gets one definition of "decided",
"approved" and "within target". The T-SQL serving layer reads the precomputed
columns and does not re-implement them.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

from hdp.model import DECIDED_STATUSES


def is_decided(status: Column) -> Column:
    return status.isin(*DECIDED_STATUSES)


def is_approved(status: Column) -> Column:
    return status == F.lit("Approved")


def days_to_decision(submitted: Column, decided: Column) -> Column:
    return F.when(decided.isNotNull(), F.datediff(decided, submitted))


def met_sla(days: Column, sla_days: int) -> Column:
    """True/False once a decision exists, NULL while the application is still open."""
    return F.when(days.isNotNull(), days <= F.lit(sla_days))


def age_band(age: Column) -> Column:
    return (
        F.when(age.isNull(), F.lit("Unknown"))
        .when(age < 18, F.lit("0-17"))
        .when(age < 30, F.lit("18-29"))
        .when(age < 45, F.lit("30-44"))
        .when(age < 60, F.lit("45-59"))
        .when(age < 75, F.lit("60-74"))
        .otherwise(F.lit("75+"))
    )
