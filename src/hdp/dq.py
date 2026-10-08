"""Small, dependency-free data quality framework.

A `Rule` is a named boolean expression that is True when a row is acceptable.
Severity decides what happens to a row that fails:

* ``error``   - the row is quarantined (kept, with the rules it broke, but not loaded)
* ``warning`` - the row is loaded and the rule name is recorded in ``dq_warnings``

NULL counts as a failure, so "not null" is implied by every rule. Columns whose names
start with ``_chk_`` are helper flags: rules may use them, and they are dropped before
loading. Columns starting with ``_`` are audit columns and are kept out of the
quarantine payload.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class Rule:
    name: str
    passes: Column
    severity: str = "error"  # "error" | "warning"

    def __post_init__(self) -> None:
        if self.severity not in {"error", "warning"}:
            raise ValueError(f"severity must be 'error' or 'warning', got {self.severity!r}")


def _failed(rules: list[Rule]) -> Column:
    """Array of the names of rules the row failed (NULLs filtered out)."""
    if not rules:
        return F.array().cast("array<string>")
    flags = [F.when(~F.coalesce(r.passes, F.lit(False)), F.lit(r.name)) for r in rules]
    return F.filter(F.array(*flags), lambda x: x.isNotNull())


def apply_rules(df: DataFrame, rules: list[Rule]) -> tuple[DataFrame, DataFrame, dict[str, int]]:
    """Split `df` into (valid, quarantined, stats).

    valid       - rows with no error-level failures, with a ``dq_warnings`` array column added
    quarantined - rows with at least one error-level failure: entity payload as JSON, the failed
                  rule names, and the audit columns
    stats       - failures per rule plus ``_total`` and ``_quarantined`` counts
    """
    errors = [r for r in rules if r.severity == "error"]
    warnings = [r for r in rules if r.severity == "warning"]

    flagged = df.withColumn("_dq_errors", _failed(errors)).withColumn("dq_warnings", _failed(warnings))

    business_cols = [c for c in df.columns if not c.startswith("_")]
    audit_cols = [c for c in df.columns if c.startswith("_") and not c.startswith("_chk_")]
    helper_cols = [c for c in df.columns if c.startswith("_chk_")]

    valid = flagged.filter(F.size("_dq_errors") == 0).drop("_dq_errors", *helper_cols)
    quarantined = flagged.filter(F.size("_dq_errors") > 0).select(
        # keep NULLs in the payload: "study_id": null is exactly what an analyst needs to see
        F.to_json(F.struct(*[F.col(c) for c in business_cols]), {"ignoreNullFields": "false"}).alias("payload"),
        F.col("_dq_errors").alias("failed_rules"),
        *[F.col(c) for c in audit_cols],
    )

    aggregates = [F.count(F.lit(1)).alias("_total")]
    aggregates += [F.sum(F.when(~F.coalesce(r.passes, F.lit(False)), 1).otherwise(0)).alias(r.name) for r in rules]
    row = df.agg(*aggregates).first()
    stats = {k: int(v or 0) for k, v in row.asDict().items()}
    stats["_quarantined"] = quarantined.count()
    return valid, quarantined, stats


def duplicate_keys(df: DataFrame, keys: list[str]) -> int:
    """Number of key values that appear more than once (0 means the key is unique)."""
    return df.groupBy(*keys).count().filter(F.col("count") > 1).count()
