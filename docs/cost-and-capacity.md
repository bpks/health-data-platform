# Cost and capacity

What this repository does about cloud cost and capacity, what it measured locally, and, just as important,
what it cannot measure. **Nothing here is a cloud bill.** The pipeline was run on one laptop-sized VM against
Parquet files, so the figures below show *shape* (how much work an incremental load avoids), not pounds.

## What was measured

The two-day demo (`make demo`) generates a full historical load and then a next-day extract, and runs the
whole pipeline over each. Row counts come straight from the run log (`_control/run_log`).

| | Day 1 (full load) | Day 2 (incremental) | Day 2 as a share of day 1 |
|---|---:|---:|---:|
| Rows read from landing into bronze | 233,654 | 9,243 | **4.0%** |
| Silver rows written (after de-duplication and quarantine) | 228,893 | 9,242 | 4.0% |
| Rows quarantined | 3,232 | 1 | n/a |
| Gold fact rows upserted (changed rows only) | 167,484 | 8,499 | **5.1%** |
| Sum of per-stage timings, excluding session start-up (seconds) | 72.5 | 58.0 | 80% |

Reading the table honestly:

- **Rows processed fell by about 95%** between a full and an incremental run. That is the number that matters
  for cost on a platform that bills for compute time and data scanned, because the bronze, silver and fact
  work scales with changed rows, not with table size. (The dimensions and the mart are rebuilt every run, which
  is cheap here but is not incremental.) It is the mechanism behind incremental-load savings, shown on synthetic
  data. It is **not** a reproduction of any real saving; this repository makes no claim about pounds.
- **Run time fell by only about 20%.** Reasons worth knowing: the dimensions and the mart are rebuilt on every
  run, the first Spark job of a run pays a warm-up cost that does not shrink with the data, and the local
  **Parquet fallback rewrites a whole table on every upsert** because plain Parquet has no `MERGE`. On Delta, which this pipeline is designed
  for, the upsert touches only the affected files, so the time saving should move much closer to the row saving.
  That Delta path is written but **was not run here**, so treat the expectation as a hypothesis, not a result.
- Timings are the *sum* of per-stage durations from the run log on a two-core local session. They are not
  comparable to a cluster, and a single run of each day is not a benchmark.

## Where cost comes from, and the levers pulled

| Cost driver | Lever in this repository | Where |
|---|---|---|
| Compute re-processing everything daily | Watermark per entity plus idempotent upserts: only new or changed rows move through silver and gold | `src/hdp/silver.py`, `lake.py` |
| Always-on clusters | Job cluster that exists only for the run, **autoscaling** 1 to 4 workers, **spot capacity with on-demand fallback** | `databricks.yml` |
| SQL compute when nobody is querying | **Serverless** Azure SQL with auto-pause in dev and test (60 and 120 minutes); provisioned only in prod, where predictable latency is worth the money | `infra/terraform/envs/*.tfvars` |
| Log ingestion growing unnoticed | Log Analytics **daily quota** per environment (1, 2, 5 GB), retention set per environment (30, 60, 90 days), and a query that shows ingestion by data type | `monitoring.tf`, `monitoring/kql/log_ingestion_cost.kql` |
| Storage redundancy you do not need | **LRS** replication in dev and test, **GZRS** only in prod | `envs/*.tfvars` |
| Surprise bills | A **budget per environment** alerting at 80% of actual spend and at 100% of *forecast* spend, so the warning comes before the money is gone | `monitoring.tf` |
| Over-indexing and unused structures | Missing-index review and index and columnstore health checks as weekly routine rather than reactive tuning | `sql/ops/01`, `06` |
| Slow regressions | Query Store top-queries review and a KQL query comparing pipeline duration with its recent baseline | `sql/ops/03`, `pipeline_duration_regression.kql` |

The environment sizing values (budgets of 150, 300 and 1,200 GBP a month; the SKUs; the quotas) are
**placeholders chosen to show the pattern**. They were not derived from a real workload or a price list, and
should be replaced after a sizing exercise.

## Capacity management

Capacity work is making sure the platform has room for next quarter, found out before it is an incident.

| Question | How it is answered | Cadence |
|---|---|---|
| How big is each table, and how fast is it growing? | `sql/ops/05_capacity_and_growth.sql` part 1 (size per table) and part 3 (rows loaded per month from `ops.load_log`) | Monthly |
| Is the transaction log healthy? | Same script, part 2. A log that stays near full usually means an open transaction or a stalled log backup: find the cause before growing it | Monthly, and when blocked |
| Is the lake growing as expected? | Bronze is append-only, so it grows with every batch; compare its size month on month | Monthly |
| Is log ingestion about to hit the quota? | `log_ingestion_cost.kql`: ingestion by data type and day | Monthly |
| Are loads getting slower? | `pipeline_duration_regression.kql` | Weekly |

A back-of-envelope view of growth using the demo's own numbers: day 1 loaded 233,654 bronze rows and the one
next-day extract added 9,243, about 4% of the initial size. That is a single synthetic day, and the real growth
rate depends entirely on the source systems, so this number should not be quoted as one.

## What is not covered

- No real cost data was collected, so there is no cost model, no price comparison between Fabric capacity and
  Databricks, and no claim about any percentage saving.
- Fabric capacity (F-SKU) sizing and smoothing, Databricks pool and cluster-policy tuning, and Reserved
  Instance or savings-plan analysis are not demonstrated.
- Lifecycle rules that move old bronze files to a cooler storage tier are not configured; they would need a
  retention decision from the data owner first.
