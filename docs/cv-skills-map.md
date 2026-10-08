# CV skills map

A portfolio project is a sample, not a replacement for experience. This page maps the capabilities listed on
the author's CV to where (if anywhere) this repository shows them, so a reader can tell what the project
demonstrates and what it does not.

Status key:

- **Ran**: the code was executed and tested here, with results recorded in the README.
- **Written**: implemented as code or configuration, but not run against the real service (no Azure subscription,
  SQL Server, Databricks workspace or Fabric tenant was available). The README's verification table says exactly what was checked.
- **Described**: covered in prose (runbook, security document) rather than code.
- **Not covered**: not in this repository. Absence here says nothing about the author's experience; it means this sample does not show it.

| CV capability | Where | Status |
|---|---|---|
| **Bronze / Silver / Gold on Delta** | `src/hdp/bronze.py`, `silver.py`, `gold.py`; `docs/architecture.md` | **Ran** on a Parquet fallback; the Delta `MERGE` branch is **Written**, not run |
| **Incremental loads, idempotent re-runs** | `src/hdp/lake.py` (watermarks, ingested-batch ledger, upsert); `tests/test_pipeline_e2e.py` | **Ran**: re-run changes nothing, stale record ignored, day 2 touches about 4% of rows |
| **Automated data quality (Python, pytest, SQL)** | `src/hdp/dq.py`, `silver.py` rules, 58 tests, `sql/tests/001_reconciliation_checks.sql` | Python and pytest **Ran**; the SQL checks are **Written** |
| **Kimball dimensional model, conformed dimensions, one set of measure definitions** | `gold.py`, `measures.py`, `docs/data-model.md` | **Ran**, including a test that every fact resolves to the site version valid on its date |
| **Slowly changing dimensions** | `dim_site` (type 2) | **Ran** |
| **PySpark and Python** | `src/hdp/` | **Ran** on Spark 3.5 and 4.x |
| **T-SQL, SQL Server and Azure SQL management and optimisation** | `sql/ddl`, `sql/procs`, `sql/ops` (index and columnstore health, statistics, Query Store, blocking, capacity, missing indexes) | **Written**: parsed and linted with sqlfluff (T-SQL dialect), **never executed** on SQL Server |
| **Row-level security, data masking** | `sql/security/`, `sql/tests/002_rls_and_masking_checks.sql` | **Written**, never executed on SQL Server |
| **Least-privilege RBAC, Zero Trust principles, Key Vault** | `infra/terraform/main.tf`, `sql/ddl/001_schemas_and_roles.sql`, `docs/security-and-governance.md` | **Written** and **Described** |
| **UK GDPR controls on sensitive data** | `src/hdp/privacy.py` (pseudonymisation, generalisation), `docs/security-and-governance.md` | Pseudonymisation **Ran** and is tested; the rest is **Described**. This is controls on synthetic data, not a compliance claim |
| **Platform as code with Terraform** | `infra/terraform/` | **Written**: formatting, `validate` and `plan` were **not run** (the binary was not available); static consistency checks passed, and CI runs `fmt` and `validate` |
| **GitHub Actions, dev / test / prod promotion with approval gates** | `.github/workflows/` | **Written**: YAML structure checked, **never run** on GitHub |
| **Databricks Asset Bundles** | `databricks.yml`, `notebooks/` | **Written** as an unvalidated template |
| **Monitoring, observability and alerting (Azure Monitor, Log Analytics)** | `monitoring/kql/`, `infra/terraform/monitoring.tf`, `src/hdp/observability.py` | Structured run log **Ran**; KQL and alert rules **Written**, not deployed |
| **Incident, request, change and problem management to SLAs; RCA; out-of-hours cover** | `docs/operations-runbook.md` | **Described**. Service levels are illustrative defaults, not commitments |
| **Capacity management and cloud cost monitoring** | `docs/cost-and-capacity.md`, `sql/ops/05`, budgets and quotas in Terraform | **Described** and **Written**; no real bill was measured |
| **Microsoft Fabric (Lakehouse, Warehouse, OneLake, Pipelines, Notebooks)** | `notebooks/run_pipeline_fabric_or_databricks.py`; the lake layout is OneLake-compatible | **Written** as a template; **never run** in Fabric. Fabric items as code are listed as the next step |
| **Azure Data Factory** | Provisioned (with a managed identity and diagnostic settings) in Terraform | **Written**; no pipeline definitions are included |
| **Azure Synapse** | none | **Not covered** |
| **Unity Catalog, Microsoft Purview** | none | **Not covered** |
| **Streaming: Azure Event Hubs, Spark Structured Streaming** | none | **Not covered**: this sample is daily batch |
| **Informatica, third-party APIs, legacy on-premises sources** | none | **Not covered** |
| **Snowflake** | none | **Not covered** |
| **AWS (certified 2019)** | none | **Not covered** |
| **PowerShell** | none | **Not covered** |
| **Azure DevOps pipelines** | none | **Not covered** (GitHub Actions only) |
| **Power BI, governed reporting** | The serving layer is designed for BI use, with analyst roles and RLS | **Not covered**: no report is included |
| **Agile / Scrum, stakeholder engagement, mentoring** | A pull-request template with a risk and rollback section | Largely **Not covered**: a repository cannot show this |

## Claims on the CV that this repository does not reproduce

The CV states outcomes from the author's professional work: pipeline counts, daily data volumes, and
reductions in compute cost and processing time. None of those is reproduced or evidenced here. The demo runs five source entities over a synthetic dataset of a few hundred
thousand rows. What it does show is the **mechanism** behind the incremental-load claim: on the synthetic data a
next-day run read about 4% of the rows a full load did. It does not show a saving in pounds, and the README
and `cost-and-capacity.md` say so.

## Independence of this project

This project is independent work using generated data. It contains no data, code, schema, configuration or
documentation from any employer or client of the author, and the subject domain (research study applications)
is a generic invention for the sake of the demonstration.
