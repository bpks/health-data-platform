# Operations runbook

How the platform is run: what to do when an alert fires, how incidents, changes and problems are handled,
and what out-of-hours cover can and cannot do. Targets in this document are **illustrative defaults** to be
agreed with the service owner; they are not commitments made by anyone.

## Service levels (illustrative)

| Severity | Meaning | Respond within | Update every | Target resolution |
|---|---|---|---|---|
| **1: Critical** | Platform down, or a data-protection incident (suspected exposure of personal data) | 30 min, 24/7 | 30 min | 4 hours |
| **2: High** | Daily load failed or stale; reports wrong or late for many users | 1 hour in hours, 2 hours out of hours | 2 hours | 1 working day |
| **3: Medium** | One entity or report degraded; workaround exists | 4 working hours | Daily | 3 working days |
| **4: Low** | Question, cosmetic issue, access request | 1 working day | On change | 5 working days |

Service requests (access, a new report column) are tracked separately from incidents and follow their own,
longer targets.

## What alerts exist and what they mean

| Alert | Alert severity | Query | Fires when |
|---|---|---|---|
| `pipeline-failed` | 1 | `monitoring/kql/alert_pipeline_failed.kql` | Any pipeline run ends `Failed` |
| `pipeline-stale` | 2 | `monitoring/kql/alert_pipeline_stale.kql` | No successful run of the Data Factory pipeline named `pl_hdp_daily` in 26 hours (catches silent failure). Assumes that pipeline exists: its definition is not in this repository, so until one with that name runs, this alert fires permanently |
| `sql-contention` | 2 | `monitoring/kql/alert_sql_contention.kql` | Deadlocks or blocking on the serving database |
| Budget | n/a | `azurerm_consumption_budget_resource_group` | 80% of budget actual, or 100% forecast |

*Alert severity* (how loudly Azure Monitor notifies) is set per rule; *incident severity* (the table above) is decided by a person from the impact. A failed-run alert starts at alert severity 1 so it is seen quickly, and the incident is then classified on what is actually affected. Both alert rules read Data Factory run history only; a Databricks job triggered directly (see `databricks.yml`) emails on failure but does not raise these alerts.

Quarantine volume is a data-quality signal rather than an alert: each run logs `rows_quarantined` per entity
to `_control/run_log`. A sudden jump usually means an upstream change, not a platform fault.

## First response: pipeline failed

1. **Acknowledge** the alert and note the time. Open an incident record (severity 2 unless data is wrong in
   production reports, then 1).
2. **Find the failing stage.** The run log names it: `stage`, `entity`, `status = failed`, and the first 500
   characters of the error in `message`. Or run `monitoring/kql/pipeline_runs_daily_summary.kql`.
3. **Classify the cause:**
   - *Transient* (cluster start failure, throttling, a timeout): re-run. The pipeline is idempotent, so a
     re-run is safe: bronze skips batches it already ingested, silver upserts converge, and gold loads whatever it has not yet caught up with, so a run that stopped part-way is recovered by running again. (`--full-refresh` rebuilds silver and gold from all of bronze if you ever need it, but it re-appends any still-failing rows to quarantine, so de-duplicate when reporting on it.)
   - *Data* (schema drift in a landing file, a new status value): look at `quarantine` for the entity and at the
     rule failures in the run log message. Do **not** loosen a rule to make the alert go away.
   - *Code or config change* (the failure began after a deployment): check the last merged pull request and the
     last Terraform apply before anything else.
   - *Access* (expired credential, role removed): check Key Vault and storage audit logs
     (`key_vault_access_audit.kql`, `lake_delete_audit.kql`).
4. **Communicate.** Tell stakeholders what is affected (which reports are stale, as of when) and when the next
   update is due, even if the answer is "still investigating".
5. **Recover**, confirm the three post-conditions below, then close the incident and record the cause.

Post-conditions before closing: the run completed with no `failed` rows; `sql/tests/001_reconciliation_checks.sql`
passes; the stale-pipeline alert is clear.

## First response: pipeline stale (no failure, no success)

Nothing errored, so look at what *should* have started it: trigger disabled or paused, an upstream extract that
never landed (check `landing/batch_date=*` for today), an expired managed-identity role. Re-run manually once
the cause is fixed.

## First response: SQL contention

1. Run `sql/ops/04_blocking_and_waits.sql` to see the blocking chain and statements.
2. Check whether the nightly merge (`dw.usp_merge_*`) overlapped a report refresh: `ops.load_log` has start and
   end times.
3. Do **not** kill a session on your own. Ask the incident lead: killing a session mid-merge rolls back its
   transaction, which is safe but can take as long as the work already done.
4. For a recurring pattern, raise a problem record (below): the fix is usually scheduling, an index, or a
   snapshot isolation setting, and each of those is a change.

## Incident management

```
detect (alert / user report) -> triage (severity, owner) -> contain -> diagnose -> fix or workaround
        -> verify -> communicate -> close -> review (root cause analysis)
```

- **Incident lead:** one named person owns coordination and communication; engineers work the fix.
- **Containment before diagnosis** when personal data may be exposed: revoke the access or disable the
  export first, then investigate. Report a suspected personal-data breach to the data protection officer
  immediately; the legal clock for notification is short.
- **Safe without approval:** re-run an idempotent pipeline, read logs and run the read-only `sql/ops` scripts,
  pause a trigger.
- **Needs approval:** killing sessions, restoring from backup, altering production data, changing access,
  anything that deletes.

### Root cause analysis template

```
Incident:           <id, severity, start/end, duration, who was affected>
Timeline:           <UTC timestamps: first symptom, detection, response, mitigation, resolution>
Impact:             <what users saw; data affected; reports stale for how long>
Detection:          <how we found out; how long it took; could the alert have fired sooner?>
Root cause:         <the underlying cause, not the trigger. Keep asking "why" until it is a
                     system or process gap rather than a person>
Contributing:       <conditions that made it worse or harder to find>
What went well:     <...>
Corrective actions: <fix the cause: owner, due date, ticket>
Preventive actions: <stop the class of problem: test, alert, runbook, guard-rail; owner, due date>
```

A root cause analysis assigns no blame. If the answer is "someone made a mistake", the real cause is the
missing guard-rail that let one mistake reach production.

## Change management

| Type | Examples | Process |
|---|---|---|
| **Standard** (pre-approved, low risk, repeatable) | Dependency patch via Dependabot, adding a DQ warning rule, a KQL query tweak | Pull request, CI green, one review; no change window |
| **Normal** | New pipeline logic, SQL schema change, Terraform change in test or prod | Pull request with the risk/rollback section completed, CI green, Terraform plan reviewed, environment approval for test and prod |
| **Emergency** | Fix for a severity 1 incident | Fix first with the incident lead's verbal approval, pull request raised straight afterwards, reviewed retrospectively within one working day |

Infrastructure changes always go through `deploy-infra`: dev is planned on the pull request and applied on
merge with no approval; then test, and only after it, prod, are each planned and wait for a reviewer, who reads
the plan before approving, and the saved plan is what gets applied. The plan is the change record: it states exactly what
will be created, changed or destroyed. **A plan that destroys something nobody intended is a stop sign.**
Production is also protected by a `CanNotDelete` resource lock.

## Problem management

An incident is fixed when service is restored. A **problem** is fixed when it cannot recur.

- Open a problem record when the same alert fires three times in a month, when an RCA finds a systemic gap,
  or when a workaround is being used repeatedly.
- Track it as a known error with its workaround until the permanent fix ships, so whoever is on call at 3am
  finds the workaround in minutes.
- Review open problems monthly; close one only when its corrective action is deployed and verified.

## Out-of-hours cover

- Alerts of severity 1 page the on-call engineer; severity 2 notify by email and are picked up at start of
  business unless the on-call engineer judges the impact warrants waking someone. (The action group in
  Terraform sends email; routing to a paging service is an environment-specific addition.)
- The on-call engineer can do everything under "Safe without approval" above. Anything else is escalated to the
  incident lead.
- **Handover:** end of each on-call week, a short note covering open incidents, known errors in play, changes
  scheduled in the next week, and anything that felt fragile.
- Every out-of-hours call is recorded and reviewed in the weekly operations meeting: a call that could have
  waited until morning is a candidate for a better alert threshold; a call that should have come sooner is a
  candidate for a new alert.

## Capacity, cost and housekeeping calendar

| Cadence | Task | Where |
|---|---|---|
| Daily | Confirm last night's run and reconciliation | `pipeline_runs_daily_summary.kql`, `sql/tests/001_reconciliation_checks.sql` |
| Weekly | Index and columnstore health, statistics freshness, missing-index review | `sql/ops/01`, `02`, `06` |
| Weekly | Top queries by CPU, regressions against the previous week | `sql/ops/03_query_store_top_queries.sql` |
| Monthly | Capacity and growth review, Log Analytics ingestion by data type | `sql/ops/05`, `log_ingestion_cost.kql` |
| Monthly | Review open problems, access groups, budget versus actual | n/a |
| Quarterly | Restore test: recover the SQL database to a point in time, and a deleted lake folder from soft delete | n/a |
| Quarterly | Review who can read the pseudonymisation salt. Rotating it is a deliberate, approved change, never routine: it re-keys every participant and every downstream join | n/a |
