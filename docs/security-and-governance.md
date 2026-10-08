# Security and governance

This is a demonstration of technical controls on **synthetic data**. It is not legal advice and it is not a
compliance claim. A real deployment also needs a data protection impact assessment, a documented lawful
basis, retention schedules agreed with the data owner, and an information-security review.

## Zero Trust principles, and where each shows up

| Principle | Implementation here |
|---|---|
| Verify explicitly | Identity is Entra ID everywhere: the lake has shared-key access disabled, SQL is Entra-only (no SQL logins, so no SQL passwords), CI authenticates with OIDC federation (no stored secret), Key Vault uses RBAC |
| Least privilege | Separate roles for analysts, stewards, ETL and ops (`sql/ddl/001_schemas_and_roles.sql`). Engineers get read-only access to secrets (`Key Vault Secrets User`). Analysts have no access to the lake at all |
| Assume breach | Raw identifiers exist only in the restricted zone (landing and bronze); downstream layers hold pseudonyms and generalised values, so a leak of silver or gold is far less harmful. Audit logs for the lake and Key Vault, and error, timeout, blocking and deadlock logs for SQL, go to a central workspace (SQL auditing of logins and queries is not configured). Soft delete (14 days on the lake, 90 on Key Vault) limits damage from deletion |

## Access model (RBAC)

| Principal | Lake (container) | Key Vault | SQL serving layer |
|---|---|---|---|
| Pipeline identity (Data Factory managed identity) | Storage Blob Data Contributor | Secrets User | `role_etl` |
| Platform engineers (Entra group) | Storage Blob Data Contributor | Secrets User | via `role_ops_readonly` or admin group as needed |
| Analysts (Entra group) | none | none | `role_analyst`: region-filtered rows on the facts and the mart, masked participant columns |
| Data stewards (Entra group) | none | none | `role_data_steward`: all regions, `UNMASK` |
| Platform administrators | via approval | set/rotate secrets (just-in-time) | SQL admin group |

Access is granted to **groups**, never to individuals, so joiners and leavers are handled in one place.

## Data classification by zone

| Zone | Classification | Why |
|---|---|---|
| Landing, bronze | Restricted | Raw direct identifiers and full dates of birth |
| Silver, quarantine, gold | Sensitive | Pseudonymised and generalised, but still personal data |
| Serving layer | Sensitive, masked | Row-level security and masking on top |

## Privacy techniques, honestly described

| Technique | What it does | What it does **not** do |
|---|---|---|
| **Pseudonymisation** (salted SHA-256 of the participant reference) | Removes the direct identifier from analytic layers; the same person gets the same pseudonym everywhere, so joins still work | The output is still **personal data**. A ten-digit identifier has only 10^10 possibilities, so the **salt must stay secret**: with it, anyone can recompute pseudonyms for a list of candidate identifiers. The salt lives in Key Vault, the pipeline refuses to start when `HDP_ENV` is not `local` and no salt is set (it checks the environment name, not the storage path), and rotating it is a controlled change (it re-keys every participant) |
| **Generalisation** (date of birth to birth year, postcode to outward code) | Data minimisation: the analytic layers never hold the precise values | A birth year plus an outward code plus a rare ethnicity can still single someone out in a small area; small-number suppression for published statistics is out of scope |
| **Dynamic data masking** (SQL) | Hides sensitive columns in query results for non-stewards | It is **not** an access boundary. A user with `SELECT` can infer masked values by filtering on them. It is a belt-and-braces layer on top of the controls above |
| **Row-level security** (SQL) | The database engine filters rows by the user's region on every query, view and BI tool | It protects rows, not columns, and it only works as well as the `security.region_access` table is governed. It is applied to `fact_application`, `fact_enrolment` and the monthly mart; the dimensions are not region-filtered (the participant dimension is masked instead) |
| **Quarantine built from cleaned columns** | Rejected rows do not reintroduce raw values | Quarantine still holds pseudonymised personal data and is access-controlled like silver |

`sql/tests/002_rls_and_masking_checks.sql` impersonates test users and asserts the filtering and masking
actually behave as described. It needs a SQL Server or Azure SQL instance to run.

## UK GDPR: controls demonstrated, mapped to principles

| Principle / article | Control |
|---|---|
| Data minimisation (Art. 5(1)(c)) | Date of birth and full postcode are dropped in silver; only what analysis needs reaches gold |
| Integrity and confidentiality (Art. 5(1)(f), 32) | Entra-only authentication, RBAC, TLS 1.2+, encryption at rest (platform default), pseudonymisation, masking, audit logs for the lake and Key Vault |
| Accuracy (Art. 5(1)(d)) | Data quality rules with quarantine; reconciliation checks as a script to run after each load (nothing schedules it yet) |
| Storage limitation (Art. 5(1)(e)) | Log retention is a variable per environment; **lake retention and erasure workflows are not implemented** and would need to be agreed with the data owner |
| Accountability (Art. 5(2)) | Everything is code reviewed through pull requests; infrastructure changes have a plan and an approval gate; access and secret reads are logged |
| Pseudonymisation (Art. 4(5), 25, 32) | See above, including its limits |

**Not covered:** data subject rights handling (access, erasure, rectification) across the layers, breach
notification procedure, international transfer assessment, and a retention/disposal schedule. These are
organisational as much as technical.

## Secrets

- The pseudonymisation salt (`hdp-pseudo-salt`) is set by a platform administrator with
  `az keyvault secret set`. It is deliberately not a Terraform resource, which would write it into state.
- It reaches the pipeline as the `HDP_PSEUDO_SALT` environment variable (from a Key Vault-backed secret scope). The code does not log it, and it is excluded from the settings object's printed form; error text from Spark is not scrubbed, so treat run logs as sensitive.
- CI runs a secret scan (gitleaks) on every pull request and every push to `main`. `.gitignore` excludes `.env`, keys and Terraform state.
- No credential of any kind is stored in this repository; the placeholder identifiers in `infra/terraform/envs/`
  are all-zero or sequential placeholder values and obviously fake.

## Known weaknesses of this demo

- The firewall default in `dev` is `Allow` (no private endpoints are provisioned); test and prod default to `Deny`
  but need private endpoints and a self-hosted runner before they are usable.
- Folder-level ACLs inside the lake (for example making `bronze/` readable only by the pipeline identity) are
  not set; access is at container level.
- There is no automated access review, no break-glass procedure and no key rotation job for the salt.
