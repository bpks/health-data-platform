# Infrastructure (Terraform)

Defines one environment's worth of Azure resources: resource group, ADLS Gen2 lake, Key Vault, Azure SQL
(serverless in dev and test), Data Factory (used for its managed identity and diagnostics here), Log Analytics,
three alert rules and a budget. One configuration, three variable files (`envs/dev.tfvars`, `test.tfvars`,
`prod.tfvars`), one remote state file per environment.

**Status: written and statically checked, never applied.** No Terraform binary or Azure subscription was
available when this was written. `scripts/check_terraform_static.py` parses every file and cross-checks variables
and references, and CI runs `terraform fmt -check` and `terraform validate`. The first real `plan` may still
need small corrections. Every identifier in `envs/*.tfvars` is a placeholder (all-zero or sequential GUIDs, example addresses).

## One-time bootstrap (outside this configuration)

1. **State storage.** Create a resource group and a storage account with a `tfstate` container for remote
   state, with shared-key access disabled. It is deliberately not managed here: a configuration should not
   own the state that tracks it.
2. **CI identity.** Create an Entra application (or a user-assigned managed identity) with a federated
   credential for this repository, one per GitHub Environment (`dev`, `test`, `prod`). Grant it
   `Contributor` and `User Access Administrator` at **subscription** scope (Terraform creates the resource
   group itself, and the second role is needed because the configuration creates role assignments), and
   `Storage Blob Data Contributor` on the state container.
   **Known gap:** shared-key access is disabled on the lake and the provider uses Entra authentication, so
   creating the lake container goes through the data plane, which needs a Storage Blob role for the identity
   running Terraform on the lake account. The configuration does not grant that to itself, and the account does
   not exist before the first apply, so the first apply will likely fail at that step. The fix is either a role
   assignment for the deploying identity in the configuration or `storage_account_id` on the container (check
   the minimum azurerm 4.x version that supports it). Not run, so expect to adjust.
3. **GitHub Environments.** Create `dev`, `test` and `prod` (with the variables listed at the top of
   `.github/workflows/terraform-env.yml`, and *no* protection rules) and two extra environments,
   `test-approval` and `prod-approval`, with required reviewers. The workflow plans first, then waits on the
   `-approval` environment, then applies the saved plan, so the reviewer approves after reading the plan, not
   before it exists.
4. **Switch deployment on.** The deploy workflow skips every job until the repository variable `DEPLOY_ENABLED` is set to
   `true` (Settings, Secrets and variables, Actions, Variables), so a fresh fork does not fail for lack of credentials.
5. **Fill in the placeholders** in `envs/*.tfvars`: subscription id, owner and cost centre, the engineer and SQL admin group ids, alert addresses and the budget start date. The tenant id is a GitHub Environment variable, not a Terraform one.

## Day to day

Open a pull request: CI plans **dev** only and posts the plan to the job summary. Merging to `main` plans and
applies dev, then plans test and waits for approval before applying, then the same for prod. Read the plan: it
is the change record, and a plan that destroys something nobody intended is a reason to stop.

The first `terraform init` will create `.terraform.lock.hcl`. Commit it, so provider versions are pinned.

To run by hand:

```bash
cd infra/terraform
terraform init -backend-config="resource_group_name=<rg>" -backend-config="storage_account_name=<sa>" \
  -backend-config="container_name=tfstate" -backend-config="key=dev.tfstate" -backend-config="use_azuread_auth=true"
terraform plan -var-file=envs/dev.tfvars -var="subscription_id=<subscription id>"
```

## After the first apply

The pseudonymisation salt is **not** a Terraform resource, because that would write it into state. A platform
administrator sets it directly:

```bash
az keyvault secret set --vault-name <name of the vault in the key_vault_uri output> --name hdp-pseudo-salt --value "<long random value>"
```

Treat rotating it as an approved change, not routine: it re-keys every participant.
See `docs/security-and-governance.md`.
