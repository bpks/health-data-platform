data "azurerm_client_config" "current" {}

locals {
  name = "${var.project}-${var.environment}"

  # Globally unique names derive a stable suffix from the subscription, so names are
  # deterministic (no random provider, no churn between plans).
  suffix               = substr(md5(var.subscription_id), 0, 6)
  storage_account_name = lower("st${var.project}${var.environment}${local.suffix}")
  key_vault_name       = "kv-${local.name}-${local.suffix}"
  sql_server_name      = "sql-${local.name}-${local.suffix}"

  sql_serverless = startswith(var.sql_sku_name, "GP_S_")

  tags = merge(
    {
      project             = var.project
      environment         = var.environment
      owner               = var.owner
      cost_centre         = var.cost_centre
      data_classification = "sensitive"
      managed_by          = "terraform"
    },
    var.extra_tags
  )
}

resource "azurerm_resource_group" "this" {
  name     = "rg-${local.name}"
  location = var.location
  tags     = local.tags
}

# Production resources cannot be deleted by accident; remove the lock deliberately first.
resource "azurerm_management_lock" "resource_group" {
  count = var.environment == "prod" ? 1 : 0

  name       = "lock-${local.name}-no-delete"
  scope      = azurerm_resource_group.this.id
  lock_level = "CanNotDelete"
  notes      = "Managed by Terraform. Remove deliberately, with change approval, before destroying production."
}

# --- data lake ----------------------------------------------------------------------------------------

resource "azurerm_storage_account" "lake" {
  name                = local.storage_account_name
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location

  account_tier             = "Standard"
  account_kind             = "StorageV2"
  account_replication_type = var.storage_replication
  is_hns_enabled           = true # hierarchical namespace: the Data Lake Gen2 file system

  min_tls_version                 = "TLS1_2"
  https_traffic_only_enabled      = true
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = false # Entra ID only: no account keys to leak
  default_to_oauth_authentication = true

  blob_properties {
    delete_retention_policy {
      days = 14
    }
    container_delete_retention_policy {
      days = 14
    }
  }

  network_rules {
    default_action = var.network_default_action
    bypass         = ["AzureServices"]
    ip_rules       = var.allowed_ip_ranges
  }

  tags = local.tags
}

# One container holds the zones as folders (landing, bronze, silver, gold, quarantine, _control),
# matching HDP_BASE_PATH = abfss://lake@<account>.dfs.core.windows.net. Analysts have no access to the
# lake at all: they query the SQL serving layer, where masking and row-level security apply.
resource "azurerm_storage_container" "lake" {
  name                  = "lake"
  storage_account_name  = azurerm_storage_account.lake.name
  container_access_type = "private"
}

# --- secrets --------------------------------------------------------------------------------------------

resource "azurerm_key_vault" "this" {
  name                = local.key_vault_name
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"

  rbac_authorization_enabled = true # permissions are Azure RBAC roles, not legacy access policies
  purge_protection_enabled   = var.environment == "prod"
  soft_delete_retention_days = 90

  network_acls {
    default_action = var.network_default_action
    bypass         = "AzureServices"
    ip_rules       = var.allowed_ip_ranges
  }

  tags = local.tags
}

# The pseudonymisation salt (HDP_PSEUDO_SALT) is set out-of-band by a platform administrator:
#   az keyvault secret set --vault-name <kv> --name hdp-pseudo-salt --value "<random 32+ bytes>"
# It is deliberately NOT a Terraform resource, because that would write the secret into state.

# --- orchestration identity -------------------------------------------------------------------------------

resource "azurerm_data_factory" "this" {
  name                = "adf-${local.name}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}

# --- access (RBAC, least privilege) ----------------------------------------------------------------------------

resource "azurerm_role_assignment" "engineers_lake" {
  scope                = azurerm_storage_container.lake.resource_manager_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = var.data_engineer_group_object_id
}

resource "azurerm_role_assignment" "pipeline_lake" {
  scope                = azurerm_storage_container.lake.resource_manager_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.this.identity[0].principal_id
}

resource "azurerm_role_assignment" "engineers_secrets" {
  scope                = azurerm_key_vault.this.id
  role_definition_name = "Key Vault Secrets User" # read secrets; cannot create or change them
  principal_id         = var.data_engineer_group_object_id
}

resource "azurerm_role_assignment" "pipeline_secrets" {
  scope                = azurerm_key_vault.this.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_data_factory.this.identity[0].principal_id
}

# --- serving layer ------------------------------------------------------------------------------------------------

resource "azurerm_mssql_server" "this" {
  name                = local.sql_server_name
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  version             = "12.0"

  minimum_tls_version           = "1.2"
  public_network_access_enabled = var.sql_public_network_access

  azuread_administrator {
    login_username              = var.sql_admin_group_name
    object_id                   = var.sql_admin_group_object_id
    azuread_authentication_only = true # no SQL logins, so no SQL passwords to manage or leak
  }

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}

resource "azurerm_mssql_database" "serving" {
  name      = "sqldb-${local.name}-serving"
  server_id = azurerm_mssql_server.this.id

  sku_name                    = var.sql_sku_name
  max_size_gb                 = var.sql_max_size_gb
  min_capacity                = local.sql_serverless ? var.sql_min_capacity : null
  auto_pause_delay_in_minutes = local.sql_serverless ? var.sql_auto_pause_delay_minutes : null

  storage_account_type = var.environment == "prod" ? "Geo" : "Local" # backup redundancy

  short_term_retention_policy {
    retention_days = var.sql_pitr_days
  }

  tags = local.tags
}
