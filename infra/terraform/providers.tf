provider "azurerm" {
  features {
    key_vault {
      # Do not purge soft-deleted vaults on destroy; recovery stays possible.
      purge_soft_delete_on_destroy = false
    }
  }

  subscription_id = var.subscription_id

  # The lake account has shared-key access disabled, so the provider itself must use Entra ID for
  # data-plane calls (creating containers). Authentication comes from the environment
  # (ARM_USE_OIDC / ARM_CLIENT_ID / ARM_TENANT_ID in CI), never from values in this repository.
  storage_use_azuread = true
}
