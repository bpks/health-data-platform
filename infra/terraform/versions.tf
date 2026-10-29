terraform {
  required_version = ">= 1.6.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 5.11"
    }
  }

  # Remote state in a storage account that is created once, outside this configuration.
  # The settings are supplied at init time (see infra/terraform/README.md and the deploy workflow):
  #   terraform init -backend-config="resource_group_name=..." -backend-config="key=dev.tfstate" ...
  backend "azurerm" {}
}
