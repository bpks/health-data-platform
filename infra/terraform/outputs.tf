output "resource_group_name" {
  description = "Resource group holding the platform."
  value       = azurerm_resource_group.this.name
}

output "lake_base_path" {
  description = "Value for HDP_BASE_PATH."
  value       = "abfss://${azurerm_storage_container.lake.name}@${azurerm_storage_account.lake.name}.dfs.core.windows.net"
}

output "key_vault_uri" {
  description = "Key Vault holding the pseudonymisation salt (secret name: hdp-pseudo-salt)."
  value       = azurerm_key_vault.this.vault_uri
}

output "log_analytics_workspace_id" {
  description = "Workspace receiving platform logs; scope for the KQL queries in monitoring/kql."
  value       = azurerm_log_analytics_workspace.this.id
}

output "data_factory_name" {
  description = "Orchestrator; its managed identity has lake and Key Vault access."
  value       = azurerm_data_factory.this.name
}

output "sql_server_fqdn" {
  description = "Serving-layer SQL server."
  value       = azurerm_mssql_server.this.fully_qualified_domain_name
}

output "sql_database_name" {
  description = "Serving-layer database; deploy sql/ddl, sql/procs and sql/security to it in that order."
  value       = azurerm_mssql_database.serving.name
}
