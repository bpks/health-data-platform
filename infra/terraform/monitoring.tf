# Observability as code: logs, alerts and cost guard-rails are deployed with the platform they watch,
# so a new environment is monitored from the first minute rather than "when someone remembers".

resource "azurerm_log_analytics_workspace" "this" {
  name                = "log-${local.name}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location

  sku               = "PerGB2018"
  retention_in_days = var.log_retention_days
  daily_quota_gb    = var.log_daily_quota_gb # cost guard: ingestion stops at the cap

  tags = local.tags
}

# --- diagnostics: send resource logs to the workspace -----------------------------------------------------------------

resource "azurerm_monitor_diagnostic_setting" "lake_blob" {
  name                       = "to-log-analytics"
  target_resource_id         = "${azurerm_storage_account.lake.id}/blobServices/default"
  log_analytics_workspace_id = azurerm_log_analytics_workspace.this.id

  enabled_log {
    category_group = "audit" # who read, wrote or deleted what
  }

  enabled_metric {
    category = "Transaction"
  }
}

resource "azurerm_monitor_diagnostic_setting" "key_vault" {
  name                       = "to-log-analytics"
  target_resource_id         = azurerm_key_vault.this.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.this.id

  enabled_log {
    category_group = "audit" # every secret access
  }
}

resource "azurerm_monitor_diagnostic_setting" "data_factory" {
  name                           = "to-log-analytics"
  target_resource_id             = azurerm_data_factory.this.id
  log_analytics_workspace_id     = azurerm_log_analytics_workspace.this.id
  log_analytics_destination_type = "Dedicated" # resource-specific tables such as ADFPipelineRun

  enabled_log {
    category = "PipelineRuns"
  }
  enabled_log {
    category = "ActivityRuns"
  }
  enabled_log {
    category = "TriggerRuns"
  }
}

resource "azurerm_monitor_diagnostic_setting" "sql_database" {
  name                       = "to-log-analytics"
  target_resource_id         = azurerm_mssql_database.serving.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.this.id

  enabled_log {
    category = "Errors"
  }
  enabled_log {
    category = "Timeouts"
  }
  enabled_log {
    category = "Blocks"
  }
  enabled_log {
    category = "Deadlocks"
  }
}

# --- alerting -------------------------------------------------------------------------------------------------------

resource "azurerm_monitor_action_group" "platform" {
  name                = "ag-${local.name}"
  resource_group_name = azurerm_resource_group.this.name
  short_name          = "${var.project}${var.environment}"

  dynamic "email_receiver" {
    for_each = { for i, address in var.alert_emails : "recipient-${i}" => address }
    content {
      name                    = email_receiver.key
      email_address           = email_receiver.value
      use_common_alert_schema = true
    }
  }

  tags = local.tags
}

# The queries live in monitoring/kql so they can be run and tuned by hand in Log Analytics first.

resource "azurerm_monitor_scheduled_query_rules_alert_v2" "pipeline_failed" {
  name                = "alert-${local.name}-pipeline-failed"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  description         = "A pipeline run failed. Follow the incident runbook (docs/operations-runbook.md)."

  evaluation_frequency = "PT5M"
  window_duration      = "PT15M"
  scopes               = [azurerm_log_analytics_workspace.this.id]
  severity             = 1

  criteria {
    query                   = file("${path.module}/../../monitoring/kql/alert_pipeline_failed.kql")
    time_aggregation_method = "Count"
    threshold               = 0
    operator                = "GreaterThan"

    failing_periods {
      minimum_failing_periods_to_trigger_alert = 1
      number_of_evaluation_periods             = 1
    }
  }

  action {
    action_groups = [azurerm_monitor_action_group.platform.id]
  }

  tags = local.tags
}

# Absence alert: nothing failed, but nothing succeeded either. Silent failures are the dangerous ones.
resource "azurerm_monitor_scheduled_query_rules_alert_v2" "pipeline_stale" {
  name                = "alert-${local.name}-pipeline-stale"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  description         = "No successful daily pipeline run in the last 26 hours."

  evaluation_frequency = "PT1H"
  window_duration      = "P2D"
  scopes               = [azurerm_log_analytics_workspace.this.id]
  severity             = 2

  criteria {
    query                   = file("${path.module}/../../monitoring/kql/alert_pipeline_stale.kql")
    time_aggregation_method = "Count"
    threshold               = 0
    operator                = "GreaterThan"

    failing_periods {
      minimum_failing_periods_to_trigger_alert = 1
      number_of_evaluation_periods             = 1
    }
  }

  action {
    action_groups = [azurerm_monitor_action_group.platform.id]
  }

  tags = local.tags
}

resource "azurerm_monitor_scheduled_query_rules_alert_v2" "sql_contention" {
  name                = "alert-${local.name}-sql-contention"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  description         = "Deadlocks or long blocking on the serving database. Start with sql/ops/04_blocking_and_waits.sql."

  evaluation_frequency = "PT15M"
  window_duration      = "PT30M"
  scopes               = [azurerm_log_analytics_workspace.this.id]
  severity             = 2

  criteria {
    query                   = file("${path.module}/../../monitoring/kql/alert_sql_contention.kql")
    time_aggregation_method = "Count"
    threshold               = 0
    operator                = "GreaterThan"

    failing_periods {
      minimum_failing_periods_to_trigger_alert = 1
      number_of_evaluation_periods             = 1
    }
  }

  action {
    action_groups = [azurerm_monitor_action_group.platform.id]
  }

  tags = local.tags
}

# --- cost -----------------------------------------------------------------------------------------------------------

resource "azurerm_consumption_budget_resource_group" "this" {
  name              = "budget-${local.name}"
  resource_group_id = azurerm_resource_group.this.id

  amount     = var.monthly_budget_gbp
  time_grain = "Monthly"

  time_period {
    start_date = var.budget_start_date
  }

  notification {
    enabled        = true
    threshold      = 80
    operator       = "GreaterThan"
    threshold_type = "Actual"
    contact_emails = var.alert_emails
  }

  notification {
    enabled        = true
    threshold      = 100
    operator       = "GreaterThan"
    threshold_type = "Forecasted" # warns before the money is spent, not after
    contact_emails = var.alert_emails
  }
}
