variable "subscription_id" {
  description = "Azure subscription to deploy into."
  type        = string
}

variable "environment" {
  description = "Deployment stage. Drives sizing, retention and protection defaults."
  type        = string

  validation {
    condition     = contains(["dev", "test", "prod"], var.environment)
    error_message = "environment must be one of: dev, test, prod."
  }
}

variable "project" {
  description = "Short project code used in resource names (lowercase letters and digits)."
  type        = string
  default     = "hdp"

  validation {
    condition     = can(regex("^[a-z0-9]{2,8}$", var.project))
    error_message = "project must be 2-8 lowercase letters or digits."
  }
}

variable "location" {
  description = "Azure region."
  type        = string
  default     = "uksouth"
}

variable "owner" {
  description = "Team or mailbox accountable for the resources (tag)."
  type        = string
}

variable "cost_centre" {
  description = "Cost centre for chargeback (tag)."
  type        = string
}

# --- access -----------------------------------------------------------------------------------------

variable "data_engineer_group_object_id" {
  description = "Entra ID group of platform engineers: read/write on the lake, read on secrets."
  type        = string
}

variable "sql_admin_group_name" {
  description = "Entra ID group that administers the SQL server (there is no SQL-authentication admin)."
  type        = string
}

variable "sql_admin_group_object_id" {
  description = "Object ID of the SQL admin group."
  type        = string
}

variable "alert_emails" {
  description = "Mailboxes notified by alerts and budget thresholds."
  type        = list(string)
}

# --- network ----------------------------------------------------------------------------------------

variable "network_default_action" {
  description = "Default firewall action for the lake and Key Vault. Use Deny wherever private endpoints or an allow-list exist."
  type        = string
  default     = "Deny"

  validation {
    condition     = contains(["Allow", "Deny"], var.network_default_action)
    error_message = "network_default_action must be Allow or Deny."
  }
}

variable "allowed_ip_ranges" {
  description = "Public IPv4 addresses or CIDR ranges allowed through the lake and Key Vault firewalls (for example a build agent)."
  type        = list(string)
  default     = []
}

variable "sql_public_network_access" {
  description = "Whether the SQL server accepts public connections. Keep false in test and prod and use private endpoints."
  type        = bool
  default     = false
}

# --- sizing and cost --------------------------------------------------------------------------------

variable "storage_replication" {
  description = "Lake replication: LRS (cheapest), ZRS, GRS."
  type        = string
  default     = "LRS"

  validation {
    condition     = contains(["LRS", "ZRS", "GRS", "GZRS"], var.storage_replication)
    error_message = "storage_replication must be LRS, ZRS, GRS or GZRS."
  }
}

variable "sql_sku_name" {
  description = "Database SKU. GP_S_* is serverless (auto-pauses, bills per second of use): good for dev and test."
  type        = string
  default     = "GP_S_Gen5_1"
}

variable "sql_max_size_gb" {
  description = "Maximum database size in GB."
  type        = number
  default     = 32
}

variable "sql_min_capacity" {
  description = "Serverless only: minimum vCores while running."
  type        = number
  default     = 0.5
}

variable "sql_auto_pause_delay_minutes" {
  description = "Serverless only: pause after this many idle minutes (-1 disables pausing)."
  type        = number
  default     = 60
}

variable "sql_pitr_days" {
  description = "Point-in-time-restore retention in days (1-35)."
  type        = number
  default     = 7
}

variable "log_retention_days" {
  description = "Log Analytics retention. Longer costs more; pick the shortest the audit requirement allows."
  type        = number
  default     = 30
}

variable "log_daily_quota_gb" {
  description = "Daily ingestion cap for Log Analytics: a cost guard against a runaway log source."
  type        = number
  default     = 1
}

variable "monthly_budget_gbp" {
  description = "Monthly budget for the resource group, in the billing currency."
  type        = number
}

variable "budget_start_date" {
  description = "First day of a month, RFC 3339, for example 2026-10-01T00:00:00Z."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{4}-[0-9]{2}-01T00:00:00Z$", var.budget_start_date))
    error_message = "budget_start_date must be the first day of a month, formatted like 2026-10-01T00:00:00Z."
  }
}

variable "extra_tags" {
  description = "Additional tags merged onto every resource."
  type        = map(string)
  default     = {}
}
