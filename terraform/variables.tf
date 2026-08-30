variable "snowflake_organization_name" {
  type        = string
  description = "Snowflake organization name"
}

variable "snowflake_account_name" {
  type        = string
  description = "Snowflake account name"
}

variable "snowflake_user" {
  type        = string
  description = "Snowflake user used for Terraform authentication"
}

variable "snowflake_private_key_path" {
  type        = string
  default     = "./.snowflake_keys/snowflake_key.p8"
  description = "Path to the private key used for key-pair authentication"
}