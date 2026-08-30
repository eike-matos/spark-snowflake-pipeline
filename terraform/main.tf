terraform {
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 2.0"
    }
  }
  required_version = ">= 1.6.0"
}

provider "snowflake" {
  organization_name = var.snowflake_organization_name
  account_name       = var.snowflake_account_name
  user               = var.snowflake_user
  authenticator      = "SNOWFLAKE_JWT"
  private_key        = file(var.snowflake_private_key_path)
  role               = "ACCOUNTADMIN"
}