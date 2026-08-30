resource "snowflake_warehouse" "spark_pipeline_wh" {
  name           = "SPARK_PIPELINE_WH"
  warehouse_size = "XSMALL"
  auto_suspend   = 60
  auto_resume    = true
}

resource "snowflake_database" "spark_pipeline_db" {
  name = "SPARK_PIPELINE_DB"
}

resource "snowflake_account_role" "spark_pipeline_role" {
  name = "SPARK_PIPELINE_ROLE"
}

resource "snowflake_grant_privileges_to_account_role" "warehouse_usage" {
  account_role_name = snowflake_account_role.spark_pipeline_role.name
  privileges         = ["USAGE", "OPERATE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.spark_pipeline_wh.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "database_usage" {
  account_role_name = snowflake_account_role.spark_pipeline_role.name
  privileges         = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.spark_pipeline_db.name
  }
}