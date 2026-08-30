output "warehouse_name" {
  value = snowflake_warehouse.spark_pipeline_wh.name
}

output "database_name" {
  value = snowflake_database.spark_pipeline_db.name
}

output "role_name" {
  value = snowflake_account_role.spark_pipeline_role.name
}