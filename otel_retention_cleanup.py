# Databricks notebook source
# MAGIC %md
# MAGIC # OTel Raw Table Retention Cleanup
# MAGIC Deletes rows older than the configured retention period from raw OTel tables and reclaims storage.
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `retention_days` (default: 90)
# MAGIC - `source_catalog` — UC catalog containing raw OTel tables
# MAGIC - `source_schema` — UC schema containing raw OTel tables
# MAGIC - `table_prefix` — prefix used for OTel table names

# COMMAND ----------

dbutils.widgets.text("retention_days", "90", "Retention Days")
dbutils.widgets.text("source_catalog", "", "Source Catalog")
dbutils.widgets.text("source_schema", "", "Source Schema")
dbutils.widgets.text("table_prefix", "", "Table Prefix")

retention_days = int(dbutils.widgets.get("retention_days"))
source_catalog = dbutils.widgets.get("source_catalog")
source_schema = dbutils.widgets.get("source_schema")
table_prefix = dbutils.widgets.get("table_prefix")

spans_table = f"{source_catalog}.{source_schema}.{table_prefix}_otel_spans"
logs_table = f"{source_catalog}.{source_schema}.{table_prefix}_otel_logs"

print(f"Retention period: {retention_days} days")
print(f"Spans table: {spans_table}")
print(f"Logs table:  {logs_table}")

# COMMAND ----------

spark.sql(f"""
DELETE FROM {spans_table}
WHERE date < current_date() - INTERVAL {retention_days} DAYS
""")

# COMMAND ----------

spark.sql(f"""
DELETE FROM {logs_table}
WHERE date < current_date() - INTERVAL {retention_days} DAYS
""")

# COMMAND ----------

spark.sql(f"VACUUM {spans_table}")

# COMMAND ----------

spark.sql(f"VACUUM {logs_table}")

