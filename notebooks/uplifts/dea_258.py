# Databricks notebook source
# MAGIC %run ../_modules/epma_global/functions

# COMMAND ----------

def add_fields_to_existing_tables(db: str, uplift_table_name: str):
  if table_exists(db, uplift_table_name):
    if 'version_id' not in spark.table(f'{db}.{uplift_table_name}').columns:
      spark.sql(f"ALTER TABLE {db}.{uplift_table_name} ADD COLUMNS (version_id STRING)")
    if 'match_term' not in spark.table(f'{db}.{uplift_table_name}').columns:
      spark.sql(f"ALTER TABLE {db}.{uplift_table_name} ADD COLUMNS (match_term STRING)")

# COMMAND ----------

UPLIFT_TABLE_NAME = 'match_lookup_final'

dbutils.widgets.text('db', '', 'db')
DB = dbutils.widgets.get('db')
assert DB

add_fields_to_existing_tables(DB, UPLIFT_TABLE_NAME)