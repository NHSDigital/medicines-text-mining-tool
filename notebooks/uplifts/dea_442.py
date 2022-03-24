# Databricks notebook source
# MAGIC %run ../_modules/epma_global/functions

# COMMAND ----------

# MAGIC %run ../_modules/epma_global/test_helpers

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text('db', '', 'db')
DB = dbutils.widgets.get('db')
assert DB

dbutils.widgets.text('match_lookup_final_name', 'match_lookup_final', 'match_lookup_final_name')
MATCH_LOOKUP_FINAL_TABLE_NAME = dbutils.widgets.get('match_lookup_final_name')
assert MATCH_LOOKUP_FINAL_TABLE_NAME

# COMMAND ----------

def remove_fuzzy_non_linked_rows_from_match_lookup_final_table(db: str, table: str) -> None:

  if not table_exists(db, table):
    return
  
  df = spark.table(f'{db}.{table}')
  
  with TemporaryTable(df, db, create=True) as tmp_table:
    df_tmp_table = spark.table(f'{db}.{tmp_table.name}')
    df_tmp_table = df_tmp_table.where(F.col('match_level') != 'fuzzy_non_linked')
    
    create_table(df_tmp_table, db, table=table, overwrite=True)

# COMMAND ----------

remove_fuzzy_non_linked_rows_from_match_lookup_final_table(DB, MATCH_LOOKUP_FINAL_TABLE_NAME)