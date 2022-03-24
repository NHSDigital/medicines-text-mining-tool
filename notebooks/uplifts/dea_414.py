# Databricks notebook source
dbutils.widgets.text('db', '', 'db')
DB = dbutils.widgets.get('db')
assert DB

dbutils.widgets.text('match_lookup_final_name', 'match_lookup_final', 'match_lookup_final_name')
MATCH_LOOKUP_FINAL_TABLE_NAME = dbutils.widgets.get('match_lookup_final_name')
assert MATCH_LOOKUP_FINAL_TABLE_NAME

# COMMAND ----------

# MAGIC %run ../_modules/epma_global/functions

# COMMAND ----------

# MAGIC %run ../_modules/epma_global/test_helpers

# COMMAND ----------

#Remove all records with "ref_id" in the "id_level" column.

# COMMAND ----------

def remove_records(db: str, table: str) -> None:

  if not table_exists(db, table):
    return
  
  df = spark.table(f'{db}.{table}')

  with TemporaryTable(df, db, create=True) as tmp_table:
    df_tmp_table = spark.table(f'{db}.{tmp_table.name}').where(col('id_level') != 'ref_id')

    create_table(df_tmp_table, db, table=table, overwrite=True)

# COMMAND ----------

remove_records(DB, MATCH_LOOKUP_FINAL_TABLE_NAME)