# Databricks notebook source
# MAGIC %run ../_modules/epma_global/functions

# COMMAND ----------

# MAGIC %run ../_modules/epma_global/test_helpers

# COMMAND ----------

dbutils.widgets.text('db', 'test_epma_autocoding', 'db')
DB = dbutils.widgets.get('db')
assert DB

dbutils.widgets.text('unmappable_table_name', 'unmappable', 'unmappable_table_name')
UNMAPPABLE_TABLE_NAME = dbutils.widgets.get('unmappable_table_name')
assert UNMAPPABLE_TABLE_NAME

# COMMAND ----------

#Remove all duplicate records  in the "epma_id" column.

# COMMAND ----------

def remove_records(db: str, table: str) -> None:

  if not table_exists(db, table):
    return
  
  df = spark.table(f'{db}.{table}')

  with TemporaryTable(df, db, create=True) as tmp_table:
    df_tmp_table = spark.table(f'{db}.{tmp_table.name}').dropDuplicates(['original_epma_description'])

    create_table(df_tmp_table, db, table=table, overwrite=True)

# COMMAND ----------

remove_records(DB, UNMAPPABLE_TABLE_NAME)