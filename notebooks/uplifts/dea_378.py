# Databricks notebook source
dbutils.widgets.text('db', '', 'db')
DB = dbutils.widgets.get('db')
assert DB

dbutils.widgets.text('accuracy_table_name', 'accuracy', 'accuracy_table_name')
ACCURACY_TABLE_NAME = dbutils.widgets.get('accuracy_table_name')
assert ACCURACY_TABLE_NAME

# COMMAND ----------

# MAGIC %run ../_modules/epma_global/functions

# COMMAND ----------

# MAGIC %run ../_modules/epma_global/test_helpers

# COMMAND ----------

from pyspark.sql.functions import lit

def add_run_id_to_accuracy_table(db: str, table: str) -> None:

  if not table_exists(db, table):
    return
  
  df = spark.table(f'{db}.{table}')

  if 'run_id' not in df.columns:
    with TemporaryTable(df, db, create=True) as tmp_table:
      df_tmp_table = spark.table(f'{db}.{tmp_table.name}').drop('version_id') \
                                                          .withColumn('run_id', lit('Cumulative'))
        
      create_table(df_tmp_table, db, table=table, overwrite=True)

# COMMAND ----------

add_run_id_to_accuracy_table(DB, ACCURACY_TABLE_NAME)