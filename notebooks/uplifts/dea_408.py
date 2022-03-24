# Databricks notebook source
dbutils.widgets.text('db', '', 'db')
DB = dbutils.widgets.get('db')
assert DB

dbutils.widgets.text('unmappable_table_name', 'unmappable', 'unmappable_table_name')
UNMAPPABLE_TABLE_NAME = dbutils.widgets.get('unmappable_table_name')
assert UNMAPPABLE_TABLE_NAME

dbutils.widgets.text('match_lookup_final_name', 'match_lookup_final', 'match_lookup_final_name')
MATCH_LOOKUP_FINAL_TABLE_NAME = dbutils.widgets.get('match_lookup_final_name')
assert MATCH_LOOKUP_FINAL_TABLE_NAME

# COMMAND ----------

# MAGIC %run ../_modules/epma_global/functions

# COMMAND ----------

# MAGIC %run ../_modules/epma_global/test_helpers

# COMMAND ----------

from uuid import uuid4

# COMMAND ----------

#Drop the epma_id column from the match_lookup_final and the unmappable tables. Change the order of the match_lookup_final column.

# COMMAND ----------

def remove_epma_id_from_table(db: str, table: str, is_match_lookup_final: bool=False) -> None:

  if not table_exists(db, table):
    return
  
  df = spark.table(f'{db}.{table}')

  if 'epma_id' in df.columns:
    with TemporaryTable(df, db, create=True) as tmp_table:
      df_tmp_table = spark.table(f'{db}.{tmp_table.name}').drop('epma_id')
      
      if is_match_lookup_final:
        df_tmp_table = df_tmp_table.select('original_epma_description', 'match_id', 
                                           'id_level', 'match_level', 'match_datetime', 
                                           'match_term', 'version_id')
        
      create_table(df_tmp_table, db, table=table, overwrite=True)

# COMMAND ----------

remove_epma_id_from_table(DB, UNMAPPABLE_TABLE_NAME)

# COMMAND ----------

remove_epma_id_from_table(DB, MATCH_LOOKUP_FINAL_TABLE_NAME, is_match_lookup_final=True)