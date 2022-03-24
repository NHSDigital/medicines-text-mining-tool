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

dbutils.widgets.text('unmappable_table_name', 'unmappable', 'unmappable_table_name')
UNMAPPABLE_TABLE_NAME = dbutils.widgets.get('unmappable_table_name')
assert UNMAPPABLE_TABLE_NAME

dbutils.widgets.text('match_lookup_final_name', 'match_lookup_final', 'match_lookup_final_name')
MATCH_LOOKUP_FINAL_TABLE_NAME = dbutils.widgets.get('match_lookup_final_name')
assert MATCH_LOOKUP_FINAL_TABLE_NAME

dbutils.widgets.text('processed_descriptions_table_name', 'processed_epma_descriptions', 'processed_descriptions_table_name')
PROCESSED_DESCRIPTIONS_TABLE_NAME = dbutils.widgets.get('processed_descriptions_table_name')
assert PROCESSED_DESCRIPTIONS_TABLE_NAME

# COMMAND ----------

def add_form_in_text_col_to_match_lookup_final_table(db: str, table: str) -> None:

  if not table_exists(db, table):
    return
  
  df = spark.table(f'{db}.{table}')

  if 'form_in_text' not in df.columns:
    with TemporaryTable(df, db, create=True) as tmp_table:
      df_tmp_table = spark.table(f'{db}.{tmp_table.name}').withColumn('form_in_text', F.lit(' '))
      
      df_tmp_table = df_tmp_table.select('original_epma_description', 'form_in_text', 'match_id', 
                                           'match_term', 'id_level', 'match_level', 'match_datetime', 
                                            'version_id', 'run_id')
        
      create_table(df_tmp_table, db, table=table, overwrite=True)

# COMMAND ----------

def add_form_in_text_col_to_unmappable_table(db: str, table: str) -> None:

  if not table_exists(db, table):
    return
  
  df = spark.table(f'{db}.{table}')

  if 'form_in_text' not in df.columns:
    with TemporaryTable(df, db, create=True) as tmp_table:
      df_tmp_table = spark.table(f'{db}.{tmp_table.name}').withColumn('form_in_text', F.lit(' '))
      
      df_tmp_table = df_tmp_table.select('original_epma_description', 'form_in_text', 'reason', 
                                           'match_datetime', 'run_id')
        
      create_table(df_tmp_table, db, table=table, overwrite=True)

# COMMAND ----------

def add_form_in_text_col_to_processed_descriptions_table(db: str, table: str) -> None:

  if not table_exists(db, table):
    return
  
  df = spark.table(f'{db}.{table}')

  if 'form_in_text' not in df.columns:
    with TemporaryTable(df, db, create=True) as tmp_table:
      df_tmp_table = spark.table(f'{db}.{tmp_table.name}').withColumn('form_in_text', F.lit(' '))
      
      df_tmp_table = df_tmp_table.select('original_epma_description', 'form_in_text', 'matched')
        
      create_table(df_tmp_table, db, table=table, overwrite=True)

# COMMAND ----------

add_form_in_text_col_to_match_lookup_final_table(DB, MATCH_LOOKUP_FINAL_TABLE_NAME)
add_form_in_text_col_to_unmappable_table(DB, UNMAPPABLE_TABLE_NAME)
add_form_in_text_col_to_processed_descriptions_table(DB, PROCESSED_DESCRIPTIONS_TABLE_NAME)