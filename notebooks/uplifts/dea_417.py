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

# COMMAND ----------

def replace_strings_in_match_lookup_final_table(db: str, table: str) -> None:

  if not table_exists(db, table):
    return
  
  df = spark.table(f'{db}.{table}')
  
  with TemporaryTable(df, db, create=True) as tmp_table:
    df_tmp_table = spark.table(f'{db}.{tmp_table.name}')
    df_tmp_table = df_tmp_table.withColumn('match_level', F.regexp_replace('match_level', 'sodium_chloride_fuzzy_wratio', 'fuzzy_sodium_chloride'))
    df_tmp_table = df_tmp_table.withColumn('match_level', F.regexp_replace('match_level', 'fuzzy_high_score_step2_vtm', 'fuzzy_linked_vtm'))
    df_tmp_table = df_tmp_table.withColumn('match_level', F.regexp_replace('match_level', 'fuzzy_matching', 'fuzzy_linked'))
    df_tmp_table = df_tmp_table.withColumn('match_level', F.regexp_replace('match_level', 'full_fuzzy_high_score', 'fuzzy_non_linked'))
    df_tmp_table = df_tmp_table.withColumn('match_level', F.regexp_replace('match_level', 'fuzzy_matched_moiety_with_unique_vtm', 'fuzzy_moiety_vtm'))
    
    create_table(df_tmp_table, db, table=table, overwrite=True)

# COMMAND ----------

def replace_strings_in_unmappable_table(db: str, table: str) -> None:

  if not table_exists(db, table):
    return

  df = spark.table(f'{db}.{table}')
  
  with TemporaryTable(df, db, create=True) as tmp_table:
    df_tmp_table = spark.table(f'{db}.{tmp_table.name}')
    df_tmp_table = df_tmp_table.withColumn('reason', F.regexp_replace('reason', 'sodium_chloride_fuzzy_wratio_low_score', 'fuzzy_sodium_chloride_low_score'))
    df_tmp_table = df_tmp_table.withColumn('reason', F.regexp_replace('reason', 'tied max confidence scores at all refdata levels', 'fuzzy_linked_tied_confidence_score'))
    df_tmp_table = df_tmp_table.withColumn('reason', F.regexp_replace('reason', 'multi-match_linked_record', 'fuzzy_linked_tied_confidence_score'))
    df_tmp_table = df_tmp_table.withColumn('reason', F.regexp_replace('reason', 'low_score_fuzzy_non_linked', 'fuzzy_moiety_low_score'))
    df_tmp_table = df_tmp_table.withColumn('reason', F.regexp_replace('reason', 'low-score_fuzzy_non_linked', 'fuzzy_moiety_low_score'))
    df_tmp_table = df_tmp_table.withColumn('reason', F.regexp_replace('reason', 'unmappable_fuzzy_non_linked', 'fuzzy_moiety_no_unique_vtm'))
    df_tmp_table = df_tmp_table.withColumn('reason', F.regexp_replace('reason', 'no_moieties_fuzzy_non_linked', 'fuzzy_moiety_no_moieties'))
    df_tmp_table = df_tmp_table.withColumn('match_datetime', F.to_timestamp(F.regexp_replace('match_datetime', 'abc', 'abc')))
    
    create_table(df_tmp_table, db, table=table, overwrite=True)

# COMMAND ----------

replace_strings_in_match_lookup_final_table(DB, MATCH_LOOKUP_FINAL_TABLE_NAME)
replace_strings_in_unmappable_table(DB, UNMAPPABLE_TABLE_NAME)