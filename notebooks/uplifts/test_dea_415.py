# Databricks notebook source
# MAGIC %run ../_modules/epma_global/functions

# COMMAND ----------

# MAGIC %run ../_modules/epma_global/test_helpers

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

df_fuzzy_unmappable_input = spark.createDataFrame([
  ('1', 'a', '4', 'APID', 'fuzzy_step1', 'ml0', 'v0'),
  ('1', 'a', '4', 'APID', 'fuzzy_step1', 'ml0', 'v0'),
  ('2', 'b', '5', 'VPID', 'fuzzy_step2', 'ml1', 'v1')
], ['epma_id', 'original_epma_description', 'match_id', 'id_level', 'match_level', 'match_term', 'version_id'])

df_fuzzy_unmappable_expected = spark.createDataFrame([
  ('1', 'a', '4', 'APID', 'fuzzy_step1', 'ml0', 'v0'),
  ('2', 'b', '5', 'VPID', 'fuzzy_step2', 'ml1', 'v1')
], ['epma_id', 'original_epma_description', 'match_id', 'id_level', 'match_level', 'match_term', 'version_id'])

with TemporaryTable(df_fuzzy_unmappable_input, 'test_epma_autocoding', create=True) as tmp_table_fuzzy_unmappable:

  dbutils.notebook.run('./dea_415', 0, {'db': 'test_epma_autocoding', 
                                        'unmappable_table_name': tmp_table_fuzzy_unmappable.name})

  df_fuzzy_unmappable_actual = spark.table(f'test_epma_autocoding.{tmp_table_fuzzy_unmappable.name}')

  assert compare_results(df_fuzzy_unmappable_actual, df_fuzzy_unmappable_expected, join_columns=['epma_id'])    