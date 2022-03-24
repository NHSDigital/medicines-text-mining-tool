# Databricks notebook source
# MAGIC %run ../_modules/epma_global/functions

# COMMAND ----------

# MAGIC %run ../_modules/epma_global/test_helpers

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

df_match_lookup_final_input = spark.createDataFrame([
  ('a', '4', 'APID', 'fuzzy_step1', datetime(2021, 1, 1, 1, 1, 1), 'ml0', 'v0'),
  ('b', '5', 'VPID', 'fuzzy_step2', datetime(2021, 1, 2, 1, 1, 1), 'ml1', 'v1'),
  ('d', '7', 'ref_id', 'entity', datetime(2021, 1, 4, 1, 1, 1), 'ml4', 'v4'),
  ('c', '6', 'VTMID', 'fuzzy_step3', datetime(2021, 1, 3, 1, 1, 1), 'ml2', 'v2'),
], ['original_epma_description', 'match_id', 'id_level', 'match_level', 'match_datetime', 'match_term', 'version_id'])

df_match_lookup_final_expected = spark.createDataFrame([
  ('a', '4', 'APID', 'fuzzy_step1', datetime(2021, 1, 1, 1, 1, 1), 'ml0', 'v0'),
  ('b', '5', 'VPID', 'fuzzy_step2', datetime(2021, 1, 2, 1, 1, 1), 'ml1', 'v1'),
  ('c', '6', 'VTMID', 'fuzzy_step3', datetime(2021, 1, 3, 1, 1, 1), 'ml2', 'v2'),
], ['original_epma_description', 'match_id', 'id_level', 'match_level', 'match_datetime', 'match_term', 'version_id'])

with TemporaryTable(df_match_lookup_final_input, 'test_epma_autocoding', create=True) as tmp_table_match_lookup_final:

  dbutils.notebook.run('./dea_414', 0, {'db': 'test_epma_autocoding', 
                                        'match_lookup_final_name': tmp_table_match_lookup_final.name})

  df_match_lookup_final_actual = spark.table(f'test_epma_autocoding.{tmp_table_match_lookup_final.name}')

  assert compare_results(df_match_lookup_final_actual, df_match_lookup_final_expected, join_columns=['original_epma_description'])    