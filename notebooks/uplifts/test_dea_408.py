# Databricks notebook source
# MAGIC %run ../_modules/epma_global/functions

# COMMAND ----------

# MAGIC %run ../_modules/epma_global/test_helpers

# COMMAND ----------

from datetime import datetime
from uuid import uuid4

# COMMAND ----------

df_unmappable_input = spark.createDataFrame([
  ('0', 'a', 'blah1', datetime(2021, 1, 1, 1, 1, 1)),
  ('1', 'b', 'blah2', datetime(2021, 1, 2, 1, 1, 1)),
  ('2', 'c', 'blah3', datetime(2021, 1, 3, 1, 1, 1)),
], ['epma_id', 'original_epma_description', 'reason', 'match_datetime'])

df_unmappable_expected = spark.createDataFrame([
  ('a', 'blah1', datetime(2021, 1, 1, 1, 1, 1)),
  ('b', 'blah2', datetime(2021, 1, 2, 1, 1, 1)),
  ('c', 'blah3', datetime(2021, 1, 3, 1, 1, 1)),
], ['original_epma_description', 'reason', 'match_datetime'])

df_match_lookup_final_input = spark.createDataFrame([
  ('0', 'a', '4', 'APID', 'fuzzy_step1', datetime(2021, 1, 1, 1, 1, 1), 'v0', 'ml0'),
  ('1', 'b', '5', 'VPID', 'fuzzy_step2', datetime(2021, 1, 2, 1, 1, 1), 'v1', 'ml1'),
  ('2', 'c', '6', 'VTMID', 'fuzzy_step3', datetime(2021, 1, 3, 1, 1, 1), 'v2', 'ml2'),
], ['epma_id', 'original_epma_description', 'match_id', 'id_level', 'match_level', 'match_datetime', 'version_id', 'match_term'])

df_match_lookup_final_expected = spark.createDataFrame([
  ('a', '4', 'APID', 'fuzzy_step1', datetime(2021, 1, 1, 1, 1, 1), 'ml0', 'v0'),
  ('b', '5', 'VPID', 'fuzzy_step2', datetime(2021, 1, 2, 1, 1, 1), 'ml1', 'v1'),
  ('c', '6', 'VTMID', 'fuzzy_step3', datetime(2021, 1, 3, 1, 1, 1), 'ml2', 'v2'),
], ['original_epma_description', 'match_id', 'id_level', 'match_level', 'match_datetime', 'match_term', 'version_id'])

with TemporaryTable(df_unmappable_input, 'test_epma_autocoding', create=True) as tmp_table_unmappable:
  with TemporaryTable(df_match_lookup_final_input, 'test_epma_autocoding', create=True) as tmp_table_match_lookup_final:

    dbutils.notebook.run('./dea_408', 0, {'db': 'test_epma_autocoding', 
                                          'unmappable_table_name': tmp_table_unmappable.name, 
                                          'match_lookup_final_name': tmp_table_match_lookup_final.name})

    df_unmappable_actual = spark.table(f'test_epma_autocoding.{tmp_table_unmappable.name}')
    df_match_lookup_final_actual = spark.table(f'test_epma_autocoding.{tmp_table_match_lookup_final.name}')

    assert compare_results(df_unmappable_actual, df_unmappable_expected, join_columns=['original_epma_description'])
    assert compare_results(df_match_lookup_final_actual, df_match_lookup_final_expected, join_columns=['original_epma_description'])    