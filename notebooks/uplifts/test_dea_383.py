# Databricks notebook source
# MAGIC %run ../_modules/epma_global/functions

# COMMAND ----------

# MAGIC %run ../_modules/epma_global/test_helpers

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

df_match_lookup_final_input = spark.createDataFrame([
  ('a', '4', 'APID', 'fuzzy_step1', datetime(2021, 1, 1, 1, 1, 1), 'ml0', 'v0', 'r0'),
  ('b', '5', 'VPID', 'fuzzy_step2', datetime(2021, 1, 2, 1, 1, 1), 'ml1', 'v1', 'r1'),
  ('c', '6', 'VTMID', 'fuzzy_step3', datetime(2021, 1, 3, 1, 1, 1), 'ml2', 'v2', 'r2'),
], ['original_epma_description', 'match_id', 'id_level', 'match_level', 'match_datetime', 'match_term', 'version_id', 'run_id'])

df_match_lookup_final_expected = spark.createDataFrame([
  ('a', ' ', '4', 'ml0', 'APID', 'fuzzy_step1', datetime(2021, 1, 1, 1, 1, 1), 'v0', 'r0'),
  ('b', ' ', '5', 'ml1', 'VPID', 'fuzzy_step2', datetime(2021, 1, 2, 1, 1, 1), 'v1', 'r1'),
  ('c', ' ', '6', 'ml2', 'VTMID', 'fuzzy_step3', datetime(2021, 1, 3, 1, 1, 1), 'v2', 'r2'),
], ['original_epma_description', 'form_in_text', 'match_id', 'match_term', 'id_level', 'match_level', 'match_datetime', 'version_id', 'run_id'])

df_unmappable_table_input = spark.createDataFrame([
  ('a', 'unmappable_reason', datetime(2021, 1, 1, 1, 1, 1), 'r0'),
  ('b', 'unmappable_reason', datetime(2021, 1, 2, 1, 1, 1), 'r1'),
  ('c', 'unmappable_reason', datetime(2021, 1, 3, 1, 1, 1), 'r2'),
], ['original_epma_description', 'reason', 'match_datetime', 'run_id'])

df_unmappable_table_expected = spark.createDataFrame([
  ('a', ' ', 'unmappable_reason', datetime(2021, 1, 1, 1, 1, 1), 'r0'),
  ('b', ' ', 'unmappable_reason', datetime(2021, 1, 2, 1, 1, 1), 'r1'),
  ('c', ' ', 'unmappable_reason', datetime(2021, 1, 3, 1, 1, 1), 'r2'),
], ['original_epma_description', 'form_in_text', 'reason', 'match_datetime', 'run_id'])

df_processed_descriptions_table_input = spark.createDataFrame([
  ('a', 'm1'),
  ('b', 'm1'),
  ('c', 'm1'),
], ['original_epma_description', 'matched'])

df_processed_descriptions_table_expected = spark.createDataFrame([
  ('a', ' ', 'm1'),
  ('b', ' ', 'm1'),
  ('c', ' ', 'm1'),
], ['original_epma_description', 'form_in_text', 'matched'])

with TemporaryTable(df_match_lookup_final_input, 'test_epma_autocoding', create=True) as tmp_table_match_lookup_final:
  with TemporaryTable(df_unmappable_table_input, 'test_epma_autocoding', create=True) as tmp_table_unmappable_table:
    with TemporaryTable(df_processed_descriptions_table_input, 'test_epma_autocoding', create=True) as tmp_table_processed_descriptions_table:

      dbutils.notebook.run('./dea_383', 0, {'db': 'test_epma_autocoding', 
                                            'match_lookup_final_name': tmp_table_match_lookup_final.name,
                                            'unmappable_table_name': tmp_table_unmappable_table.name,
                                            'processed_descriptions_table_name': tmp_table_processed_descriptions_table.name})

      df_match_lookup_final_actual = spark.table(f'test_epma_autocoding.{tmp_table_match_lookup_final.name}')
      df_unmappable_table_actual = spark.table(f'test_epma_autocoding.{tmp_table_unmappable_table.name}')
      df_processed_descriptions_table_actual = spark.table(f'test_epma_autocoding.{tmp_table_processed_descriptions_table.name}')

      assert compare_results(df_match_lookup_final_actual, df_match_lookup_final_expected, join_columns=['original_epma_description']) 
      assert compare_results(df_unmappable_table_actual, df_unmappable_table_expected, join_columns=['original_epma_description']) 
      assert compare_results(df_processed_descriptions_table_actual, df_processed_descriptions_table_expected, join_columns=['original_epma_description']) 

# COMMAND ----------

