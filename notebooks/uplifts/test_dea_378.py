# Databricks notebook source
# MAGIC %run ../_modules/epma_global/functions

# COMMAND ----------

# MAGIC %run ../_modules/epma_global/test_helpers

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

df_accuracy_table_input = spark.createDataFrame([
  ('1', 'vmp', 'z999', False, '1821', datetime(2021, 1, 1, 1, 1, 1), 'v1'),
  ('2', 'vtm', 'b142', True, '12', datetime(2021, 1, 2, 1, 1, 1), 'v2'),
  ('3', 'amp', 'a001', False, '23', datetime(2021, 1, 3, 1, 1, 1), 'v3'),
], ['pipeline_match_id_level', 'pipeline_match_level', 'source_match_id_level', 'pipeline_mismatch', 'total_match_count', 'match_datetime', 'version_id'])

df_accuracy_table_expected = spark.createDataFrame([
  ('1', 'vmp', 'z999', False, '1821', datetime(2021, 1, 1, 1, 1, 1), 'Cumulative'),
  ('2', 'vtm', 'b142', True, '12', datetime(2021, 1, 2, 1, 1, 1), 'Cumulative'),
  ('3', 'amp', 'a001', False, '23', datetime(2021, 1, 3, 1, 1, 1), 'Cumulative'),
], ['pipeline_match_id_level', 'pipeline_match_level', 'source_match_id_level', 'pipeline_mismatch', 'total_match_count', 'match_datetime', 'run_id'])

with TemporaryTable(df_accuracy_table_input, 'test_epma_autocoding', create=True) as tmp_table_accuracy_table:
  
  dbutils.notebook.run('./dea_378', 0, {'db': 'test_epma_autocoding', 
                                        'accuracy_table_name': tmp_table_accuracy_table.name})

  df_accuracy_table_actual = spark.table(f'test_epma_autocoding.{tmp_table_accuracy_table.name}')

  assert compare_results(df_accuracy_table_actual, df_accuracy_table_expected, join_columns=['match_datetime', 'run_id']) 