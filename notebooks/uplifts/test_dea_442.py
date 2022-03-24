# Databricks notebook source
# MAGIC %run ../_modules/epma_global/functions

# COMMAND ----------

# MAGIC %run ../_modules/epma_global/test_helpers

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

df_match_lookup_final_input = spark.createDataFrame([
  ('a', '4', 'APID', 'exact_by_name', datetime(2021, 1, 1, 1, 1, 1), 'm1', 'v0', 'r0'),
  ('b', '5', 'VPID', 'exact_by_prev_name', datetime(2021, 1, 2, 1, 1, 1), 'm2', 'v1', 'r1'),
  ('c', '6', 'VTMID', 'entity', datetime(2021, 1, 3, 1, 1, 1), 'm3', 'v2', 'r2'),
  ('d', '7', 'APID', 'fuzzy_sodium_chloride', datetime(2021, 1, 1, 1, 1, 1), 'm4', 'v0', 'r0'),
  ('e', '8', 'VPID', 'fuzzy_linked_vtm', datetime(2021, 1, 2, 1, 1, 1), 'm5', 'v1', 'r1'),
  ('f', '9', 'VTMID', 'fuzzy_linked', datetime(2021, 1, 3, 1, 1, 1), 'm6', 'v2', 'r2'),
  ('g', '1', 'APID', 'fuzzy_non_linked', datetime(2021, 1, 1, 1, 1, 1), 'm7', 'v0', 'r0'),
  ('h', '2', 'VPID', 'fuzzy_moiety_vtm', datetime(2021, 1, 2, 1, 1, 1), 'm8', 'v1', 'r1'),
], ['original_epma_description', 'match_id', 'id_level', 'match_level', 'match_datetime', 'match_term', 'version_id', 'run_id'])

df_match_lookup_final_expected = spark.createDataFrame([
  ('a', '4', 'APID', 'exact_by_name', datetime(2021, 1, 1, 1, 1, 1), 'm1', 'v0', 'r0'),
  ('b', '5', 'VPID', 'exact_by_prev_name', datetime(2021, 1, 2, 1, 1, 1), 'm2', 'v1', 'r1'),
  ('c', '6', 'VTMID', 'entity', datetime(2021, 1, 3, 1, 1, 1), 'm3', 'v2', 'r2'),
  ('d', '7', 'APID', 'fuzzy_sodium_chloride', datetime(2021, 1, 1, 1, 1, 1), 'm4', 'v0', 'r0'),
  ('e', '8', 'VPID', 'fuzzy_linked_vtm', datetime(2021, 1, 2, 1, 1, 1), 'm5', 'v1', 'r1'),
  ('f', '9', 'VTMID', 'fuzzy_linked', datetime(2021, 1, 3, 1, 1, 1), 'm6', 'v2', 'r2'),
  ('h', '2', 'VPID', 'fuzzy_moiety_vtm', datetime(2021, 1, 2, 1, 1, 1), 'm8', 'v1', 'r1'),
], ['original_epma_description', 'match_id', 'id_level', 'match_level', 'match_datetime', 'match_term', 'version_id', 'run_id'])

df_unmappable_table_input = spark.createDataFrame([
  ('a', 'user_curated_list', datetime(2021, 1, 1, 1, 1, 1), 'r0'),
  ('b', 'sodium_chloride_fuzzy_wratio_low_score', datetime(2021, 1, 2, 1, 1, 1), 'r1'),
], ['original_epma_description', 'reason', 'match_datetime', 'run_id'])

df_accuracy_table_input = spark.createDataFrame([
  ('APID', 'fuzzy_matching', 'AMP', 'pipeline_mismatch', 2, datetime(2021, 1, 1, 1, 1, 1), 'r0'),
  ('APID', 'fuzzy_matching', 'AMP', 'pipeline_mismatch', 2, datetime(2021, 1, 1, 1, 1, 1), 'r0'),
], ['pipeline_match_id_level', 'pipeline_match_level', 'source_match_id_level', 'pipeline_mismatch', 'total_match_count', 'match_datetime', 'run_id'])

with TemporaryTable(df_match_lookup_final_input, 'test_epma_autocoding', create=True) as tmp_table_match_lookup_final:
    with TemporaryTable(df_unmappable_table_input, 'test_epma_autocoding', create=True) as tmp_table_unmappable_table:
      with TemporaryTable(df_accuracy_table_input, 'test_epma_autocoding', create=True) as tmp_table_accuracy_table:

        dbutils.notebook.run('./dea_442', 0, {'db': 'test_epma_autocoding', 
                                              'match_lookup_final_name': tmp_table_match_lookup_final.name,
                                              'unmappable_table_name': tmp_table_unmappable_table.name,
                                              'accuracy_table_name': tmp_table_accuracy_table.name})

        df_match_lookup_final_actual = spark.table(f'test_epma_autocoding.{tmp_table_match_lookup_final.name}')
        assert compare_results(df_match_lookup_final_actual, df_match_lookup_final_expected, join_columns=['original_epma_description']) 

# COMMAND ----------

