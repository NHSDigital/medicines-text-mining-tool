# Databricks notebook source
# MAGIC %run ../../_modules/epma_global/ref_data_lib

# COMMAND ----------

from pyspark.sql.functions import col
import pyspark.sql.functions as F

# COMMAND ----------

def combine_ref_data(ref_data_store: ReferenceDataFormatter) -> DataFrame:
  #TODO: Why change the id level? It is completely unnecessary.
  df_amp = ref_data_store.amp.withColumn(ref_data_store.ID_LEVEL_COL, lit('AMP')) \
                             .select(ref_data_store.ID_COL, ref_data_store.TEXT_COL, ref_data_store.ID_LEVEL_COL)
  df_vmp = ref_data_store.vmp.withColumn(ref_data_store.ID_LEVEL_COL, lit('VMP')) \
                             .select(ref_data_store.ID_COL, ref_data_store.TEXT_COL, ref_data_store.ID_LEVEL_COL)
  df_vtm = ref_data_store.vtm.withColumn(ref_data_store.ID_LEVEL_COL, lit('VTM')) \
                             .select(ref_data_store.ID_COL, ref_data_store.TEXT_COL, ref_data_store.ID_LEVEL_COL)
  
  df_vmp_prev = ref_data_store.vmp_prev.withColumn(ref_data_store.ID_COL, col('VPIDPREV')) \
                                       .withColumn(ref_data_store.ID_LEVEL_COL, lit('VMP')) \
                                       .select(ref_data_store.ID_COL, ref_data_store.TEXT_COL, ref_data_store.ID_LEVEL_COL)
  df_vtm_prev = ref_data_store.vtm_prev.withColumn(ref_data_store.ID_LEVEL_COL, lit('VTM')) \
                                       .select(ref_data_store.ID_COL, ref_data_store.TEXT_COL, ref_data_store.ID_LEVEL_COL)
  
  df_amp_vmp_vtm = df_amp.union(df_vmp) \
                         .union(df_vtm) \
                         .union(df_vmp_prev) \
                         .union(df_vtm_prev)
  
  return df_amp_vmp_vtm

# COMMAND ----------

def add_dss_to_lookup(df_amp_vmp_vtm: DataFrame,
                      df_lookup: DataFrame,
                      ref_data_id_col: str,
                      ref_data_text_col: str,
                      new_text_col: str,
                      lookup_id_col: str
                     ) -> DataFrame:
  '''
  Add the match term to the pipeline output
  '''
  return df_lookup.join(df_amp_vmp_vtm, on=(df_lookup[lookup_id_col] == df_amp_vmp_vtm[ref_data_id_col]), how='left') \
                  .withColumn(new_text_col, col(ref_data_text_col)) \
                  .drop(ref_data_id_col, ref_data_text_col)

# COMMAND ----------

def ground_truth_add_id_level(df_amp_vmp_vtm: DataFrame,
                              df_ground_truth: DataFrame,
                              ref_data_id_col: str,
                              ref_data_id_level_col: str,
                              new_id_level_col: str,
                              ground_truth_id_col: str
                             ) -> DataFrame:
  '''
  Add the ground truth id_level
  '''
  df_ref_data_select = df_amp_vmp_vtm.select(ref_data_id_col, ref_data_id_level_col)
  return df_ground_truth.join(df_ref_data_select, on=(df_ground_truth[ground_truth_id_col] == df_amp_vmp_vtm[ref_data_id_col]), how='left') \
                        .withColumn(new_id_level_col, col(ref_data_id_level_col)) \
                        .drop(ref_data_id_col, ref_data_id_level_col)

# COMMAND ----------

def add_ground_truth_to_lookup(df_lookup: DataFrame,
                               df_ground_truth: DataFrame,
                               join_col: str
                              ) -> DataFrame:
  '''
  Join pipeline results to ground truth.
  Set both sides to lower case before joining.
  '''
  df_lookup_ref_ground_truth = df_lookup.join(df_ground_truth, F.lower(df_lookup[join_col]) == F.lower(df_ground_truth[join_col]), 'left') \
                                        .drop(df_ground_truth[join_col])
  
  return df_lookup_ref_ground_truth