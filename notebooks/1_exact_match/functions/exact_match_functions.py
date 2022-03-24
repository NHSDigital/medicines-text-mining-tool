# Databricks notebook source
# MAGIC %run ../../_modules/epma_global/functions

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType

# COMMAND ----------

def select_distinct_tokens(df: DataFrame,
                           select_col: str,
                           token_col: str
                          ) -> DataFrame:    
  '''
  Pre-processing function to remove punctuation, in, for, and add space in between unit and strength.
  Collect the distinct tokens, sort them by order.
  This function cannot apply on null or ''.
  '''
  
  df_output = df.withColumn(token_col, F.lower(col(select_col))) \
  .withColumn(token_col, F.regexp_replace(col(token_col), '\\[|\\]|\\(|\\)|\\+|\\&| in |/| for | x |-|\\|', ' ')) \
  .withColumn(token_col, F.regexp_replace(col(token_col), r'(\d+\.?\,?\d*)', ' $1 ')) \
  .withColumn(token_col, F.array_remove(F.sort_array(F.array_distinct(F.split(col(token_col), ' '))), '')) 
  
  return(df_output)

# COMMAND ----------

def concat_array_column(df_input: DataFrame,
                        token_col: str,
                        output_col: str,
                        delimiter: str=','
                       ) -> DataFrame: 
  
  return df_input.withColumn(output_col, F.concat_ws(delimiter, col(token_col)))    

# COMMAND ----------

def get_exact_matches(df_epma_input: DataFrame,
                      df_vtm_input: DataFrame,
                      df_vmp_input: DataFrame,
                      df_amp_input: DataFrame,
                      df_vmp_prevName_input: DataFrame,
                      df_amp_prevName_input: DataFrame,
                      id_col: str,
                      original_text_col: str,
                      text_col: str,
                      form_in_text_col: str,
                      text_without_form_col: str,
                      match_id_col: str,
                      id_level_col: str,
                      match_level_col: str,
                      match_datetime_col: str,
                      token_col: str,                      
                      ref_id_col: str
                     ) -> (DataFrame, DataFrame): 
 
  '''
  Exact matching at every level of reference data: vtm, amp, vmp, amp_prev, vmp_prev.
  '''
  df_epma_input = concat_array_column(df_epma_input, token_col, '_concat_token')
  df_vtm_input = concat_array_column(df_vtm_input, token_col, '_concat_token')
  df_amp_input = concat_array_column(df_amp_input, token_col, '_concat_token')
  df_vmp_input = concat_array_column(df_vmp_input, token_col, '_concat_token')
  df_amp_prevName_input = concat_array_column(df_amp_prevName_input, token_col, '_concat_token')
  df_vmp_prevName_input = concat_array_column(df_vmp_prevName_input, token_col, '_concat_token')
  
  # After each round of matching, perform an anti join to remove the matches found at the previous level.
  
  df_vtm_matches = df_epma_input.join(df_vtm_input, '_concat_token', 'inner') \
                                .drop_duplicates([id_col])

  df_vtm_non_matches = df_epma_input.join(df_vtm_matches.select(id_col), [id_col], 'leftanti')

  df_amp_matches = df_vtm_non_matches.join(df_amp_input, '_concat_token', 'inner') \
                                     .drop_duplicates([id_col])

  df_vtm_amp_non_matches = df_vtm_non_matches.join(df_amp_matches.select(id_col), [id_col], 'leftanti')
  
  df_vmp_matches = df_vtm_amp_non_matches.join(df_vmp_input, '_concat_token', 'inner') \
                                         .drop_duplicates([id_col])

  df_vtm_amp_vmp_non_matches = df_vtm_amp_non_matches.join(df_vmp_matches.select(id_col), [id_col], 'leftanti')

  df_amp_prev_matches = df_vtm_amp_vmp_non_matches.join(df_amp_prevName_input, '_concat_token', 'inner') \
                                                  .drop_duplicates([id_col])

  df_vtm_amp_vmp_amp_prev_non_matches = df_vtm_amp_vmp_non_matches.join(df_amp_prev_matches.select(id_col), id_col, 'leftanti')

  df_vmp_prev_matches = df_vtm_amp_vmp_amp_prev_non_matches.join(df_vmp_prevName_input, '_concat_token', 'inner') \
                                                           .drop_duplicates([id_col])

  df_non_matched = df_vtm_amp_vmp_amp_prev_non_matches.join(df_vmp_prev_matches.select(id_col), [id_col], 'leftanti') \
                                                      .select(id_col, original_text_col, text_col, form_in_text_col, text_without_form_col)
    
  df_matched = df_vtm_matches.union(df_vmp_matches).union(df_amp_matches) \
                             .withColumn(match_level_col, lit('exact_by_name')) \
                             .withColumn(match_datetime_col, F.current_timestamp())

  df_matched_prev_name = df_vmp_prev_matches.union(df_amp_prev_matches) \
                                            .withColumn(match_level_col, lit('exact_by_prev_name')) \
                                            .withColumn(match_datetime_col, F.current_timestamp())

  df_matched_all = df_matched.union(df_matched_prev_name) \
                             .select(id_col, original_text_col, text_col, form_in_text_col, col(ref_id_col).alias(match_id_col),
                                     id_level_col, match_level_col, match_datetime_col)
 
  return df_matched_all, df_non_matched