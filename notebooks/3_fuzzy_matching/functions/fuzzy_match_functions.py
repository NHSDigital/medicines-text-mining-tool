# Databricks notebook source
# MAGIC %run ../../_modules/fuzzy_wuzzy/process

# COMMAND ----------

# MAGIC %run ../../_modules/fuzzy_wuzzy/fuzz

# COMMAND ----------

# MAGIC %run ../../_modules/epma_global/functions

# COMMAND ----------

# MAGIC %run ../../_modules/epma_global/ref_data_lib

# COMMAND ----------

import re
import pandas as pd
from typing import Dict, List
from functools import partial
from collections import namedtuple
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import lit, col, pandas_udf, PandasUDFType
from pyspark.sql.types import StringType, ArrayType, FloatType, StructType, StructField, LongType, IntegerType
from pyspark.broadcast import Broadcast
from pyspark.sql import DataFrame

# COMMAND ----------

def wratio_lite(p1, p2):
  ''' 
  Simplified version of WRatio from fuzzywuzzy.fuzz.
  This only considers the ratio and token_set_ratio scorers.
  In WRatio, if one string is more than 1.5 times the length of the other, then partial scorers ar tried.
  However, in out pipeline we have a threshold for WRatio of 91, which partial scorers can't achieve.
  So we can save time by not running them.
  '''
  if not validate_string(p1):
      return 0
  if not validate_string(p2):
      return 0

  unbase_scale = .95

  len_ratio = float(max(len(p1), len(p2))) / min(len(p1), len(p2))

  # if strings are not similar length, don't run ratio or token set ratio as they would never get a score above 67 (below our thresholds)
  if len_ratio > 1.5:
      return 0
  else:
      base = ratio(p1, p2)
      tser = token_set_ratio(p1, p2, full_process_bool=False) * unbase_scale
      return intr(max(base, tser))

# COMMAND ----------

def extract_one_wratio_lite(query, choices, score_cutoff=0):
  ''' 
  Extracts the best match to the query from a list of choices.
  Returns a tuple of the best choice string and it's confidence score.
  '''
  try:
      if choices is None or len(choices) == 0:
          return
  except TypeError:
      pass

  processed_query = full_process(query, force_ascii=True)
  if len(processed_query) == 0:
      logging.warning(u"Applied processor reduces input query to empty string, "
                      "all comparisons will have score 0. "
                      "[Query: \'{0}\']".format(query))

  best_list = []    
  for choice in choices:
      processed = full_process(choice, force_ascii=True)
      score = wratio_lite(processed_query, processed)
      if score >= score_cutoff:
          best_list.append((choice, score))

  try:
      return max(best_list, key=lambda i: i[1])
  except ValueError:
      return None

# COMMAND ----------

def create_fuzzy_wratio_udf_with_broadcast(check_matches_broadcast: Broadcast,
                                          output_desc_col,
                                          output_score_col):
  '''
  Create a Wratio fuzzy matching UDF that uses the given broadcast variable. It returns the single best Wratio match only.
  The output field is a struct field with the given names as the output text filed and confidence score. Access the values with dot notation.
  '''  
  return_schema = StructType([
    StructField(output_desc_col, StringType(), True),
    StructField(output_score_col, LongType(), True),
  ])
  
  @pandas_udf(return_schema)
  def _func(column_data:pd.Series) -> pd.DataFrame: 
    df_out = pd.DataFrame()
    for row in column_data:
      result = extract_one_wratio_lite(str(row), check_matches_broadcast.value)
      df_out = df_out.append(pd.DataFrame({output_desc_col:str(result[0]), output_score_col:result[1]}, index=[0]))
    return df_out 

  return _func

# COMMAND ----------

def run_wratio_fuzzy_match_to_ref_data(df_src_to_match: DataFrame,
                                      df_ref_data: DataFrame,
                                      text_col: str, 
                                      confidence_score_col: str,
                                      match_term_col: str,
                                      id_level_col: str,
                                      match_id_col: str,
                                      ref_data_id_col: str,
                                      ref_data_text_col: str
                                     ) -> DataFrame:
  '''
  Run fuzzy matching on all records in the given source data to all records in the given ref data. The two dataframes must have different 
  column names for the text field. The ref data must contain the id_level column, and that column name becomes the column name in the 
  resulting data. 
  '''
  
  if text_col == ref_data_text_col:
    raise AssertionError(f"Given source ('{text_col}') and ref data ('{ref_data_text_col}') text column names cannot be the same")
    
  match_list_bc = sc.broadcast(df_ref_data.select(F.collect_set(ref_data_text_col).alias(ref_data_text_col)).first()[ref_data_text_col])

  output_desc_col = '_desc_match'
  output_score_col = '_score_match'
  wratio_fuzzy_udf = create_fuzzy_wratio_udf_with_broadcast(match_list_bc, output_desc_col=output_desc_col, 
                                                            output_score_col=output_score_col)
  
  df_best_match = df_src_to_match.withColumn('_cs', wratio_fuzzy_udf(col(text_col))) \
                                 .withColumn(match_term_col, col(f'_cs.{output_desc_col}')) \
                                 .withColumn(confidence_score_col, col(f'_cs.{output_score_col}')) \
                                 .drop('_cs')
  
  df_res_with_ids = df_best_match.alias('lhs') \
                                 .join(df_ref_data.alias('rhs'), 
                                       on=(df_best_match[match_term_col] == df_ref_data[ref_data_text_col]), 
                                       how='left') \
                                 .select(['lhs.*', f'rhs.{id_level_col}', f'rhs.{ref_data_id_col}']) \
                                 .withColumnRenamed(ref_data_id_col, match_id_col)

  return df_res_with_ids

# COMMAND ----------

def best_match_fuzzy_wratio(ref_data_store: ReferenceDataFormatter,
                           df_src_to_match: DataFrame,
                           ref_data_level: str, 
                           text_col: str, 
                           confidence_score_col: str,
                           match_term_col: str,
                           match_level_col: str,
                           id_level_col: str,
                           match_id_col: str,
                           match_level_value: str
                           ) -> DataFrame:
  '''
  Runs Wratio fuzzy matching on the given src data to match using the reference data indicated by the given level [amp, vmp, vtm].
  '''
  id_levels = {
    'vtm' : 'VTMID',
    'vmp' : 'VPID',
    'amp_dedup' : 'APID',
  }
  
  if ref_data_level not in id_levels.keys():
    raise AssertionError(f"Given level '{ref_data_level}' is not a valid choice (choices: {list(id_levels.keys())})")
  
  df_ref_data = getattr(ref_data_store, ref_data_level)
  
  df_best_match = run_wratio_fuzzy_match_to_ref_data(df_src_to_match,
                                                     df_ref_data, 
                                                     text_col=text_col, 
                                                     confidence_score_col=confidence_score_col,
                                                     match_term_col=match_term_col,
                                                     id_level_col=id_level_col,
                                                     match_id_col=match_id_col,
                                                     ref_data_id_col=ref_data_store.ID_COL,
                                                     ref_data_text_col=ref_data_store.TEXT_COL)
  
  return df_best_match.withColumn(match_level_col, lit(match_level_value))

# COMMAND ----------

def diluent_best_match_fuzzy_wratio(ref_data_store: ReferenceDataFormatter,
                                    df_input: DataFrame, 
                                    search_term: str,
                                    text_col: str, 
                                    confidence_score_col: str,
                                    match_term_col: str,
                                    match_level_col: str,
                                    id_level_col: str,
                                    match_id_col: str,
                                    ) -> DataFrame:
  '''
  Runs Wratio fuzzy matching on the input data using only the given ref data that contains the search_term ('sodium chloride' or 'glucose' or 'phosphate').
  '''  
  df_ref_data = None
  for level in ['amp_dedup', 'vmp', 'vtm']:
    df_level = getattr(ref_data_store, level) \
               .where(col(ref_data_store.TEXT_COL).contains(search_term)) \
               .select(ref_data_store.ID_COL, ref_data_store.TEXT_COL, id_level_col)
    df_ref_data = df_level if df_ref_data is None else df_ref_data.union(df_level)

  df_best_match = run_wratio_fuzzy_match_to_ref_data(df_input,
                                                    df_ref_data, 
                                                    text_col=text_col, 
                                                    confidence_score_col=confidence_score_col,
                                                    match_term_col=match_term_col,
                                                    id_level_col=id_level_col,
                                                    match_id_col=match_id_col,
                                                    ref_data_id_col=ref_data_store.ID_COL,
                                                    ref_data_text_col=ref_data_store.TEXT_COL)
  
  return df_best_match.withColumn(match_level_col, lit('fuzzy_diluent'))

# COMMAND ----------

@pandas_udf(LongType(), PandasUDFType.SCALAR)
def fuzzy_wratio_udf(src_text_col: pd.Series, text_to_match_col: pd.Series) -> pd.Series:
  '''
  This pandas UDF runs fuzzy wuzzy WRatio on the values in the given two columns.
  '''
  return pd.Series(WRatio(src_text, text_to_match) for src_text, text_to_match in zip(src_text_col, text_to_match_col))

# COMMAND ----------

def calculate_max_confidence_score(df_records_to_score: DataFrame,
                                   confidence_score_col: str,
                                   text_col: str,
                                   match_term_col: str,
                                   id_col: str
                                  ) -> DataFrame:
  '''
  Compare text_col to match_term_col using fuzzy matching.
  Find the maximum confidence score per epma_id.

  arguments:
    df_records_to_score (DataFrame): input table. May have multiple records per epma_id, with the candidate match_terms found by partial entity matching.
    text_col (str): the input string
    match_term_col (str): candidate match terms in the reference data, found by partial entity matching.
    confidence_score_col (str): the maximum confidence score per input description.
    id_col: epma_id identifier for input records.
      
  returns:
    df_max_confidence_score: Only one record per epma_id, with the maximum score found across all match_terms. Or multiple records if the maximum score is tied.
  '''
  # Calculate confidence score
  df_confidence_score = df_records_to_score.withColumn(confidence_score_col, lit(fuzzy_wratio_udf(col(text_col), col(match_term_col))))

  # Filter on max confidence per id
  per_id_epma = Window.partitionBy(id_col)
  df_max_confidence_score =  df_confidence_score.withColumn('_max_confidence', F.max(confidence_score_col).over(per_id_epma)) \
                                                .where(col('_max_confidence') == col(confidence_score_col)) \
                                                .drop('_max_confidence')
  
  return df_max_confidence_score

# COMMAND ----------

def APID_match_candidates(df_records_to_match: DataFrame,
                          ref_data_store: ReferenceDataFormatter,
                          confidence_score_col: str,
                          original_text_col: str,
                          form_in_text_col: str,
                          text_col: str,
                          match_term_col: str,
                          match_level_col: str,
                          reason_col: str,
                          id_col: str, 
                          id_level_col: str,
                          match_id_col: str,
                          ref_data_id_col: str
                         ) -> DataFrame:
  
  """
  Records with partial entity matches at amp level are taken and checked for non tied amp maches (mapped at amp).
  Tied APID matches with 1 unique VPID are mapped at vmp.
  Tied APID and VPID matches with 1 Unique VTMID are mapped at vtm.
  The results of this function are only candidates for a match, as the will still be subject to a strength check.
  """
    
  per_id = Window.partitionBy(id_col)
  
  APID_match_candidates = df_records_to_match.where(col(match_level_col) == "APID") \
                                            .join(ref_data_store.amp.alias('amp'), col(match_id_col) == col(f'amp.{ref_data_store.ID_COL}'), how ="left") \
                                            .join(ref_data_store.vmp.alias('vmp'), col('amp.VPID') == col(f'vmp.{ref_data_store.ID_COL}'), how = "left") \
                                            .withColumn("unique_VPIDs", F.size(F.collect_set('amp.VPID').over(per_id))) \
                                            .withColumn("unique_VTMIDs", F.size(F.collect_set('vmp.VTMID').over(per_id))) \
                                            .withColumn("maximum_confidence_count_per_id", F.count('*').over(per_id)) \
                                            .drop(ref_data_store.ID_LEVEL_COL)
 

  # Non tied APID matches
  APID_matches = APID_match_candidates.where(col("maximum_confidence_count_per_id") == 1)  \
                        .withColumn(id_level_col, lit("APID")) \
                        .withColumn(reason_col, lit("unique max confidence APID")) \
                        .select(id_col, original_text_col, form_in_text_col, text_col,
                                col(f'amp.{ref_data_store.ID_COL}').alias(ref_data_id_col),
                                id_level_col, match_term_col, confidence_score_col, 'VTMID', reason_col)

  # Tied APID matches with 1 unique VPID count as a match 
  APID_tied_matches_VPID = APID_match_candidates.where((col("maximum_confidence_count_per_id") != 1) & (col("unique_VPIDs") == 1)) \
                                        .dropDuplicates([id_col, "VPID"]) \
                                        .withColumn(id_level_col, lit("VPID")) \
                                        .withColumn(reason_col, lit("tied max confidence APID, unique max confidence VPID")) \
                                        .select(id_col, original_text_col, form_in_text_col, text_col,
                                                col('amp.VPID').alias(ref_data_id_col), id_level_col, match_term_col,
                                                confidence_score_col, 'VTMID', reason_col)

  # Tied APID and VPID and Unique VTM
  APID_tied_matches_VTMID = APID_match_candidates.where(
  (col("maximum_confidence_count_per_id") != 1) & (col("unique_VPIDs") != 1) & (col("unique_VTMIDs") == 1) & col("VTMID").isNotNull()) \
                                        .dropDuplicates([id_col, 'VTMID']) \
                                        .withColumn(id_level_col, lit("VTMID")) \
                                        .withColumn(reason_col, lit("tied max confidence APID/VPID, unique max confidence VTMID")) \
                                         .select(id_col, original_text_col, form_in_text_col, text_col,
                                                 col("VTMID").alias(ref_data_id_col), id_level_col, match_term_col,
                                                 confidence_score_col, 'VTMID', reason_col)
  
  # Collate successful match candidates
  df_apid_matches=APID_matches.union(APID_tied_matches_VPID).union(APID_tied_matches_VTMID)

  return df_apid_matches

# COMMAND ----------

def VPID_match_candidates(df_records_to_match: DataFrame,
                          ref_data_store: ReferenceDataFormatter,
                          confidence_score_col: str,
                          original_text_col: str,
                          form_in_text_col: str,
                          text_col: str,
                          match_term_col: str,
                          match_level_col: str,
                          reason_col: str,
                          id_col: str, 
                          id_level_col: str,
                          match_id_col: str,
                          ref_data_id_col: str
                         )-> DataFrame:
  
  """
  Records with partial entity matches at vmp level are taken and checked for non tied vmp maches (mapped at vmp).
  Tied VPID matches with 1 unique VTMID are mapped at vtm.
  The results of this function are only candidates for a match, as the will still be subject to a strength check.
  """
  
  per_id = Window.partitionBy(id_col)
  VPID_match_candidates = df_records_to_match.where(col(match_level_col) == "VPID") \
                                            .join(ref_data_store.vmp, col(match_id_col) == col(ref_data_store.ID_COL), how ="left") \
                                            .withColumn("unique_VTMIDs", F.size(F.collect_set('VTMID').over(per_id))) \
                                            .withColumn("maximum_confidence_count_per_id", F.count('*').over(per_id)) 

  # Non tied VPID matches
  VPID_matches = VPID_match_candidates.where(col("maximum_confidence_count_per_id") == 1) \
                              .withColumn(id_level_col, lit("VPID")) \
                              .withColumn(reason_col, lit("unique max confidence VPID")) \
                              .select(id_col, original_text_col, form_in_text_col, text_col,
                                      col(ref_data_store.ID_COL).alias(ref_data_id_col), id_level_col, match_term_col, 
                                      confidence_score_col, 'VTMID', reason_col)

  # Tied VPID matches with 1 unique VTMID count as a match
  VPID_tied_matches = VPID_match_candidates.where((col("maximum_confidence_count_per_id") != 1) & (col("unique_VTMIDs") == 1) & col("VTMID").isNotNull()) \
                                   .dropDuplicates([id_col, 'VTMID']) \
                                   .withColumn(id_level_col, lit("VTMID")) \
                                   .withColumn(reason_col, lit("tied max confidence VPID, unique max confidence VTMID")) \
                                   .select(id_col, original_text_col, form_in_text_col, text_col,
                                           col("VTMID").alias(ref_data_id_col), id_level_col, match_term_col,
                                           confidence_score_col, 'VTMID', reason_col)
  
  # Collate successful match candidates
  df_vpid_matches=VPID_matches.union(VPID_tied_matches)

  return  df_vpid_matches

# COMMAND ----------

def select_best_match_from_scored_records(df_records_to_score: DataFrame,
                                          ref_data_store: ReferenceDataFormatter,
                                          confidence_score_col: str,
                                          original_text_col: str,
                                          form_in_text_col: str,
                                          text_col: str,
                                          match_term_col: str,
                                          reason_col: str,
                                          id_col: str, 
                                          id_level_col: str,
                                          match_id_col: str,
                                          match_datetime_col: str,
                                          match_level_col: str,
                                          has_strength_col: str
                                         ) -> (DataFrame, DataFrame):
  
  '''
  Fuzzy matching is applied to records with potential matches in the reference data from partial entity matching.
  For each input epma_id, choose the match_term which matches best.
  Then check that the strengths match.
  If the strengths don't match, we can still match at VTM level (if there is a corresponding VTM).    
  '''
  
  # Apply fuzzy matching and for each epma_id get the match_term that best matches. (or multiple match_terms if scores are tied)
  df_records_with_confidence_score = calculate_max_confidence_score(df_records_to_score,
                                                                    confidence_score_col=confidence_score_col,
                                                                    text_col=text_col,
                                                                    match_term_col=match_term_col,
                                                                    id_col=id_col)

  # If the best match is an APID or multiple APIDs, apply some logic to decide what level to match at.
  df_apid_matches = APID_match_candidates(df_records_with_confidence_score,
                                          ref_data_store, 
                                          confidence_score_col=confidence_score_col,
                                          original_text_col=original_text_col,
                                          form_in_text_col=form_in_text_col,
                                          text_col=text_col,
                                          match_term_col=match_term_col,
                                          match_level_col=match_level_col,
                                          reason_col=reason_col,
                                          id_col=id_col, 
                                          id_level_col=id_level_col,
                                          match_id_col=match_id_col,
                                          ref_data_id_col='ref_data_id')
  
  # Remaining records are candidates for VPID match
  VPID_records_to_match = df_records_with_confidence_score.where(col(match_level_col) == "VPID") \
                                                    .join(df_apid_matches, on = id_col, how = "leftanti")
  
  # If the best match is an VPID or multiple VPIDs, apply some logic to decide what level to match at.                                                      
  df_vpid_matches = VPID_match_candidates(VPID_records_to_match,
                                          ref_data_store,
                                          confidence_score_col=confidence_score_col,
                                          original_text_col=original_text_col,
                                          form_in_text_col=form_in_text_col,
                                          text_col=text_col,
                                          match_term_col=match_term_col,
                                          match_level_col=match_level_col,
                                          reason_col=reason_col,
                                          id_col=id_col, 
                                          id_level_col=id_level_col,
                                          match_id_col=match_id_col,
                                          ref_data_id_col='ref_data_id')

  df_match_table = df_apid_matches.union(df_vpid_matches)

  df_match_table = add_strength_match_check_column(df_match_table, 
                                                   text_col=text_col, 
                                                   match_term_col=match_term_col, 
                                                   has_strength_col=has_strength_col)

  # Remaining records go to a non match table
  df_non_match_table = DFCheckpoint(df_records_to_score.select(id_col, original_text_col, form_in_text_col, text_col, 
                                                               match_id_col, match_term_col) \
                                                       .withColumn(reason_col, 
                                                                   lit("fuzzy_linked_tied_confidence_score")) \
                                                       .join(df_match_table, id_col, how = "leftanti") \
                                                       .dropDuplicates([id_col]))
  
  # If strength match is not present and vtm is not null, vtm is chosen.
  df_ref_vtm = ref_data_store.vtm
  df_match_table = DFCheckpoint(df_match_table.withColumn(match_id_col, 
                                                          F.when(col(has_strength_col) 
                                                                 | (~col(has_strength_col) 
                                                                    & col('VTMID').isNull()), 
                                                                 col('ref_data_id')).otherwise(col('VTMID'))) \
                                              .withColumn(id_level_col, 
                                                          F.when(col(has_strength_col) 
                                                                 | (~col(has_strength_col) 
                                                                    & col('VTMID').isNull()), 
                                                                 col(id_level_col)).otherwise(lit('VTMID'))) \
                                              .withColumn(match_level_col, lit('fuzzy_linked'))\
                                              .withColumn(match_datetime_col, F.current_timestamp()) \
                                              .join(df_ref_vtm.select(ref_data_store.ID_COL, 
                                                                      ref_data_store.TEXT_COL), 
                                                    on=(df_ref_vtm[ref_data_store.ID_COL] == df_match_table['VTMID']), 
                                                    how='left') \
                                              .withColumn('match_term_debug', 
                                                          F.when(col(has_strength_col) 
                                                                 | (~col(has_strength_col) 
                                                                    & col('VTMID').isNull()), 
                                                                 col(match_term_col)).otherwise(col(ref_data_store.TEXT_COL))) \
                                              .withColumn('match_reason_debug', 
                                                          F.when(col(has_strength_col) 
                                                                 | (~col(has_strength_col) 
                                                                    & col('VTMID').isNull()), 
                                                                 col(reason_col)).otherwise(lit('Strength mismatch so map to VTMID'))) \
                                              .select(id_col, original_text_col, 
                                                      form_in_text_col, text_col, match_id_col, id_level_col, 
                                                      match_term_col, match_level_col, confidence_score_col, has_strength_col, 
                                                      col(ref_data_store.TEXT_COL).alias('NM_VTMID'), 'match_term_debug', 'match_reason_debug'))

  return df_match_table, df_non_match_table

# COMMAND ----------

@F.udf(ArrayType(StringType()))
def get_numbers_from_text_udf(text):
  '''Extracts the numbers from text and returns them as an array.
  '''
  if text is None:
    raise AssertionError('Given text is None.')

  return ['0' + number_found if number_found[0] == '.' else number_found for number_found in re.findall(r'([.]?\d[\d.]*)', text)]

# COMMAND ----------

def add_strength_match_check_column(df : DataFrame,
                                    text_col: str,
                                    match_term_col: str, 
                                    has_strength_col: str,
                                   ) -> DataFrame:
  ''' Adds a boolean column which is true only if the strengths parsed from text_col and the strengths parsed from match_term_col are the same.
  '''
  return df.withColumn('_src_numbers', get_numbers_from_text_udf(col(text_col))) \
           .withColumn('_ref_numbers', get_numbers_from_text_udf(col(match_term_col))) \
           .withColumn(has_strength_col, 
                       F.when((F.size(F.array_except('_src_numbers', '_ref_numbers')) == 0) 
                              & (F.size(F.array_except('_ref_numbers', '_src_numbers')) == 0), True).otherwise(False)) \
           .drop('_src_numbers', '_ref_numbers')

# COMMAND ----------

def best_wratio_match_at_all_levels(ref_data_store: ReferenceDataFormatter,
                                    df_non_linked_output: DataFrame,
                                    confidence_threshold,
                                    id_col: str,
                                    text_col: str,
                                    confidence_score_col: str,
                                    match_term_col: str,
                                    match_level_col: str,
                                    id_level_col: str,
                                    match_id_col: str,
                                    original_text_col: str,
                                    form_in_text_col: str,
                                    has_strength_col: str,
                                   ) -> DataFrame:
    '''
    Full fuzzy matching to VTM, then VMP, then AMP.
    '''
    best_match_fuzzy_wratio_partial = partial(best_match_fuzzy_wratio,
                                              text_col=text_col, 
                                              confidence_score_col=confidence_score_col,
                                              match_term_col=match_term_col,
                                              match_level_col=match_level_col,
                                              id_level_col=id_level_col,
                                              match_id_col=match_id_col,
                                              match_level_value='fuzzy_non_linked')
       
    # Direct Match with the VTM and extract records >= confidence threshold as matched and others as unmatched 
    vtm_non_linked = best_match_fuzzy_wratio_partial(ref_data_store, df_non_linked_output, 'vtm')
    vtm_non_linked_with_strength_check = add_strength_match_check_column(vtm_non_linked,
                                                                         text_col=text_col, 
                                                                         match_term_col=match_term_col, 
                                                                         has_strength_col=has_strength_col)
    matched_at_vtm = vtm_non_linked_with_strength_check.where((col(confidence_score_col) >= confidence_threshold) & col(has_strength_col))
    non_matched_at_vtm = vtm_non_linked.join(matched_at_vtm.select(id_col), on=(vtm_non_linked[id_col] == matched_at_vtm[id_col]), how='left_anti')
    non_matched_at_vtm = non_matched_at_vtm.drop(id_level_col, match_id_col, match_term_col, match_level_col, confidence_score_col)
      
    # Direct Match with the VMP and extract records >= confidence threshold as matched and others as unmatched 
    vmp_non_linked = best_match_fuzzy_wratio_partial(ref_data_store, non_matched_at_vtm, 'vmp')
    vmp_non_linked_with_strength_check = add_strength_match_check_column(vmp_non_linked,
                                                                         text_col=text_col, 
                                                                         match_term_col=match_term_col, 
                                                                         has_strength_col=has_strength_col)
    matched_at_vmp = vmp_non_linked_with_strength_check.where((col(confidence_score_col) >= confidence_threshold) & col(has_strength_col))
    non_matched_at_vmp = vmp_non_linked.join(matched_at_vmp.select(id_col), on=id_col, how='left_anti')
    non_matched_at_vmp = non_matched_at_vmp.drop(id_level_col, match_id_col, match_term_col, match_level_col, confidence_score_col)
    
    # Direct Match with the AMP and extract records >= confidence threshold as matched and others as unmatched 
    amp_non_linked = best_match_fuzzy_wratio_partial(ref_data_store, non_matched_at_vmp, 'amp_dedup')
    amp_non_linked_with_strength_check = add_strength_match_check_column(amp_non_linked,
                                                                         text_col=text_col, 
                                                                         match_term_col=match_term_col, 
                                                                         has_strength_col=has_strength_col)
    matched_at_amp = amp_non_linked_with_strength_check.where((col(confidence_score_col) >= confidence_threshold) & col(has_strength_col))
    
    # Collate the results
    matched = matched_at_vtm.union(matched_at_vmp).union(matched_at_amp)
    
    return matched

# COMMAND ----------

# A uniform structure for the output of each fuzzy matching stage
FunctionStepOutputStruct = namedtuple('FunctionStepOutputStruct', ['df_remaining', 'df_mappable', 'df_unmappable'])

# COMMAND ----------

def diluent_fuzzy_match_step(df_input: DataFrame,
                             ref_data_store: ReferenceDataFormatter,
                             confidence_threshold: int,
                             id_col: str,
                             original_text_col: str,
                             text_col: str, 
                             form_in_text_col: str,
                             match_term_col: str,
                             match_level_col: str,
                             id_level_col: str,
                             match_id_col: str,
                             match_datetime_col: str,
                             reason_col: str,
                             timestamp: datetime=None,
                            ) -> FunctionStepOutputStruct:
  '''
  First fuzzy matching step. It runs fuzzy matching only on a subset of input data that contains 'sodium chloride' or 'glucose' or 'phosphate' but not
  at the start of the string, and reference data that contains 'sodium chloride' or 'glucose' or 'phosphate'.  
  '''   

  # drop the match ID from the previous entity match stage.
  # The input data can contain many partial entity record matches for each id. Here we don't consider the partial 
  # matches, so drop duplicates to only select the single id and text for each original record.
  df_input_select = df_input.drop(match_id_col).dropDuplicates(subset=[id_col, text_col])
  
  df_sodium_chloride = df_input_select.where(col(text_col).contains('sodium chloride') & ~col(text_col).startswith('sodium chloride'))
                                      
  df_glucose = df_input_select.where(col(text_col).contains('glucose') & ~col(text_col).startswith('glucose'))
  
  df_phosphate = df_input_select.where(col(text_col).contains('phosphate') & ~col(text_col).startswith('phosphate'))

  df_best_match_sodium_chloride = diluent_best_match_fuzzy_wratio(
                                                      ref_data_store,
                                                      df_sodium_chloride,
                                                      search_term='sodium chloride',
                                                      text_col=text_col, 
                                                      confidence_score_col='_confidence_score',
                                                      match_term_col=match_term_col,
                                                      match_level_col=match_level_col,
                                                      id_level_col=ref_data_store.ID_LEVEL_COL,
                                                      match_id_col=match_id_col)

  df_best_match_glucose = diluent_best_match_fuzzy_wratio(
                                                      ref_data_store,
                                                      df_glucose,
                                                      search_term='glucose',
                                                      text_col=text_col, 
                                                      confidence_score_col='_confidence_score',
                                                      match_term_col=match_term_col,
                                                      match_level_col=match_level_col,
                                                      id_level_col=ref_data_store.ID_LEVEL_COL,
                                                      match_id_col=match_id_col)  

  df_best_match_phosphate = diluent_best_match_fuzzy_wratio(
                                                      ref_data_store,
                                                      df_phosphate,
                                                      search_term='phosphate',
                                                      text_col=text_col, 
                                                      confidence_score_col='_confidence_score',
                                                      match_term_col=match_term_col,
                                                      match_level_col=match_level_col,
                                                      id_level_col=ref_data_store.ID_LEVEL_COL,
                                                      match_id_col=match_id_col)  
    
  # Records without sodium chloride or glucose or phosphate go to next stage
  df_remaining = DFCheckpoint(df_input.join(df_sodium_chloride, on=id_col, how='left_anti') \
                                      .join(df_glucose, on=id_col, how='left_anti') \
                                      .join(df_phosphate, on=id_col, how='left_anti'))
    
  timestamp = F.current_timestamp() if timestamp is None else lit(timestamp)
  
  # There could be duplicate IDs for two reasons, so dropDuplicates is used.
  # Firstly if an input contains more than one of sodium chloride, glucose, and phosphate.
  # Secondly, because the diluent_best_match_fuzzy_ratio function could return two matches if there is a AMP and a VMP with the same match_term.
  # It doesn't matter whether we keep the AMP or the VMP match, as the final step in fuzzy matching handles mapping up to VMP if AMP and VMP have the same match_term.
  df_mappable = df_best_match_sodium_chloride.union(df_best_match_glucose).union(df_best_match_phosphate) \
                                             .where(col('_confidence_score') >= confidence_threshold) \
                                             .dropDuplicates([id_col]) \
                                             .withColumn(match_datetime_col, timestamp) \
                                             .select(id_col, original_text_col, form_in_text_col, text_col, match_id_col, id_level_col, match_level_col, match_datetime_col) 
  
  # Records with sodium chloride or glucose or phosphate that are below the threshold are unmappble.
  # There could be duplicates here if an input contains more than one of sodium chloride, glucose, and phosphate, so dropDuplicates is used.
  df_unmappable = df_sodium_chloride.union(df_glucose).union(df_phosphate) \
                                    .join(df_mappable, on=id_col, how='left_anti') \
                                    .dropDuplicates([id_col]) \
                                    .withColumn(reason_col, lit('fuzzy_diluent_low_score')) \
                                    .withColumn(match_datetime_col, timestamp) \
                                    .select(original_text_col, form_in_text_col, reason_col, match_datetime_col)

  return FunctionStepOutputStruct(df_remaining, df_mappable, df_unmappable)

# COMMAND ----------

def linked_fuzzy_matching_step(df_step1_remaining: DataFrame,
                               ref_data_store: ReferenceDataFormatter,
                               confidence_threshold_vtm_direct_match: int,
                               confidence_threshold_fuzzy_match: int,
                               id_col: str,
                               match_id_col: str,
                               original_text_col: str,
                               form_in_text_col: str,
                               text_col: str,
                               id_level_col: str,
                               match_level_col: str,
                               match_term_col: str,
                               match_datetime_col: str,
                               reason_col: str,
                               timestamp: datetime = None,
                              ) -> FunctionStepOutputStruct:
  '''
  Second fuzzy matching step. This takes the partially matched records and chooses the best fuzzy match with
  the refdata. vtm level data is not available as parsed data and so is missed by the entity matching.
  As a first step, there's a fuzzy match direct to VTM to see if any records match well with VTM missed by the entity 
  stage.
  '''
  
  # Full fuzzy match at vtm level, on the linked records with a partial entity match 
  # (those without a partial entity match will be picked up by full fuzzy matching later anyway)
  df_linked_output = df_step1_remaining.where(col(match_id_col).isNotNull())

  # Drop the match_ids for the linked matches. We want to do fuzzy matching against all refdata vtms.
  df_linked_matches_to_match_to_vtm = df_linked_output.drop_duplicates([id_col]).drop(match_id_col)
  df_best_fuzzy_match_vtm = best_match_fuzzy_wratio(ref_data_store,
                                                    df_linked_matches_to_match_to_vtm,
                                                    'vtm',
                                                    text_col=text_col,
                                                    confidence_score_col='_confidence_score',
                                                    match_term_col=match_term_col,
                                                    match_level_col=match_level_col,
                                                    id_level_col=id_level_col,
                                                    match_id_col=match_id_col,
                                                    match_level_value='fuzzy_linked_vtm')
  
  # Check for strength match on any vtm matches
  df_strength_match_vtm = add_strength_match_check_column(df_best_fuzzy_match_vtm, 
                                                          text_col=text_col, 
                                                          match_term_col=match_term_col, 
                                                          has_strength_col='_has_strength'
                                                         ).where(col('_has_strength'))

  # Filter for successful matches using confidence threshold and strength check
  timestamp = F.current_timestamp() if timestamp is None else lit(timestamp)
  df_matched_at_vtm = DFCheckpoint(df_strength_match_vtm.where(col('_confidence_score') >= confidence_threshold_vtm_direct_match) \
                                                        .withColumn(match_datetime_col, timestamp) \
                                                        .select(id_col, original_text_col, form_in_text_col, 
                                                                text_col, match_id_col, id_level_col, 
                                                                match_level_col, match_datetime_col))
  
  # Remove any vtm matches from the partial entity matches.
  df_linked_output_less_vtm_matches = df_linked_output.join(df_matched_at_vtm.df, 
                                                            on=(df_linked_output[id_col] == df_matched_at_vtm.df[id_col]), 
                                                            how='left_anti')
  
  # Take the linked records (with match id) and fetch records with high score.
  df_linked_match_fuzzy_matches, df_linked_match_fuzzy_non_matches = select_best_match_from_scored_records(
                                                                         df_linked_output_less_vtm_matches, 
                                                                         ref_data_store,
                                                                         confidence_score_col='_confidence_score',
                                                                         original_text_col=original_text_col,
                                                                         form_in_text_col=form_in_text_col,
                                                                         text_col=text_col,
                                                                         match_term_col=match_term_col,
                                                                         reason_col=reason_col,
                                                                         id_col=id_col, 
                                                                         id_level_col=id_level_col,
                                                                         match_id_col=match_id_col,
                                                                         match_datetime_col=match_datetime_col,
                                                                         match_level_col=match_level_col,
                                                                         has_strength_col='_has_strength')

  # Filter by confidence threshold and strength match
  # If strength didn't match and there wasn't a VTM, then we can't match at this stage
  df_linked_match_fuzzy_matches_above_threshold = df_linked_match_fuzzy_matches.df.where(
    (col('_confidence_score') > confidence_threshold_fuzzy_match) & (col('_has_strength') | (col(id_level_col) == 'VTMID'))) \
    .withColumn(match_datetime_col, timestamp) \
    .select(id_col, original_text_col, form_in_text_col, text_col, match_id_col, id_level_col, 
            match_level_col, match_datetime_col)
  
  df_mappable = DFCheckpoint(df_matched_at_vtm.df.union(df_linked_match_fuzzy_matches_above_threshold))
  
  df_unmappable = DFCheckpoint(df_linked_match_fuzzy_non_matches.df.select(id_col, original_text_col, form_in_text_col, reason_col) \
                                                                    .withColumn(match_datetime_col, timestamp))

  del df_matched_at_vtm
  del df_linked_match_fuzzy_matches
  del df_linked_match_fuzzy_non_matches

  df_remaining = df_step1_remaining.drop_duplicates([id_col]) \
                                   .join(df_mappable.df, on=(df_step1_remaining[id_col] == df_mappable.df[id_col]), how='left_anti') \
                                   .join(df_unmappable.df, on=(df_step1_remaining[id_col] == df_unmappable.df[id_col]), how='left_anti') \
  
  df_unmappable_cp = DFCheckpoint(df_unmappable.df.drop(id_col))
  del df_unmappable
  
  return FunctionStepOutputStruct(df_remaining, df_mappable, df_unmappable_cp)

# COMMAND ----------

def full_fuzzy_matching_step(ref_data_store: ReferenceDataFormatter,
                             df_step2_remaining,
                             confidence_threshold,
                             id_col: str,
                             original_text_col: str,
                             form_in_text_col: str,
                             text_col: str,
                             match_term_col: str,
                             match_level_col: str,
                             id_level_col: str,
                             match_id_col: str,
                             match_datetime_col: str):
  ''' 
  Full fuzzy matching. For records without a partial entity match, or records which had a linked match lower than the threshold.
  '''
  
  df_mappable = best_wratio_match_at_all_levels(ref_data_store,
                                                df_step2_remaining,
                                                confidence_threshold,
                                                id_col=id_col,
                                                text_col=text_col,
                                                confidence_score_col='_confidence_score',
                                                match_term_col=match_term_col,
                                                match_level_col=match_level_col,
                                                id_level_col=id_level_col,
                                                match_id_col=match_id_col,
                                                original_text_col=original_text_col,
                                                form_in_text_col=form_in_text_col,
                                                has_strength_col='_has_strength')
  
  df_mappable = DFCheckpoint(df_mappable.withColumn(match_datetime_col, F.current_timestamp()) \
                                        .select(id_col, original_text_col, form_in_text_col, text_col, 
                                                match_id_col, id_level_col, match_level_col, match_datetime_col))
  
  df_remaining = df_step2_remaining.join(df_mappable.df, on=(df_step2_remaining[id_col] == df_mappable.df[id_col]), how='left_anti') 
  
  return FunctionStepOutputStruct(df_remaining, df_mappable, None)

# COMMAND ----------

def get_non_moiety_words(table_form='dss_corporate.form',
                         table_route='dss_corporate.route',
                         table_unit='dss_corporate.unit_of_measure'):

  # Non moiety words are found in these three tables.
  list_form = spark.table(table_form).select('DESC')
  list_route = spark.table(table_route).select('DESC')
  list_unit = spark.table(table_unit).select('DESC')

  list_form_route_unit = list_form.union(list_route).union(list_unit).select(F.collect_set(F.lower(col('DESC'))).alias('DESC')).first()['DESC']

  # Plus an additional custom list of words.
  list_extra = [
                # words found in the parsed reference data
                'and', 'daily', 'dose', 'dry', 'extra', 'flavour', 'for', 'formula', 'free', 'generic', 'in', 'inhaler', 'injection', 'kwikpen', 
                'lemon', 'max',  'mint', 'normal', 'original', 'patient', 'pen', 'penfill', 'plus', 'pre-filled', 'release', 'solution', 'strength',  
                'strong', 'weight', 'with', 
                # words found in source_b samples
                'actuated', 'add', 'adult', 'advance', 'breath', 'cart', 'cfc', 'chewable', 'comment', 'dosealation', 'effervescent', 'fluid', 
                'infant', 'injectable', 'jelly', 'level', 'lubricating', 'modified', 'non-adhesive', 'only', 'per', 'pre-dose', 'ribbon',  
                'scalp', 'shower', 'sugar',  'suppositories', 'tube', 'testing',                  
                # words which could be moieties but more likely to be flavours
                'blackcurrant', 'orange', 'peppermint', 'raspberry', 'syrup'
               ]

  #   If it is a compound word e.g. 'a/b' or 'a-b', split it into tokens.
  list_with_punc = [word for word in list_form_route_unit + list_extra if ('-' in word) or ('/' in word)]
  list_with_punc_expand = []
  for word in list_with_punc:
    list_with_punc_expand += re.split('/|-| ', word)
      
  list_all = list(set(list_form_route_unit + list_extra + list_with_punc_expand))
  non_moiety_words = list_all + [word + 's' for word in list_all] + [word + 'es' for word in list_all]
  
  return non_moiety_words

# COMMAND ----------

def remove_strength_unit(input_col):

  output_col = F.regexp_replace(input_col, '(([^a-zA-Z]\\d?\\.?\\d+)+)\\s?([a-zA-Z]*\\/?\\.?[a-zA-Z]*\\-?\\%?\\.?)', '')
  
  return output_col

# COMMAND ----------

def remove_non_moiety_words(df_input: DataFrame,
                            non_moiety_words: list,
                            text_col: str,
                            clean_text_col: str
                           ) -> DataFrame:
  
  # First remove anything contained within brackets
  df_input_clean = df_input.withColumn('_text_without_brackets', F.regexp_replace(text_col, r'\(.*\)', ''))
  
  # Then remove non moiety words
  df_input_clean = df_input_clean.withColumn('_split', F.lower(col('_text_without_brackets'))).withColumn('_split', F.split(col('_split'),' ')) \
  .withColumn('_ls_to_remove', F.array([lit(word) for word in non_moiety_words])) \
  .withColumn(clean_text_col, F.concat_ws(' ', F.array_except('_split', '_ls_to_remove'))) \
  .drop('_text_without_brackets', '_split', '_ls_to_remove')
  
  return df_input_clean

# COMMAND ----------

def get_records_with_moieties(df_input: DataFrame,
                              non_moiety_words: list,
                              text_col: str,
                              clean_text_col: str
                             ) -> (DataFrame, DataFrame):
  ''' 
  Remove non moiety words, creating clean_text_col from text_col.
  If clean_text_col is empty, then the record is put in a separate dataframe, and will later be set as unmappable.
  '''
  
  df_clean = DFCheckpoint(remove_non_moiety_words(df_input, non_moiety_words, text_col=text_col, clean_text_col=clean_text_col) \
                          .withColumn(clean_text_col, remove_strength_unit(clean_text_col)))
  
  df_records_without_moieties = df_clean.df.where(col(clean_text_col) == '').drop(clean_text_col)
  df_records_with_moieties = df_clean.df.where(col(clean_text_col) != '')

  # df_clean is returned so that it can be cached, to speed up calculations
  return df_records_with_moieties, df_records_without_moieties, df_clean

# COMMAND ----------

def get_moieties_only_from_amp_vmp_ref_data(ref_data_store: ReferenceDataFormatter,
                                            non_moiety_words: list,
                                            id_level_col: str,
                                            moiety_col: str
                                           ) -> DataFrame:
  ''' 
  Get moieties from the reference data.
  From the amp and vmp parsed tables, get the text from the moiety columns and remove any strengths and units that may be in there.
  For records not present in the parsed tables, extract the moieties from the input description by removing non-moiety words, and removing strengths and units.
  '''
  
  # Get moieties from parsed files and remove strengths and units
  df_amp_parsed = ref_data_store.amp_parsed.withColumn('_moiety_all', F.concat_ws(' ', col('MOIETY'), col('MOIETY_2'), 
                                                                                   col('MOIETY_3'), col('MOIETY_4'), col('MOIETY_5'))) \
                                             .withColumn('_moiety_clean', remove_strength_unit('_moiety_all')) \
                                             .withColumn(id_level_col, lit('APID')) \
                                             .select(ref_data_store.ID_COL, ref_data_store.TEXT_COL, '_moiety_clean', id_level_col)
  
  df_vmp_parsed = ref_data_store.vmp_parsed.withColumn('_moiety_all', F.concat_ws(' ', col('MOIETY'), col('MOIETY_2'), 
                                                                                   col('MOIETY_3'), col('MOIETY_4'), col('MOIETY_5'))) \
                                             .withColumn('_moiety_clean', remove_strength_unit('_moiety_all')) \
                                             .withColumn(id_level_col, lit('VPID')) \
                                             .select(ref_data_store.ID_COL, ref_data_store.TEXT_COL, '_moiety_clean', id_level_col) 
 
  df_parsed = df_amp_parsed.union(df_vmp_parsed)
  
  # For records without a parsed record
  df_amp_dedup = ref_data_store.amp_dedup.select(col('APID').alias(ref_data_store.ID_COL), ref_data_store.TEXT_COL, ref_data_store.ID_LEVEL_COL)
  df_vmp = ref_data_store.vmp.select(ref_data_store.ID_COL, ref_data_store.TEXT_COL, ref_data_store.ID_LEVEL_COL)
  df_not_parsed = df_amp_dedup.union(df_vmp).join(df_parsed, on=ref_data_store.ID_COL, how='left_anti')
  
  # Get moieties from the non-parsed records, by removing non-moiety words, strengths, and units.
  df_not_parsed_moiety = remove_non_moiety_words(df_not_parsed, non_moiety_words, text_col=ref_data_store.TEXT_COL, clean_text_col='_clean_text') \
                        .withColumn('_moiety_clean', remove_strength_unit('_clean_text')) \
                        .drop('_clean_text')
  
  # Join the parsed and non-parsed
  df_moiety_all = df_parsed.unionByName(df_not_parsed_moiety)
  
  # Concatenating the moiety columns with whitespace can create some unecessary whitespace. Remove it here with trim.
  # Also remove anything within brackets so that we get more chance of a match (e.g. remove "(base)" from "Adrenaline (base)")
  df_moiety_all_no_space = df_moiety_all.withColumn(moiety_col, F.trim(col('_moiety_clean'))) \
                                        .withColumn(moiety_col, F.regexp_replace(moiety_col, r'\(.*\)', '')) \
                                        .drop('_moiety_clean')
  
  return df_moiety_all_no_space

# COMMAND ----------

def map_moiety_ref_data_to_vtm(df_amp_vmp_ref_data_moieties_only: DataFrame,
                               ref_data_store: ReferenceDataFormatter,
                               ref_vtm_text_col: str,
                               moiety_col: str
                              ) -> DataFrame:
  ''' 
  Create a dataframe which takes moieties found in amp and vmp reference data and maps them up to their corresponding vtm.
  The input dataframe is the moieties from the reference data.
  '''
  df_amp_dedup = ref_data_store.amp_dedup.select('VPID', col('APID').alias(ref_data_store.ID_COL))
  df_vmp = ref_data_store.vmp.select(col(ref_data_store.ID_COL).alias('VPID'), 'VTMID')
  df_vtm = ref_data_store.vtm.select(col(ref_data_store.ID_COL).alias('VTMID'), col(ref_data_store.TEXT_COL).alias(ref_vtm_text_col))
  
  # For moieties found in amp records, join moieties to amp to vmp to vtm
  df_moiety_amp_with_vtm = df_amp_vmp_ref_data_moieties_only.where(col(ref_data_store.ID_LEVEL_COL) == 'APID') \
                               .join(df_amp_dedup, on=ref_data_store.ID_COL, how='left') \
                               .join(df_vmp, on='VPID', how='left') \
                               .join(df_vtm, on='VTMID', how='left') \
                               .select(ref_data_store.ID_COL, ref_data_store.TEXT_COL, moiety_col, ref_vtm_text_col, 'VTMID')
  
  # For moieties found in vmp records, join moieties to vmp to vtm
  df_moiety_vmp_with_vtm = df_amp_vmp_ref_data_moieties_only.where(col('id_level') == 'VPID') \
                               .join(df_vmp, on=(df_vmp['VPID'] == df_amp_vmp_ref_data_moieties_only[ref_data_store.ID_COL]), how='left') \
                               .join(df_vtm, on='VTMID', how='left') \
                               .select(ref_data_store.ID_COL, ref_data_store.TEXT_COL, moiety_col, ref_vtm_text_col, 'VTMID')
    
  df_moiety_ref_data_with_vtm = df_moiety_amp_with_vtm.union(df_moiety_vmp_with_vtm)
  
  return df_moiety_ref_data_with_vtm

# COMMAND ----------

def split_by_unique_vtm(df_epma_with_moiety_matches: DataFrame,
                        df_moiety_ref_data_with_vtm: DataFrame,
                        id_col: str,
                        id_level_col: str,
                        join_col: str,
                        ref_vtm_text_col: str
                       ) -> (DataFrame, DataFrame):
  ''' 
  Join the moieties from input descriptions to the moieties from reference data.
  If they map up to a unique vtm in the reference data, then we have a match at vtm level.
  '''
  
  # Join the moieties from input descriptions to the moieties from reference data.
  # Use a window function to find out how many vtms each moiety maps up to.
  per_id = Window.partitionBy(id_col)
  df_match_refer_vtm = DFCheckpoint(df_epma_with_moiety_matches.join(df_moiety_ref_data_with_vtm, 
                                                                     join_col, 
                                                                     'left') \
                                                               .withColumn('_vtm_count', 
                                                                           F.size(F.collect_set(ref_vtm_text_col).over(per_id))))
  
  # Filter for where only a unique vtm is found
  df_match_vtm_unique = DFCheckpoint(df_match_refer_vtm.df.where(col('_vtm_count') == 1) \
                                                       .drop('_vtm_count') \
                                                       .where(col(ref_vtm_text_col).isNotNull()) \
                                                       .drop_duplicates([id_col]) \
                                                       .withColumn(id_level_col, lit('VTMID')))
  
  df_match_vtm_no_unique = df_match_refer_vtm.df.where(col('_vtm_count') != 1) \
                                             .drop(ref_vtm_text_col, '_vtm_count') \
                                             .drop_duplicates([id_col])
  
  # df_match_refer_vtm is also returned, so that it can be cached to speed up calculations
  return df_match_vtm_unique, df_match_vtm_no_unique, df_match_refer_vtm

# COMMAND ----------

def create_fuzzy_sort_ratio_udf_with_broadcast(check_matches_broadcast:Broadcast):
  '''
  Create a set ratio fuzzy matching UDF that uses the given broadcast variable. It returns the single best set ratio match only.
  The output field is a struct field with the given names as the output text field and confidence score. Access the values with dot notation.
  '''

  return_schema = StructType([
    StructField('Item', StringType(), True),
    StructField('Score', LongType(), True) 
  ]) 

  @pandas_udf(return_schema)
  def _func(column_data:pd.Series) -> pd.DataFrame: 
    df_out = pd.DataFrame()
    for row in column_data:
      result = extractOne(query=str(row), choices=check_matches_broadcast.value, processor=full_process, scorer=token_sort_ratio) 
      df_out = df_out.append(pd.DataFrame({'Item': str(result[0]), 'Score': result[1]}, index=[0]))
    return df_out

  return _func

# COMMAND ----------

def sort_ratio_fuzzy_match(df_input: DataFrame, 
                          df_moieties: DataFrame, 
                          text_col: str, 
                          confidence_score_col: str, 
                          moiety_col: str
                         ) -> DataFrame:
  ''' Run fuzzy matching between every pair from df_input and df_moieties.
  The fuzzy matching scorer is token set ratio.
  The moieties are turned into a broadcast variable.
  '''
  ls_moieties = sc.broadcast(df_moieties.select(F.collect_set(moiety_col).alias(moiety_col)).first()[moiety_col])
  
  fuzzy_udf_extractOne = create_fuzzy_sort_ratio_udf_with_broadcast(ls_moieties)
  
  df_input_match = df_input.withColumn('_match', fuzzy_udf_extractOne(text_col)) \
                           .withColumn(moiety_col, col('_match.Item')) \
                           .withColumn(confidence_score_col, col('_match.Score')) \
                           .drop('_match')

  return df_input_match

# COMMAND ----------

def moiety_sort_ratio_fuzzy_match_step(df_input: DataFrame,
                                      ref_data_store: ReferenceDataFormatter,
                                      confidence_threshold: int,
                                      original_text_col: str,
                                      form_in_text_col: str,
                                      text_col: str,
                                      id_col: str,
                                      id_level_col: str,
                                      match_level_col: str,
                                      match_datetime_col: str,
                                      match_id_col: str,
                                      reason_col: str,
                                      timestamp: datetime=None
                                      ) -> FunctionStepOutputStruct:
  '''
  Last fuzzy matching step.
  It strips everything except moieties from the input descriptions and from the refdata.
  Then matches these to each other using sort ratio (so can match the moiety words in any order).
  '''   

  non_moiety_words = get_non_moiety_words()

  # Extract the moieties from reference data. Store them in moiety_col.
  df_amp_vmp_ref_data_moieties_only = DFCheckpoint(get_moieties_only_from_amp_vmp_ref_data(ref_data_store,
                                                                                           non_moiety_words,
                                                                                           id_level_col=id_level_col,
                                                                                           moiety_col='_moiety'))
  
  # Where amp or vmp data contains a moiety, map the moiety up to the corresponding vtm.
  df_moiety_ref_data_with_vtm = map_moiety_ref_data_to_vtm(df_amp_vmp_ref_data_moieties_only.df,
                                                           ref_data_store, 
                                                           ref_vtm_text_col='_ref_vtm_description', 
                                                           moiety_col='_moiety')
  
  # Make a deduplicated list of the moieties present in the refdata. Using a shorter list will make the fuzzy matching quicker
  df_moieties_present_in_refdata = df_amp_vmp_ref_data_moieties_only.df.select('_moiety').distinct()
  
  # Get moieties from input records, store them in clean_text_col.
  # Where clean_text_col is empty, these records will be unmappable.
  df_records_with_moieties, df_records_without_moieties, df_get_records_with_moieties_cp = get_records_with_moieties(df_input, 
                                                                                                                     non_moiety_words, 
                                                                                                                     text_col=text_col, 
                                                                                                                     clean_text_col='_clean_text') 
  # Run fuzzy matching, comparing the moieties from input data to the list of moieties from reference data
  df_moiety_matches = sort_ratio_fuzzy_match(df_records_with_moieties,
                                            df_moieties_present_in_refdata, 
                                            text_col='_clean_text', 
                                            confidence_score_col='_confidence_score', 
                                            moiety_col='_moiety')
  
  # If the best scoring moiety maps up to a unique vtm, then we have a match at vtm level. Otherwise unmappable.
  df_matches_with_unique_vtm, df_matches_with_no_unique_vtm, df_split_by_unique_vtm_cp = split_by_unique_vtm(df_moiety_matches,
                                                                                                             df_moiety_ref_data_with_vtm, 
                                                                                                             id_col=id_col,
                                                                                                             id_level_col=id_level_col,
                                                                                                             join_col='_moiety', 
                                                                                                             ref_vtm_text_col='_ref_vtm_description')
  
  timestamp = F.current_timestamp() if timestamp is None else lit(timestamp)

  # Unmappables can be for three reasons at this stage.
  # Add the reason column and join them.
  df_matches_with_no_unique_vtm = df_matches_with_no_unique_vtm \
    .select(original_text_col, form_in_text_col) \
    .withColumn(reason_col, lit('fuzzy_moiety_no_unique_vtm')) \
    .withColumn(match_datetime_col, timestamp)

  df_low_score = df_matches_with_unique_vtm.df.filter(col('_confidence_score') < confidence_threshold) \
    .select(original_text_col, form_in_text_col) \
    .withColumn(reason_col, lit('fuzzy_moiety_low_score')) \
    .withColumn(match_datetime_col, timestamp)
  
  df_records_without_moieties = df_records_without_moieties \
    .select(original_text_col, form_in_text_col) \
    .withColumn(reason_col, lit('fuzzy_moiety_no_moieties')) \
    .withColumn(match_datetime_col, timestamp)

  df_unmappable = DFCheckpoint(df_matches_with_no_unique_vtm.union(df_low_score).union(df_records_without_moieties))

  # Mappable records are those with a unique vtm and a confidence score above the threshold.
  df_mappable = DFCheckpoint(df_matches_with_unique_vtm.df.filter(col('_confidence_score') >= confidence_threshold) \
                                                          .select(id_col, original_text_col, form_in_text_col, text_col, col('VTMID').alias(match_id_col), id_level_col) \
                                                          .withColumn(match_level_col, lit('fuzzy_moiety_vtm')) \
                                                          .withColumn(match_datetime_col, timestamp))
  
  del df_amp_vmp_ref_data_moieties_only
  del df_get_records_with_moieties_cp
  del df_matches_with_unique_vtm
  del df_split_by_unique_vtm_cp
  
  # Check the number of records input to this function is the same as the number output.
  # This is a useful check because there ae many filters and joins. 
  input_count = df_input.count()
  output_count = df_mappable.df.count() + df_unmappable.df.count()
  if input_count != output_count:
    raise AssertionError(f"Fuzzy matching step 4. The number of input rows: '{input_count}' is not equal to the number of output rows: {output_count}")

  return FunctionStepOutputStruct(None, df_mappable, df_unmappable)

# COMMAND ----------

def map_amp_to_vmp_if_there_are_amp_desc_duplicates_or_matching_vmp_desc(df_match_lookup: DataFrame,
                                                                         ref_data_store: ReferenceDataFormatter,
                                                                         id_col: str,
                                                                         original_text_col: str,
                                                                         form_in_text_col: str,
                                                                         text_col: str,
                                                                         id_level_col: str,
                                                                         match_level_col: str,
                                                                         match_datetime_col: str,
                                                                         match_id_col: str,
                                                                        ) -> DataFrame:
  ''' Throughout the pipeline we have sometimes used deduplicated versions of amp or amp_parsed, where the amp records have the same name.
  This was to save duplicated effort in fuzzy matching. Now, however, we need to know if there were multiple amps with the same name.
  '''

  amp_desc_col = '_amp_description'
  vmp_desc_col = '_vmp_description'
  df_amp = ref_data_store.amp.select(ref_data_store.ID_COL, col(ref_data_store.TEXT_COL).alias(amp_desc_col), 'APID', 'VPID')
  df_vmp = ref_data_store.vmp.select(ref_data_store.ID_COL, col(ref_data_store.TEXT_COL).alias(vmp_desc_col))
  
  df_amp_vmp = df_amp.join(df_vmp, on=(df_amp['VPID'] == df_vmp[ref_data_store.ID_COL]), how='left') \
                     .select('APID', 'VPID', amp_desc_col, vmp_desc_col)
  
  #get the count of the number of APIDS that have the same description.
  window_desc_count = Window.partitionBy(amp_desc_col)
  df_amp_vmp_wdesc_count = df_amp_vmp.withColumn('_apid_count', F.size(F.collect_set('APID').over(window_desc_count)))
  
  df_match_lookup_with_ref_data = df_match_lookup.where((col(id_level_col) == 'APID')) \
                                                 .join(df_amp_vmp_wdesc_count, 
                                                       on=(df_match_lookup[match_id_col] == df_amp_vmp_wdesc_count['APID']), 
                                                       how='left')
  
  # If there are multiple amps with the same description and they map up to a unique vmp, then we should report at vmp level.
  # If we matched to a uniquely named amp and this maps to a vmp with the same description, then we should report at vmp level.
  # There are no AMP records with a null VPID
  df_map_to_vmp = df_match_lookup_with_ref_data.where((col('_apid_count') > 1) 
                                                      | ((col('_apid_count') == 1) 
                                                         & (col(amp_desc_col) == col(vmp_desc_col))
                                                        )) \
                                               .withColumn(match_id_col, col('VPID')) \
                                               .withColumn(id_level_col, lit('VPID')) \
                                               .select(id_col, original_text_col, form_in_text_col, text_col, match_id_col, 
                                                       id_level_col, match_level_col, match_datetime_col) 
  
  df_not_mapped = df_match_lookup.join(df_map_to_vmp, on=id_col, how='left_anti')
  
  return df_not_mapped.union(df_map_to_vmp)

# COMMAND ----------

def add_match_term(df_input: DataFrame, 
                   ref_data_store: ReferenceDataFormatter,
                   id_col: str,
                   original_text_col: str,
                   form_in_text_col: str,
                   text_col: str, 
                   match_id_col: str,
                   match_term_col: str,
                   id_level_col: str,
                   match_level_col: str,
                   match_datetime_col: str,
                   ) -> DataFrame:
  ''' 
  Throughout the pipeline we have not kept the match_term column. So now we should get it by joining to reference data on the match_id.
  '''
  df_amp_vmp_vtm = ref_data_store.amp.select(ref_data_store.ID_COL, ref_data_store.TEXT_COL) \
    .union(ref_data_store.vmp.select(ref_data_store.ID_COL, ref_data_store.TEXT_COL)) \
    .union(ref_data_store.vtm.select(ref_data_store.ID_COL, ref_data_store.TEXT_COL))

  df_output = df_input.join(df_amp_vmp_vtm, [df_input[match_id_col]==df_amp_vmp_vtm[ref_data_store.ID_COL]], 'left') \
    .select(id_col, original_text_col, form_in_text_col, text_col, match_id_col, col(ref_data_store.TEXT_COL).alias(match_term_col), id_level_col, match_level_col, match_datetime_col)

  return df_output