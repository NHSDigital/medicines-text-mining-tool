# Databricks notebook source
# MAGIC %run ../../_modules/epma_global/functions

# COMMAND ----------

from itertools import product
from collections import namedtuple
import re
from functools import reduce
from operator import and_, add
from pyspark.sql.types import ArrayType, StringType, BooleanType
from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F
from pyspark import Broadcast
from pyspark.sql.window import Window
from typing import List, Union
import warnings

# COMMAND ----------

def remove_special_characters(string_to_clean: str) -> str:
  return re.sub('[()/-]*', '', string_to_clean).strip()

# COMMAND ----------

def find_strength_unit_pairs_regex(string_to_search: str) -> str:
  '''(([^a-zA-Z]\\d?\\.?\\d+)+) - Extract numbers/decimals with no units
  ([a-zA-Z]*\\/?\\.?[a-zA-Z]*\\-?\\%?\\.?) - Extract words or % after the number followed by with or without spaces.
  '''
  return re.findall("(([^a-zA-Z]\\d?\\.?\\d+)+)\\s?([a-zA-Z]*\\/?\\.?[a-zA-Z]*\\-?\\%?\\.?)", string_to_search)

# COMMAND ----------

def three_moieties_separated_by_slashes(string_to_search: str) -> str:
  '''check for the pattern - moeity seperated by "/"
  Eg: dexamethasone/framycetin/gramicidin 0.05%-0.5%-0.005%. or dexamethasone/framycetin 0.05%-0.5% ear/eye.
  '''
  return re.search('([a-zA-Z]*\\/+[a-zA-Z]*\\/+[a-zA-Z]*)', string_to_search)

# COMMAND ----------

def two_moieties_separated_by_slashes(string_to_search: str) -> str:
  '''check for the pattern - moeity seperated by "/"
  Eg: dexamethasone/framycetin/gramicidin 0.05%-0.5%-0.005%. or dexamethasone/framycetin 0.05%-0.5% ear/eye
  '''
  return re.search('((^\\d)*[a-zA-Z]*\\/+(^\\d)*[a-zA-Z]*)', string_to_search)

# COMMAND ----------

def moiety_separated_by_dash(string_to_search: str) -> str:
  '''check for the pattern - moeity seperated by "-" and moeity> 2 characters
  Eg: betamethasone-calcipotriol 0.05%-0.005% topical foam'.
  '''
  return re.search('([a-zA-Z]{2,}\\-+[a-zA-Z]{3,})', string_to_search)

# COMMAND ----------

def check_for_dose_form(input_string: str,
                        unique_doseforms: List[str]
                       ) -> bool:
  return any([
    doseform for doseform in unique_doseforms 
    if doseform.lower() in input_string.lower()
  ])

# COMMAND ----------

# List of fields which can be populated with strength and unit pairs.
# We use a named tuple here because the strength unit values can take no other fields. 
# A named tuple allows us to enforce the structure of the outputs for the function which extracts the strength unit pairs and raise an error if an extra pair is found.
FIELDS = ('SVN', 'SVU', 'SVN2', 'SVU2', 'SVD', 'SDU', 'SVN_2', 'SVU_2', 'SVD_2', 'SDU_2', 'SVN_3', 'SVU_3')
FormattedStrengthUnitValues = namedtuple('strength_unit_values', FIELDS, defaults=(None,)*len(FIELDS))

# COMMAND ----------

def find_all_strength_unit_pairs(substrings_to_search: Union[str, List[str]]) -> list:
  ''' Parses the substrings to find strength and unit pairs to populate the corresponding fields.
  Pairs of fields are populated in a specific order, e.g. the first strenth and unit pair in the first substring is assigned to SVN and SVU
  '''
  
  if isinstance(substrings_to_search, str):
    substrings_to_search = [substrings_to_search]
  
  formatted_matches_dict = {}
  for substring_number, substring in enumerate(substrings_to_search):
    strength_and_unit_pairs = find_strength_unit_pairs_regex(substring)
    if len(strength_and_unit_pairs) > 0:
      for match_number, match in enumerate(strength_and_unit_pairs):
        if match_number > 2:
          break
        if substring_number == 0 or len(substrings_to_search) == 1:
          strength = 'SVN' if match_number == 0 else ('SVN_' + str(match_number+1))
          unit = 'SVU' if match_number == 0 else ('SVU_' + str(match_number+1))
        else:
          strength = 'SVD' if substring_number == 2 else ('SVD_' + str(substring_number+1))
          unit = 'SDU' if substring_number == 2 else ('SDU_' + str(substring_number+1))

        formatted_matches_dict[strength] = remove_special_characters(match[0])
        formatted_matches_dict[unit] = remove_special_characters(match[2])
  
  for key in formatted_matches_dict.keys():
    if key not in FIELDS:
      warnings.warn(f'At least one of the token labels ({formatted_matches_dict}) is not recognised for input {substrings_to_search}.', 
                   RuntimeWarning)
      return FormattedStrengthUnitValues()      
  else:
    return FormattedStrengthUnitValues(**formatted_matches_dict)

# COMMAND ----------

def split_denominators(data: str,
                       doseform: List[str]
                      ) -> list:
  
  #If there is "+", split tokens by +
  if re.search('[+]', data):
    tokens = re.split('[+]', data)
  #If there is "/" , split by "/"
  elif re.search('(\\s?\\/\\s?)', data):
    tokens = re.split('(\\s?\\/\\s?)', data)   
  #if there is  "in" and doseform, dont split 
  elif re.search('(\\sin\\s)', data) and check_for_dose_form(data, doseform):
     tokens = [data]
  #if there is "in" split by "in"
  elif re.search('(\\sin\\s)', data):
    tokens = re.split('(\\sin\\s)', data)
  else:
    tokens = [data]
  #Check if there is (number)st,nd,rd,th then drop them from extraction. eg: for Paracetamol O/D-3rd 
  tokens = [re.sub('\\d+\\s*rd|\\d+\\s*st|\\d+\\s*th|\\d+\\s*nd', '', token) for token in tokens]
            
  return tokens

# COMMAND ----------

def extract_strength_unit(data: str,
                          doseform: list
                         ) -> list:
  """extract strength and unit with different patterns
  check for the pattern - moeity seperated by "/"
  Eg: dexamethasone/framycetin/gramicidin 0.05%-0.5%-0.005%. or dexamethasone/framycetin 0.05%-0.5% ear/eye
  Each strength(number) will be seperated as numerator for the number of moeities seperated.
  Finding moieties separated by slashes indicates that there are no denominators in the input. If there are denominators 
  in the input then the input needs to be split again to correctly label each strength.
  """
  if two_moieties_separated_by_slashes(data) or three_moieties_separated_by_slashes(data) or moiety_separated_by_dash(data):
    result = find_all_strength_unit_pairs(data)
  else:
    tokens = split_denominators(data, doseform)
    result = find_all_strength_unit_pairs(tokens)
      
  return result

# COMMAND ----------

def join_on_columns_and_filter_max_moieties(df_input: DataFrame,
                                            moiety_join_cols: List[str],
                                            df_refdata: DataFrame,
                                            id_col: str,
                                            text_col: str,
                                           ) -> DataFrame:
  ''' Join input data to reference data on the moiety fields.
  All moiety fields which are not null must be present in the input data.
  Keep only the results with the maximum number of moiety matches per input description.
  e.g. if there is at least one record which matches all 5, then we won't keep any with 4 or less.
  '''
   
  # Join condition to see if the text_col contains the moiety. 
  # We use array_except to make sure the moiety appears as a whole token in text_col.
  # At least 1 moiety is always present so nulls are fine to join on.
  moiety_conditional = reduce(
    and_,
    [
      (
        F.lower(col(text_col)).contains(F.lower(col(moiety_col))) &
        (F.size(F.array_except(F.split(F.lower(col(moiety_col)), '[^-A-Za-z0-9]'), F.split(F.lower(col(text_col)), '[^-A-Za-z0-9]'))) == 0)                                  
      ) | 
      col(moiety_col).isNull() \
      for moiety_col in moiety_join_cols 
    ]
  )
  df_moiety_matches = df_input.join(df_refdata, on=moiety_conditional)

  # Count moieties present
  df_moiety_matches = df_moiety_matches.withColumn(
    '_moiety_count',
    reduce(add, [col(moiety).isNotNull().cast('int') for moiety in ['MOIETY', 'MOIETY_2', 'MOIETY_3', 'MOIETY_4', 'MOIETY_5']])
  )

  # We only want the records with the maximum number of moieties per id
  perId = Window.partitionBy(id_col)
  df_moiety_matches_max = df_moiety_matches.withColumn('_max_count_per_id', F.max('_moiety_count').over(perId)) \
                                           .where(col('_max_count_per_id') == col('_moiety_count')).drop('_moiety_count', '_max_count_per_id')
  
  return df_moiety_matches_max

# COMMAND ----------

def entity_match(df_input: DataFrame, 
                 df_refdata: DataFrame,
                 ref_id_level: str,
                 dose_form_list_bc: Broadcast,
                 id_col: str,
                 original_text_col: str,
                 form_in_text_col: str,
                 text_col: str,
                 match_id_col: str,
                 id_level_col: str,
                 match_level_col: str,
                 match_datetime_col: str,
                 ref_id_col: str
                ) -> DataFrame:
  ''' Entity matching function.
  Join to the parsed reference data (amp or vmp) on the moiety columns.
  Filter for records where the strength and units in the input matches those in the reference data.
  
  Arguments:
    df_input (DataFrame): input data
    df_refdata (DataFrame): reference data
    ref_id_level (str): ref data level to match to (amp or vmp)
    dose_form_list_bc (Broadcast): a list of allowable dose forms
  
  Returns:
    df_entity_matches: dataframe of the input plus columns for the best reference data match: match_id, id_level, also: match_level, match_datetime
  '''
  
  moiety_cols = ['MOIETY', 'MOIETY_2', 'MOIETY_3', 'MOIETY_4', 'MOIETY_5', 'SITE', 'DOSEFORM']
  dosage_cols = ['SVN', 'SVU', 'SVN2', 'SVU2', 'SVD', 'SDU', 'SVN_2', 'SVU_2', 'SVD_2', 'SDU_2', 'SVN_3', 'SVU_3']
            
  # Join to refdata on moiety cols and filter to max moieties per id
  df_moiety_matches_max = join_on_columns_and_filter_max_moieties(df_input, moiety_join_cols=moiety_cols, df_refdata=df_refdata, id_col=id_col, text_col=text_col)

  @F.udf(returnType=BooleanType())
  def match_strength_unit(x, dosage_cols):
    strengths_and_units = extract_strength_unit(x, dose_form_list_bc.value)
    if any(strengths_and_units):
      return all(
        dosage_cols[field] == expected_value
        for field, expected_value in zip(strengths_and_units._fields, strengths_and_units)
        if expected_value is not None
      )
    else:
      return False

  # Filter to records which match the extracted strength and unit
  dosage_cols = F.struct([df_moiety_matches_max[col] for col in dosage_cols])
  df_moiety_matches_max = df_moiety_matches_max.where(match_strength_unit(col(text_col), dosage_cols))

  # Exact matches will only have one refdata match and match strength and units
  perId = Window.partitionBy(id_col)
  df_entity_matches = df_moiety_matches_max.withColumn('_count', F.count(text_col).over(perId)) \
                                           .where(col('_count') == lit(1)).drop('_count') \
                                           .withColumn(match_level_col, lit('entity')) \
                                           .withColumn(match_datetime_col, F.current_timestamp()) \
                                           .withColumn(id_level_col, lit(ref_id_level))
                                                   
  return df_entity_matches.select(id_col, original_text_col, form_in_text_col, text_col, col(ref_id_col).alias(match_id_col), 
                                  id_level_col, match_level_col, match_datetime_col)

# COMMAND ----------

@F.udf(ArrayType(StringType()))
def remove_substrings_udf(input_array):
  '''
  Remove elements of the input_array if they are a substring of any other element.
  '''
  pairs = list(product(input_array, input_array))
  substrings = [x[0] for x in pairs if x[0]!=x[1] and x[0] in x[1]]
  return list(set(input_array) - set(substrings))

# COMMAND ----------

def partial_entity_match(df_input: DataFrame, 
                         df_amp_parsed: DataFrame, 
                         df_vmp_parsed: DataFrame, 
                         id_col: str,
                         original_text_col: str,
                         form_in_text_col: str,
                         text_col: str,
                         match_id_col: str,
                         match_level_col: str,
                         match_term_col: str,
                         ref_id_col: str,
                         ref_text_col: str,
                        ) -> DataFrame:
  
  """ For fuzzy matching it will save computation time if we reduce the number of candidate matches in the reference data.
  We can do this by applying partial entity matching, which matches on the moieties only, and doesn't require strengths, units and dose forms to match.
  The candidate matches found by partial_entity_matching are used at fuzzy matching step 2: linked fuzzy matching.
  
  We want to avoid having ref data matches with different moieties. If an input contains multiple moieties then the candidate reference data
  matches should contain all of them. So in cases where a single moiety match is found, we must set as unmappble if there are more than one 
  different moiety is found. With the exception of Sodium Chloride or Glucose which are often mixers rather than moieties, and also where one moiety
  is a substring of another.
  
  The dataframe returned by this function will have more rows than df_input.
  This is because if partial matches are found for an input record, all of the candidate matches are added to the table.
  Input records with no partial matches will have only one row in the output dataframe, and for these rows: match_level, match_id, and match_term will all be set to null.
  """
  
  moiety_cols = ['MOIETY', 'MOIETY_2', 'MOIETY_3', 'MOIETY_4', 'MOIETY_5']
  
  # De-duplicate the amp parsed data, to save time at fuzzy matching.
  col_list_for_window = [ref_text_col, 'SITE', 'DOSEFORM'] + moiety_cols + list(FIELDS)
  window_lowest_apid = Window.partitionBy([col(x) for x in col_list_for_window]).orderBy(col('_apid_long').asc())

  df_amp_parsed_dedup = df_amp_parsed.withColumn('_apid_long', col(ref_id_col).cast('long')) \
                                   .withColumn('_rn', F.row_number().over(window_lowest_apid)) \
                                   .where(col('_rn') == 1) \
                                   .drop('_apid_long', '_rn')
  
  # Find partial matches at amp level
  df_amp_non_match = join_on_columns_and_filter_max_moieties(df_input, moiety_cols, df_amp_parsed_dedup, id_col=id_col, text_col=text_col) \
                                    .withColumn(match_level_col, lit('APID')) \
                                    .select(id_col, original_text_col, form_in_text_col, text_col, *moiety_cols, match_level_col, ref_id_col, ref_text_col)
  
  # Find partial matches at vmp level
  df_vmp_non_match = join_on_columns_and_filter_max_moieties(df_input, moiety_cols, df_vmp_parsed, id_col=id_col, text_col=text_col) \
                                    .withColumn(match_level_col, lit('VPID')) \
                                    .select(id_col, original_text_col, form_in_text_col, text_col, *moiety_cols, match_level_col, ref_id_col, ref_text_col)

  df_partial_matches = df_amp_non_match.union(df_vmp_non_match)
  
  window_id = Window.partitionBy(col(id_col))
 
  # in some cases, more moieties are matched at VMP than AMP. We need to keep only the max moieties across both levels.
  df_partial_matches = df_partial_matches.withColumn('_moiety_count', reduce(add, [col(moiety).isNotNull().cast('int') for moiety in moiety_cols])) \
                                         .withColumn('_max_count_per_id', F.max('_moiety_count').over(window_id)) \
                                         .where(col('_max_count_per_id') == col('_moiety_count')) \
                                         .drop('_moiety_count', '_max_count_per_id')
  
  # keep if more than one moiety
  df_more_than_one_moiety = df_partial_matches.where(col('MOIETY_2').isNotNull())

  df_single_moiety = df_partial_matches.where(col('MOIETY_2').isNull()) \
          .withColumn('_diff_moiety_count', F.size(F.collect_set(F.lower(col('MOIETY'))).over(window_id)))

  # keep if first moiety is unique
  df_moiety_is_unique = df_single_moiety \
          .where(col('_diff_moiety_count') == 1) \
          .drop('_diff_moiety_count')

  # keep if first moiety is not unique, but is unique after excluding Sodium Chloride or Glucose or Phosphate
  df_moiety_is_unique_after_solutions_removed = df_single_moiety \
          .where(col('_diff_moiety_count') > 1) \
          .where(~F.lower(col('MOIETY')).isin(['sodium chloride', 'glucose', 'phosphate'])) \
          .withColumn('_diff_moiety_except_solutions_count', F.size(F.collect_set(F.lower(col('MOIETY'))).over(window_id))) \
          .where(col('_diff_moiety_except_solutions_count') == 1) \
          .drop('_diff_moiety_count', '_diff_moiety_except_solutions_count')

  # keep if first moiety is not unique, but is unique after removing moieties which are substrings of other present moieties.
  # also remove any moieties that only appear within brackets.
  df_moiety_is_unique_after_substring_moieties_removed = df_single_moiety \
          .where(col('_diff_moiety_count') > 1) \
          .where(~F.lower(col('MOIETY')).isin(['sodium chloride', 'glucose', 'phosphate'])) \
          .withColumn('_diff_moiety_except_solutions_count', F.size(F.collect_set(F.lower(col('MOIETY'))).over(window_id))) \
          .where(col('_diff_moiety_except_solutions_count') > 1) \
          .withColumn('_text_without_brackets', F.regexp_replace(text_col, r'\(.*\)', '')) \
          .where(col('_text_without_brackets').contains(F.lower(col('MOIETY')))) \
          .withColumn('_diff_moieties', F.collect_set(F.lower(col('MOIETY'))).over(window_id)) \
          .withColumn('_diff_moieties_except_substrings', remove_substrings_udf(col('_diff_moieties'))) \
          .where(F.size(F.array_intersect(F.array(F.lower(col('MOIETY'))), col('_diff_moieties_except_substrings'))) > 0) \
          .where(F.size('_diff_moieties_except_substrings') == 1) \
          .drop('_diff_moiety_count', '_diff_moiety_except_solutions_count', '_text_without_brackets', '_diff_moieties', '_diff_moieties_except_substrings')
  
  # collate the candidates we are keeping, and set all other partial matches to unmappable
  df_partial_entity_matches_filtered = df_more_than_one_moiety.union(df_moiety_is_unique) \
                                                              .union(df_moiety_is_unique_after_solutions_removed) \
                                                              .union(df_moiety_is_unique_after_substring_moieties_removed) \
                                                              .select(id_col, original_text_col, form_in_text_col, text_col,
                                                                      match_level_col, col(ref_id_col).alias(match_id_col),
                                                                      col(ref_text_col).alias(match_term_col))

  df_partial_entity_unmappable = df_partial_matches.join(df_partial_entity_matches_filtered, id_col, 'left_anti') \
                                                   .dropDuplicates([id_col]) \
                                                   .select(original_text_col, form_in_text_col)
                                      
  # The remainder do not have partial matches, but shoud still be passed on to fuzzy matching
  df_not_partially_matched = df_input.join(df_amp_non_match, id_col, 'leftanti') \
                                     .join(df_vmp_non_match, id_col, 'leftanti') \
                                     .withColumn(match_level_col, lit(None)) \
                                     .withColumn(match_id_col, lit(None)) \
                                     .withColumn(match_term_col, lit(None)) \
                                     .select(id_col, original_text_col, form_in_text_col, text_col, match_level_col, match_id_col,
                                            match_term_col)

  return df_partial_entity_matches_filtered.union(df_not_partially_matched), df_partial_entity_unmappable