# Databricks notebook source
# MAGIC %run ../notebooks/_modules/epma_global/functions

# COMMAND ----------

# MAGIC %run ../notebooks/1_exact_match/functions/exact_match_functions

# COMMAND ----------

# MAGIC %run ../notebooks/_modules/epma_global/function_test_suite

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

suite = FunctionTestSuite()

# COMMAND ----------

@suite.add_test
def test_select_distinct_tokens():
  
  df_example = spark.createDataFrame([
    (1,'fgd 100%|5.24units           (32mlr)'),
    (2,'werf-9.4ml+[wer43ig/8.24mg]|ml -   mg'),
  #   (3, None),
  #   (4, ''),
    (5, 'serf 5.24% ?!')
    ],
    StructType([StructField("id", IntegerType()), \
                StructField("name", StringType())
               ])  
  )

  df_process = select_distinct_tokens(df_example,'name','token')
  
  df_expected = spark.createDataFrame([
    (1, 'fgd 100%|5.24units           (32mlr)', ["%", "100", "32", "5.24", "fgd", "mlr", "units"]),
    (2, 'werf-9.4ml+[wer43ig/8.24mg]|ml -   mg', ["43", "8.24", "9.4", "ig", "mg", "ml", "wer", "werf"]),
    (5, 'serf 5.24% ?!', ["%", "5.24", "?!", "serf"])
    ],
    StructType([StructField("id", IntegerType()), \
                StructField("name", StringType()), \
                StructField("token", ArrayType(StringType()))
               ])  
    )

  assert compare_results(df_expected, df_process, join_columns=['id'])

# COMMAND ----------

# exact match tests

@suite.add_test
def test_exact_match():
  
  input_schema = StructType([
    StructField("epma_id", LongType()), 
    StructField("original_epma_description", StringType()), 
    StructField("epma_description", StringType()), 
    StructField("form_in_text", StringType()), 
    StructField("epma_description_without_form", StringType()), 
    StructField("token", ArrayType(StringType())),
  ]) 
  
  df_epma_process = spark.createDataFrame([
    (0, 'Sodium chloride 0.9% Infusion 100 mL', 'Sodium chloride 0.9% Infusion 100 mL', ' ', 'Sodium chloride 0.9% Infusion 100 mL', ['%', '0.9', '100', 'chloride', 'infusion', 'ml', 'sodium']),
    (1, 'flucloxacillin', 'flucloxacillin', ' ', 'flucloxacillin', ['flucloxacillin']), 
    (2, 'betamethasone-calcipotriol 0.05%-0.005% topical gel', 'betamethasone-calcipotriol 0.05%-0.005% topical gel', ' ', 'betamethasone-calcipotriol 0.05%-0.005% topical gel', ['%', '0.05', '0.005', 'betamethasone', 'calcipotriol', 'gel', 'topical']),
    (3, 'Bimatoprost 100 micrograms/mL eye drops', 'Bimatoprost 100 micrograms/mL eye drops', ' ', 'Bimatoprost 100 micrograms/mL eye drops', ['100', 'bimatoprost', 'drops', 'eye', 'micrograms', 'ml']),
    (4, 'diclofenac 2.32% gel', 'diclofenac 2.32% gel', ' ', 'diclofenac 2.32% gel', ['%', '2.32', 'diclofenac', 'gel']),
    (5, 'mercaptopurine', 'mercaptopurine', ' ', 'mercaptopurine', ['mercaptopurine']),
    (6, 'propranolol', 'propranolol', ' ', 'propranolol', ['propranolol'])
  ],
    input_schema    
  )
  
  ref_schema = StructType([
    StructField('ref_id', LongType()),
    StructField('text_col', StringType()),
    StructField('token', ArrayType(StringType())),
    StructField('id_level', StringType()),
  ]) 

  df_vtm_process= spark.createDataFrame([
    (1001, 'flucloxacillin', ['flucloxacillin'], 'VTMID'),
    (1002, 'mercaptopurine', ['mercaptopurine'], 'VTMID'),
    (1003, 'amoxicillin', ['amoxicillin'], 'VTMID'),
  ],
    ref_schema
  )

  df_amp_process = spark.createDataFrame([
    (2001, 'Sodium chloride 0.9% Infusion 100 mL', ['%', '0.9', '100', 'chloride', 'infusion', 'ml', 'sodium'], 'APID')
  ],
    ref_schema
  )

  df_vmp_process = spark.createDataFrame([
    (2002, 'betamethasone-calcipotriol 0.05%-0.005% topical gel', ['%', '0.05', '0.005', 'betamethasone', 'calcipotriol', 'gel', 'topical'], 'VPID')
  ],
    ref_schema
  )
  
  df_amp_process_prevName = spark.createDataFrame([
    (3001, 'Bimatoprost 100 micrograms/mL eye drops', ['100', 'bimatoprost', 'drops', 'eye', 'micrograms', 'ml'], 'APID')
  ],
    ref_schema
  )
  
  df_vmp_process_prevName = spark.createDataFrame([
    (3002, 'diclofenac 2.32% gel', ['%', '2.32', 'diclofenac', 'gel'], 'VPID') 
  ],
    ref_schema
  )
  
  matched_df, non_matched_df = get_exact_matches(df_epma_process, df_vtm_process, df_vmp_process, df_amp_process,
                      df_vmp_process_prevName, df_amp_process_prevName, id_col='epma_id', original_text_col='original_epma_description', 
                      text_col='epma_description', form_in_text_col='form_in_text', text_without_form_col='epma_description_without_form',
                      match_id_col='match_id', id_level_col='id_level', match_level_col='match_level', match_datetime_col='match_datetime', token_col='token',  ref_id_col='ref_id')
  
  matched_df_expected = spark.createDataFrame([
    (0, 'Sodium chloride 0.9% Infusion 100 mL', 'Sodium chloride 0.9% Infusion 100 mL', ' ', 2001, 'APID', 'exact_by_name'),
    (1, 'flucloxacillin', 'flucloxacillin', ' ', 1001, 'VTMID', 'exact_by_name'),
    (2, 'betamethasone-calcipotriol 0.05%-0.005% topical gel', 'betamethasone-calcipotriol 0.05%-0.005% topical gel', ' ', 2002, 'VPID', 'exact_by_name'),
    (3, 'Bimatoprost 100 micrograms/mL eye drops', 'Bimatoprost 100 micrograms/mL eye drops', ' ', 3001, 'APID', 'exact_by_prev_name'),
    (4, 'diclofenac 2.32% gel', 'diclofenac 2.32% gel', ' ', 3002, 'VPID', 'exact_by_prev_name'),
    (5, 'mercaptopurine', 'mercaptopurine', ' ', 1002, 'VTMID', 'exact_by_name')
  ],
    ['epma_id', 'original_epma_description', 'epma_description', 'form_in_text', 'match_id', 'id_level', 'match_level'])

  non_matched_df_expected = spark.createDataFrame([
    (6, 'propranolol', 'propranolol', ' ', 'propranolol')
  ],
    ['epma_id', 'original_epma_description', 'epma_description', 'form_in_text', 'epma_description_without_form'])
  
  assert compare_results(matched_df_expected, matched_df.drop('match_datetime'), join_columns=['epma_id'])
  assert compare_results(non_matched_df_expected, non_matched_df, join_columns=['epma_id'])

# COMMAND ----------

suite.run()