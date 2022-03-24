# Databricks notebook source
# MAGIC %run ../notebooks/_modules/epma_global/function_test_suite

# COMMAND ----------

# MAGIC %run ../notebooks/_modules/epma_global/test_helpers

# COMMAND ----------

# MAGIC %run ../notebooks/3_fuzzy_matching/functions/fuzzy_match_functions

# COMMAND ----------

from unittest.mock import patch
from datetime import datetime

from pyspark.sql.types import StructField, LongType, StringType, IntegerType, FloatType, ArrayType
import pyspark.sql.functions as F

# COMMAND ----------

suite = FunctionTestSuite()

# COMMAND ----------

@suite.add_test
def test_max_confidence_score():  

  df_input = spark.createDataFrame(
    [ ('w_7526','magnesium sulfate bp paste','magnesium sulfate bp paste', 'VPID', '4503211000001105', 'Magnesium sulfate 50% (magnesium 2mmol/ml) solution for injection 5ml ampoules') ,
      ('w_7526','magnesium sulfate bp paste','magnesium sulfate bp paste','VPID', '26257411000001108', 'Magnesium sulfate 1.23% cutaneous solution'),
      ('w_7526','magnesium sulfate bp paste','magnesium sulfate bp paste','APID', '26048311000001105', 'Magnesium sulfate 5% (magnesium 0.2mmol/ml) infusion 100ml bags'),
      ('w_7526','magnesium sulfate bp paste','magnesium sulfate bp paste','APID', '156811000001106', 'magnesium sulfate paste')
    ],
    ['epma_id', 'original_epma_description', 'epma_description', 'match_level', 'match_id', 'match_term']
  )
  df_expected_output = spark.createDataFrame(
    [ ('w_7526','magnesium sulfate bp paste', 'magnesium sulfate bp paste', 'APID', '156811000001106', 'magnesium sulfate paste',95)
    ],
    ['epma_id', 'original_epma_description', 'epma_description', 'match_level', 'match_id', 'match_term', 'confidence_score']
  )

  output = calculate_max_confidence_score(df_input, confidence_score_col='confidence_score', text_col='epma_description', match_term_col='match_term', id_col='epma_id')   
  assert compare_results(output, df_expected_output, join_columns=['epma_id'])

# COMMAND ----------

@suite.add_test
def test_apid_match_unique_apid():

  df_input = spark.createDataFrame(
    [('7','Co-Co-beneldopa 12.5mg/50mg capsules 12.5mg/50mg capsule', ' ', 'Co-Co-beneldopa 12.5mg/50mg capsules 12.5mg/50mg capsule',
       'APID','29294111000001107','Co-beneldopa 12.5mg/50mg capsules', 90), # Tie between Apid and Vpid
    ],
    ['epma_id', 'original_epma_description', 'form_in_text', 'epma_description', 'match_level', 'match_id', 'match_term', 'confidence_score']
  )
  
  df_ref_data_amp = spark.createDataFrame([
    ('29294111000001107', 'Colecalciferol 3,000units/ml oral solution', 'APID'),
  ], ['_id', 'text_col', 'id_level'])
  
  df_ref_data_vmp = spark.createDataFrame([
    ('11788611000001108', 'Colecalciferol 15,000units/5ml oral solution', 'VPID'),
  ], ['_id', 'text_col', 'id_level'])
    
  df_ref_data_vtm = spark.createDataFrame([
    ('108943009', 'Colecalciferol', 'VTMID'),
  ], ['_id', 'text_col', 'id_level'])
      
  class MockRefDataStore():
    amp = df_ref_data_amp
    vmp = df_ref_data_vmp 
    vtm = df_ref_data_vtm
    ID_COL = '_id'
    TEXT_COL = 'text_col' 
    
  df_expected = spark.createDataFrame(
    [ ('7', 'Co-Co-beneldopa 12.5mg/50mg capsules 12.5mg/50mg capsule', ' ', 'Co-Co-beneldopa 12.5mg/50mg capsules 12.5mg/50mg capsule',
       '29294111000001107', 'APID', 'Co-beneldopa 12.5mg/50mg capsules', 90, '108943009', 'unique max confidence APID')
    ],
    [ 'epma_id', 'original_epma_description', 'form_in_text', 'epma_description', 'ref_data_id', 'id_level', 'match_term', 'confidence_score', 'VTMID', 'reason']
  )
  
  with FunctionPatch('ReferenceDataFormatter', MockRefDataStore):
    df_output = APID_match_candidates(df_input,
                                      RefDataStore,
                                      confidence_score_col='confidence_score',
                                      original_text_col='original_epma_description',
                                      form_in_text_col='form_in_text',
                                      text_col='epma_description',
                                      match_term_col='match_term',
                                      match_level_col='match_level',
                                      reason_col='reason',
                                      id_col='epma_id',
                                      id_level_col='id_level',
                                      match_id_col='match_id',
                                      ref_data_id_col='ref_data_id')

  assert compare_results(df_output, df_expected, join_columns=['epma_id'])

# COMMAND ----------

@suite.add_test
def test_apid_match_tied_apid():

  df_input = spark.createDataFrame(
    [ ('2','colecalciferol 3000 units/ml oral solution', ' ', 'colecalciferol 3000 units/ml oral solution','APID','29294111000001107','Colecalciferol 3,000units/ml oral solution', 98), # Multi APid and 1 VPid
      ('2','colecalciferol 3000 units/ml oral solution', ' ', 'colecalciferol 3000 units/ml oral solution','APID','29317511000001101','Colecalciferol 3,000units/ml oral solution', 98),
      ('2','colecalciferol 3000 units/ml oral solution', ' ', 'colecalciferol 3000 units/ml oral solution','APID','37232911000001103','Colecalciferol 3,000units/ml oral solution', 98),  
    ],
    ['epma_id', 'original_epma_description', 'form_in_text', 'epma_description', 'match_level', 'match_id', 'match_term', 'confidence_score']
  )
  
  df_ref_data_amp = spark.createDataFrame([
    ('29294111000001107', 'Colecalciferol 3,000units/ml oral solution', 'APID'),
    ('29317511000001101', 'Colecalciferol 3,000units/ml oral solution', 'APID'),
    ('37232911000001103', 'Colecalciferol 3,000units/ml oral solution', 'APID'),
  ], ['_id', 'text_col', 'id_level'])
  
  df_ref_data_vmp = spark.createDataFrame([
    ('11788611000001108', 'Colecalciferol 15,000units/5ml oral solution', 'VPID'),
  ], ['_id', 'text_col', 'id_level'])
    
  df_ref_data_vtm = spark.createDataFrame([
    ('108943009', 'Colecalciferol', 'VTMID'),
  ], ['_id', 'text_col', 'id_level'])
      
  class MockRefDataStore():
    amp = df_ref_data_amp
    vmp = df_ref_data_vmp 
    vtm = df_ref_data_vtm
    ID_COL = '_id'
    TEXT_COL = 'text_col' 
    
  df_expected = spark.createDataFrame(
    [ ('2', 'colecalciferol 3000 units/ml oral solution', ' ', 'colecalciferol 3000 units/ml oral solution', '11788611000001108',
       'VPID', 'Colecalciferol 3,000units/ml oral solution', 98, '108943009', 'tied max confidence APID, unique max confidence VPID')
    ],
    [ 'epma_id', 'original_epma_description', 'form_in_text', 'epma_description', 'ref_data_id', 'id_level', 'match_term', 'confidence_score', 'VTMID', 'reason']
  )
    
  with FunctionPatch('ReferenceDataFormatter', MockRefDataStore):
    df_output = APID_match_candidates(df_input,
                                      RefDataStore,
                                      confidence_score_col='confidence_score',
                                      original_text_col='original_epma_description',
                                      form_in_text_col='form_in_text',
                                      text_col='epma_description',
                                      match_term_col='match_term',
                                      match_level_col='match_level',
                                      reason_col='reason',
                                      id_col='epma_id',
                                      id_level_col='id_level',
                                      match_id_col='match_id',
                                      ref_data_id_col='ref_data_id')

  assert compare_results(df_output, df_expected, join_columns=['epma_id'])

# COMMAND ----------

@suite.add_test
def test_vpid_match_unique_vpid():  

  df_input = spark.createDataFrame(
    [ ('1','sodium valproate 200 mg in 5ml oral sugar-free liquid', ' ', 'sodium valproate 200 mg in 5ml oral sugar-free liquid','VPID','39107811000001107','Sodium valproate 100mg tablets', 86),
      ('9','tacrolimus 0.03 % ointment (30g)', ' ', 'tacrolimus 0.03 % ointment (30g)','VPID','407889001','tacrolimus 0.03 % ointment ', 95),
    ],
    ['epma_id', 'original_epma_description', 'form_in_text', 'epma_description', 'match_level', 'match_id', 'match_term', 'confidence_score']
  )
 
  df_ref_data_vmp = spark.createDataFrame([
    ('39107811000001107', 'Sodium valproate 100mg tablets', 'VPID'),
    ('407889001', 'Tacrolimus 0.03% ointment', 'VPID'),
  ], ['_id', 'text_col', 'id_level'])
    
  df_ref_data_vtm = spark.createDataFrame([
    ('10049011000001109', 'Sodium valproate', 'VTMID'),
    ('109129008', 'Tacrolimus', 'VTMID'),
  ], ['_id', 'text_col', 'id_level'])
      
  class MockRefDataStore():
    vmp = df_ref_data_vmp 
    vtm = df_ref_data_vtm
    ID_COL = '_id'
    TEXT_COL = 'text_col' 
    
  df_expected = spark.createDataFrame(
    [ ('9', 'tacrolimus 0.03 % ointment (30g)', ' ', 'tacrolimus 0.03 % ointment (30g)', '407889001', 'VPID', 'tacrolimus 0.03 % ointment ', 95, '109129008', 'unique max confidence VPID'),
      ('1', 'sodium valproate 200 mg in 5ml oral sugar-free liquid', ' ', 'sodium valproate 200 mg in 5ml oral sugar-free liquid', '39107811000001107',
       'VPID', 'Sodium valproate 100mg tablets', 86, '10049011000001109', 'unique max confidence VPID')
    ],
    ['epma_id', 'original_epma_description', 'form_in_text', 'epma_description', 'ref_data_id', 'id_level', 'match_term', 'confidence_score', 'VTMID', 'reason']
  )
  
  with FunctionPatch('ReferenceDataFormatter', MockRefDataStore):
    df_output = VPID_match_candidates(df_input,
                                      RefDataStore,
                                      confidence_score_col='confidence_score',
                                      original_text_col='original_epma_description',
                                      form_in_text_col='form_in_text',
                                      text_col='epma_description',
                                      match_term_col='match_term',
                                      match_level_col='match_level',
                                      reason_col='reason',
                                      id_col='epma_id',
                                      id_level_col='id_level',
                                      match_id_col='match_id',
                                      ref_data_id_col='ref_data_id')
  
  assert compare_results(df_output, df_expected, join_columns = ['epma_id'])

# COMMAND ----------

@suite.add_test
def test_vpid_match_tied_vpid():  

  df_input = spark.createDataFrame(
    [ ('11','betamethasone 0.1% drops', ' ', 'betamethasone 0.1% drops','VPID','35910611000001103','Betamethasone 4mg/1ml solution for injection ampoules', 86),  
      ('11','betamethasone 0.1% drops', ' ', 'betamethasone 0.1% drops','VPID','354027002','Betamethasone 0.1% ear/eye/nose drops', 86),  
    ],
    ['epma_id', 'original_epma_description', 'form_in_text', 'epma_description', 'match_level', 'match_id', 'match_term', 'confidence_score']
  )
  
  df_ref_data_vmp = spark.createDataFrame([
    ('35910611000001103', 'Betamethasone 4mg/1ml solution for injection ampoules', 'VPID'),
    ('354027002', 'Betamethasone 0.1% ear/eye/nose drops', 'VPID'),
  ], ['_id', 'text_col', 'id_level'])
    
  df_ref_data_vtm = spark.createDataFrame([
    ('29896003', 'Betamethasone', 'VTMID'),
  ], ['_id', 'text_col', 'id_level'])
      
  class MockRefDataStore():
    vmp = df_ref_data_vmp 
    vtm = df_ref_data_vtm
    ID_COL = '_id'
    TEXT_COL = 'text_col' 
    
  df_expected = spark.createDataFrame(
    [ ('11', 'betamethasone 0.1% drops', ' ', 'betamethasone 0.1% drops', 'VTMID', 'tied max confidence VPID, unique max confidence VTMID')
    ],
    [ 'epma_id', 'original_epma_description', 'form_in_text', 'epma_description', 'id_level', 'reason']
  )
  
  with FunctionPatch('ReferenceDataFormatter', MockRefDataStore):
    df_output = VPID_match_candidates(df_input,
                                      RefDataStore,
                                      confidence_score_col='confidence_score',
                                      original_text_col='original_epma_description',
                                      form_in_text_col='form_in_text',
                                      text_col='epma_description',
                                      match_term_col='match_term',
                                      match_level_col='match_level',
                                      reason_col='reason',
                                      id_col='epma_id',
                                      id_level_col='id_level',
                                      match_id_col='match_id',
                                      ref_data_id_col='ref_data_id')

  df_output = df_output.select('epma_id', 'original_epma_description', 'form_in_text', 'epma_description', 'id_level', 'reason')

  assert compare_results(df_output, df_expected, join_columns=['epma_id'])

# COMMAND ----------

@suite.add_test
def test_select_best_match_from_scored_records_strength_match():         
  
  df_input = spark.createDataFrame(
    [ ('9','tacrolimus 0.03 % ointment (30g)', ' ', 'tacrolimus 0.03 % ointment (30g)','VPID', '407889001', 'tacrolimus 0.03 % ointment ', 95),
    ],
    ['epma_id', 'original_epma_description', 'form_in_text', 'epma_description', 'match_level', 'match_id', 'match_term', 'confidence_score']
  )
  
  df_expected = spark.createDataFrame(
    [('9', 'tacrolimus 0.03 % ointment (30g)', ' ', 'tacrolimus 0.03 % ointment (30g)',
      '109129008', 'VTMID', 'tacrolimus 0.03 % ointment ', 95, 'Strength mismatch so map to VTMID', 'tacrolimus'),
    ],
    ['epma_id', 'original_epma_description', 'form_in_text', 'epma_description', 'match_id', 'id_level', 'match_term', 'confidence_score',  'match_reason_debug', 
      'match_term_debug']
  )
  
  df_ref_data_vmp = spark.createDataFrame([
    ('407889001', 'Tacrolimus 0.03% ointment', 'VPID'),
  ], ['_id', 'text_col', 'id_level'])
    
  df_ref_data_vtm = spark.createDataFrame([
    ('109129008', 'Tacrolimus', 'VTMID'),
  ], ['_id', 'text_col', 'id_level'])
      
  class MockRefDataStore():
    vmp = df_ref_data_vmp 
    vtm = df_ref_data_vtm
    ID_COL = '_id'
    TEXT_COL = 'text_col'
    
  with FunctionPatch('ReferenceDataFormatter', MockRefDataStore):
    df_matched, _ = select_best_match_from_scored_records(df_input,
                                                          RefDataStore,
                                                          confidence_score_col='confidence_score',
                                                          original_text_col='original_epma_description',
                                                          form_in_text_col='form_in_text',
                                                          text_col='epma_description',
                                                          match_term_col='match_term',
                                                          reason_col='reason',
                                                          id_col='epma_id',
                                                          id_level_col='id_level',
                                                          match_id_col='match_id',
                                                          match_datetime_col='match_datetime',
                                                          match_level_col='match_level',
                                                          has_strength_col='has_strength')

  df_actual = df_matched.df.select('epma_id', 'original_epma_description', 'form_in_text', 'epma_description', 'match_id',
                                                      'id_level','match_term', 'confidence_score',  'match_reason_debug', 'match_term_debug')

  assert compare_results(df_actual, df_expected, join_columns=['epma_id'])

# COMMAND ----------

@suite.add_test
def test_select_best_match_from_scored_records_null_vtmid():         
  
  df_input = spark.createDataFrame(
    [('3', 'balance glucose calcium solution for peritoneal dialysis stay safe bags', ' ', 'balance glucose calcium solution for peritoneal dialysis stay safe bags',
      'APID', '10656211000001102', 'balance glucose 2.3% calcium 1.75mmol/1litre solution for peritoneal dialysis 2litre stay safe bags', 95) # APID with null VTMID
    ],
    ['epma_id', 'original_epma_description', 'form_in_text', 'epma_description', 'match_level', 'match_id', 'match_term', 'confidence_score']
  )
  
  df_expected = spark.createDataFrame([
    ('3','balance glucose calcium solution for peritoneal dialysis stay safe bags', ' ', 'balance glucose calcium solution for peritoneal dialysis stay safe bags',
     '10656211000001102', 'APID', 'balance glucose 2.3% calcium 1.75mmol/1litre solution for peritoneal dialysis 2litre stay safe bags', 95, 'unique max confidence APID',
     'balance glucose 2.3% calcium 1.75mmol/1litre solution for peritoneal dialysis 2litre stay safe bags')
    ],
    [ 'epma_id', 'original_epma_description', 'form_in_text', 'epma_description', 'match_id', 'id_level', 'match_term', 'confidence_score', 'match_reason_debug', 
      'match_term_debug'
    ]
  )
  
  df_ref_data_amp = spark.createDataFrame([
    ('10656211000001102', 'balance glucose 2.3% calcium 1.75mmol/1litre solution for peritoneal dialysis 2litre stay safe bags', 'APID'),
  ], ['_id', 'text_col', 'id_level'])
  
  df_ref_data_vmp = spark.createDataFrame([
    ('10656011000001107', 'Generic balance glucose 2.3% calcium 1.75mmol/1litre solution for peritoneal dialysis 2litre bags', 'VPID'),
  ], ['_id', 'text_col', 'id_level'])
      
  class MockRefDataStore():
    amp = df_ref_data_amp
    vmp = df_ref_data_vmp 
    ID_COL = '_id'
    TEXT_COL = 'text_col'
    
  with FunctionPatch('ReferenceDataFormatter', MockRefDataStore):
    df_matched, _ = select_best_match_from_scored_records(df_input,
                                                          RefDataStore,
                                                          confidence_score_col='confidence_score',
                                                          original_text_col='original_epma_description',
                                                          form_in_text_col='form_in_text',
                                                          text_col='epma_description',
                                                          match_term_col='match_term',
                                                          reason_col='reason',
                                                          id_col='epma_id',
                                                          id_level_col='id_level',
                                                          match_id_col='match_id',
                                                          match_datetime_col='match_datetime',
                                                          match_level_col='match_level',
                                                          has_strength_col='has_strength')

  df_actual = df_matched.df.select('epma_id', 'original_epma_description', 'form_in_text', 'epma_description', 'match_id', 'id_level',
                                           'match_term', 'confidence_score', 'match_reason_debug', 'match_term_debug')
  
  assert compare_results(df_actual, df_expected, join_columns=['epma_id'])

# COMMAND ----------

@suite.add_test
def test_get_moieties_only_from_amp_vmp_ref_data():
  df_amp_parsed = spark.createDataFrame([
    ('apid1', 'Coal tar 1% / Salicylic acid 3% / Sulfur 3% ointment', 'Coal tar', 'Salicylic acid', 'Sulfur 3%', None, None),
    ('apid2', 'Efavirenz 600mg / Tenofovir disoproxil 245mg tablets', None, None, None, 'Efavirenz 600mg', 'Tenofovir disoproxil')
  ], 
    ['_id', 'text_col', 'MOIETY', 'MOIETY_2', 'MOIETY_3', 'MOIETY_4', 'MOIETY_5'])

  df_vmp_parsed = spark.createDataFrame([
    ('vpid1', 'Intraven potassium chloride 0.15%', 'Intraven', None, 'potassium', None, 'chloride 0.15%'),
    ('vpid2', 'Glucose 2.5% / Sodium chloride 0.45% infusion', None, 'Glucose ', None, 'Sodium chloride 0.45%', None)
  ], 
    ['_id', 'text_col', 'MOIETY', 'MOIETY_2', 'MOIETY_3', 'MOIETY_4', 'MOIETY_5'])

  df_amp_dedup = spark.createDataFrame([
    ('apid3', 'Palifermin 6.25mg powder for solution for injection vials', 'vpid001', 'APID'),
  ], 
    ['APID', 'text_col', '_id', 'id_level'])

  df_vmp = spark.createDataFrame([
    ('vpid3', 'Desmopressin 120microgram oral lyophilisates sugar free', 'VPID'),
  ], 
    ['_id', 'text_col', 'id_level'])

  class MockRefDataStore():
    amp_dedup = df_amp_dedup
    vmp = df_vmp 
    amp_parsed = df_amp_parsed
    vmp_parsed = df_vmp_parsed
    ID_COL = '_id'
    TEXT_COL = 'text_col'
    ID_LEVEL_COL = 'id_level'

  list_of_non_moiety_words = ['powder', 'for', 'solution', 'injection', 'vial', 'sugar', 'free', 'oral']

  df_expected = spark.createDataFrame([
    ('apid1', 'Coal tar 1% / Salicylic acid 3% / Sulfur 3% ointment', 'APID', 'Coal tar Salicylic acid Sulfur'),
    ('apid2', 'Efavirenz 600mg / Tenofovir disoproxil 245mg tablets', 'APID', 'Efavirenz Tenofovir disoproxil'),
    ('vpid1', 'Intraven potassium chloride 0.15%', 'VPID', 'Intraven potassium chloride'),
    ('vpid2', 'Glucose 2.5% / Sodium chloride 0.45% infusion', 'VPID', 'Glucose  Sodium chloride'),
    ('apid3', 'Palifermin 6.25mg powder for solution for injection vials', 'APID', 'palifermin vials'),
    ('vpid3', 'Desmopressin 120microgram oral lyophilisates sugar free', 'VPID', 'desmopressin lyophilisates')
  ], 
    ['_id', 'text_col', 'id_level', 'moiety'])

  with FunctionPatch('ReferenceDataFormatter', MockRefDataStore):
    df_moiety_all_no_space = get_moieties_only_from_amp_vmp_ref_data(MockRefDataStore(),
                                                                     list_of_non_moiety_words,
                                                                     id_level_col='id_level',
                                                                     moiety_col='moiety')

  assert compare_results(df_moiety_all_no_space, df_expected, join_columns=['_id'])

# COMMAND ----------

@suite.add_test
def test_get_records_with_moieties():
  df_example = spark.createDataFrame([(1, 'LUBRICATING JELLY Gel'),
                                      (2, 'MOUTHWASH'),
                                      (3, 'Sodium bicarbonate 1.26% infusion'),
                                      (4, 'ABASAGLAR INSULIN GLARGINE (KWIKPEN) 100 units/ml Device'),
                                      (5, 'acetazolamide 125 mg in 5 mL Oral Solution')],
                                     ['epma_id', 'epma_description'])

  list_to_remove = ['lubricating','jelly','gel','mouthwash','infusion','device','solution','pen','oral']

  df_without_moieties_expected = spark.createDataFrame([(1, 'LUBRICATING JELLY Gel'),
                                              (2, 'MOUTHWASH')],
                                             ['epma_id', 'epma_description'])

  df_with_moieties_expected = spark.createDataFrame([(3, 'Sodium bicarbonate 1.26% infusion', 'sodium bicarbonate'),
                                                  (4, 'ABASAGLAR INSULIN GLARGINE (KWIKPEN) 100 units/ml Device', 'abasaglar insulin glargine '),
                                                  (5, 'acetazolamide 125 mg in 5 mL Oral Solution', 'acetazolamide in')],
                                                 ['epma_id', 'epma_description', 'cleaned_text'])

  df_records_with_moieties, df_records_without_moieties, df_cp = get_records_with_moieties(df_example, list_to_remove, text_col='epma_description', clean_text_col='cleaned_text')

  assert compare_results(df_without_moieties_expected, df_records_without_moieties, join_columns=['epma_id'])
  assert compare_results(df_with_moieties_expected, df_records_with_moieties, join_columns=['epma_id'])
  del df_cp

# COMMAND ----------

@suite.add_test
def test_sort_ratio_fuzzy_match():
  df_example = spark.createDataFrame([(1, 'hydrocortisone'),
                                      (2, 'Ensure twocal'),
                                      (3, 'Balneum Plus'),
                                      (4, 'rivastigmine'),
                                      (5, 'aciclovir')],
                                   ['epma_id', 'cleaned_text'])

  df_moieties = spark.createDataFrame([('Hydrocortisone',),
                                         ('Twocal HN',),
                                         ('Balneum Plus',),
                                         ('Rivastigmine',),
                                         ('Aciclovir',)],
                                        ['moiety'])

  schema_expected = StructType([
    StructField('epma_id', LongType()),
    StructField('cleaned_text', StringType()),
    StructField('moiety', StringType()),
    StructField('score', LongType())
  ])

  df_expected = spark.createDataFrame([(1, 'hydrocortisone', 'Hydrocortisone', 100),
                                      (2, 'Ensure twocal', 'Twocal HN', 73),
                                      (3, 'Balneum Plus', 'Balneum Plus', 100),
                                      (4, 'rivastigmine', 'Rivastigmine', 100),
                                      (5, 'aciclovir', 'Aciclovir', 100)],
                                      schema_expected)

  df_results = sort_ratio_fuzzy_match(df_example, df_moieties, text_col='cleaned_text', confidence_score_col='score', moiety_col='moiety')

  assert compare_results(df_results, df_expected, join_columns=['epma_id'])

# COMMAND ----------

@suite.add_test
def test_get_non_moiety_words():
  schema = StructType([
   StructField('id', StringType(), True),
   StructField('DESC', StringType(), True)
  ])

  df_form = spark.createDataFrame([
      ('1', 'Bath additive'),
      ('2', 'Capsule'), 
      ('3', 'Modified-release granules')
  ], schema)
  table_form_name = f'_tmp_{uuid4().hex}'
  df_form.createOrReplaceGlobalTempView(table_form_name)

  df_route = spark.createDataFrame([
    ('1', 'Perineural')
  ], schema)
  table_route_name = f'_tmp_{uuid4().hex}'
  df_route.createOrReplaceGlobalTempView(table_route_name)

  df_unit = spark.createDataFrame([
    ('1', 'microgram/ml')
  ], schema)
  table_unit_name = f'_tmp_{uuid4().hex}'
  df_unit.createOrReplaceGlobalTempView(table_unit_name)

  ls_results = get_non_moiety_words(table_form=f'global_temp.{table_form_name}', table_route=f'global_temp.{table_route_name}', table_unit=f'global_temp.{table_unit_name}')

  drop_table('global_temp', table_form_name)
  drop_table('global_temp', table_route_name)
  drop_table('global_temp', table_unit_name)
    
  ls_results_expected = ['ml', 'granules', 'modified-release granules', 'perineural', 'modified', 'microgram/ml', 'bath additive', 'microgram', 'release', 'capsule',
 'mls', 'granuless', 'modified-release granuless', 'perineurals', 'modifieds', 'microgram/mls', 'bath additives', 'micrograms', 'releases', 'capsules', 'mles', 'granuleses',
 'modified-release granuleses', 'perineurales', 'modifiedes', 'microgram/mles', 'bath additivees', 'microgrames', 'releasees', 'capsulees']
  
  assert set(ls_results_expected).issubset(set(ls_results))

# COMMAND ----------

@suite.add_test
def test_remove_non_moiety_words():
  
  df_input = spark.createDataFrame([
    (1, 'hepatitis b adult vaccine'),
    (2, 'filgrastim 480 mcg/0.5 ml injectable solution'),
    (3, 'relvar ellipta 184 mcg-22 mcg/inh inhalation powder')
  ], ['epma_id', 'epma_description'])
  
  non_moiety_words = ['adult', 'ml', 'injectable', 'solution', 'inhalation', 'powder']
  
  df_expected = spark.createDataFrame([
    (1, 'hepatitis b adult vaccine', 'hepatitis b vaccine'),
    (2, 'filgrastim 480 mcg/0.5 ml injectable solution', 'filgrastim 480 mcg/0.5'),
    (3, 'relvar ellipta 184 mcg-22 mcg/inh inhalation powder', 'relvar ellipta 184 mcg-22 mcg/inh')  
  ], ['epma_id', 'epma_description', 'clean_output'])
  
  df_results = remove_non_moiety_words(df_input, non_moiety_words, text_col='epma_description', clean_text_col='clean_output')
  
  assert compare_results(df_results, df_expected, join_columns=['epma_id'])

# COMMAND ----------

@suite.add_test
def test_map_moiety_ref_data_to_vtm():

  df_amp_dedup = spark.createDataFrame([
    ('apid1', 'E-B2 50mg capsules', 'apid1', 'vpid1', 'APID'),
    ('apid2', 'G & G Vitamin B2 50mg capsules', 'apid2', 'vpid1', 'APID'),
  ], 
    ['ref_id', 'ref_description', 'APID', 'VPID', 'id_level'])

  df_vmp = spark.createDataFrame([
    ('vpid1', 'Riboflavin 50mg capsules', 'vtmid1', 'VPID'),
    ('vpid2', 'Riboflavin 100mg tablets', 'vtmid1', 'VPID'),
    ('vpid3', 'Hepatitis B (rDNA) vaccine suspension for injection 10micrograms/1ml vials', 'vtmid2', 'VPID')
  ], 
    ['ref_id', 'ref_description', 'VTMID', 'id_level'])
  
  df_vtm = spark.createDataFrame([
    ('vtmid1', 'Riboflavin', 'VTMID'),
    ('vtmid2', 'Hepatitis B vaccine', 'VTMID')
  ], 
    ['ref_id', 'ref_description', 'id_level'])

  class MockRefDataStore():
    amp_dedup = df_amp_dedup
    vmp = df_vmp 
    vtm = df_vtm
    ID_COL = 'ref_id'
    TEXT_COL = 'ref_description'
    ID_LEVEL_COL = 'id_level'
    
  df_input = spark.createDataFrame([
    ('apid1', 'E-B2 50mg capsules', 'APID', 'E-B2'),
    ('apid2', 'G & G Vitamin B2 50mg capsules', 'APID', 'G & G Vitamin B2'),
    ('vpid1', 'Riboflavin 50mg capsules', 'VPID', 'Riboflavin'),
    ('vpid2', 'Riboflavin 100mg tablets', 'VPID', 'Riboflavin'),
    ('vpid3', 'Hepatitis B (rDNA) vaccine suspension for injection 10micrograms/1ml vials', 'VPID', 'HBvaxPRO')
  ], 
    ['ref_id', 'ref_description', 'id_level', 'moiety'])
 
  df_expected = spark.createDataFrame([
    ('apid1', 'E-B2 50mg capsules', 'E-B2', 'Riboflavin', 'vtmid1'),
    ('apid2', 'G & G Vitamin B2 50mg capsules', 'G & G Vitamin B2', 'Riboflavin', 'vtmid1'),
    ('vpid1', 'Riboflavin 50mg capsules', 'Riboflavin', 'Riboflavin', 'vtmid1'),
    ('vpid2', 'Riboflavin 100mg tablets', 'Riboflavin', 'Riboflavin', 'vtmid1'),
    ('vpid3', 'Hepatitis B (rDNA) vaccine suspension for injection 10micrograms/1ml vials', 'HBvaxPRO', 'Hepatitis B vaccine', 'vtmid2')
  ], 
    ['ref_id', 'ref_description', 'moiety', 'vtm_ref_description', 'VTMID'])
    
  with FunctionPatch('ReferenceDataFormatter', MockRefDataStore):
    df_results = map_moiety_ref_data_to_vtm(df_input, MockRefDataStore(), ref_vtm_text_col='vtm_ref_description', moiety_col='moiety')
  
  assert compare_results(df_results, df_expected, join_columns=['ref_id'])

# COMMAND ----------

@suite.add_test
def test_split_by_unique_vtm():
  
  df_input = spark.createDataFrame([
    (1, 'hydrocortisone 0.5% topical cream','Hydrocortisone', 100),
    (2, 'Ensure twocal liquid (oral nutritional supplement) 200ml bottle','Twocal HN', 80),
    (3, 'Balneum Plus 5% topical cream','Balneum Plus', 100),
    (4, 'rivastigmine 9.5 mg/24 hours transdermal modified-release patch','Rivastigmine', 100),
    (5, 'aciclovir 5% topical cream','Aciclovir', 100)
  ], ['epma_id', 'epma_description', 'moiety', 'score'])

  df_reference = spark.createDataFrame([
    ('Hydrocortisone', 'hydrocortisone'),
    ('Hydrocortisone', None),
    ('Twocal HN', 'twocal'),
    ('Twocal HN', 'hn'),
    ('Twocal HN', None),
    ('Balneum Plus', 'balneum advanced'),
    ('Balneum Plus', 'balneum'),
    ('Rivastigmine', None),
    ('Aciclovir', 'aciclovir powder')
  ], ['moiety', 'vtm'])

  df_unique_expected = spark.createDataFrame([
      ('Hydrocortisone', 1, 'hydrocortisone 0.5% topical cream', 100, 'hydrocortisone', 'VTMID'),
      ('Aciclovir', 5, 'aciclovir 5% topical cream', 100, 'aciclovir powder', 'VTMID')
    ], ['moiety', 'epma_id', 'epma_description', 'score', 'vtm', 'id_level'])

  df_not_unique_expected = spark.createDataFrame([
      ('Twocal HN', 2, 'Ensure twocal liquid (oral nutritional supplement) 200ml bottle', 80),
      ('Balneum Plus', 3, 'Balneum Plus 5% topical cream', 100),
      ('Rivastigmine', 4, 'rivastigmine 9.5 mg/24 hours transdermal modified-release patch', 100)
    ], ['moiety', 'epma_id', 'epma_description', 'score'])

  df_match_vtm_unique, df_match_vtm_not_unique, df_cp = split_by_unique_vtm(df_input,
                                                                            df_reference,
                                                                            id_col='epma_id',
                                                                            id_level_col='id_level',
                                                                            join_col='moiety',
                                                                            ref_vtm_text_col='vtm')
                  
  assert compare_results(df_match_vtm_unique.df, df_unique_expected, join_columns=['epma_id'])
  assert compare_results(df_match_vtm_not_unique, df_not_unique_expected, join_columns=['epma_id'])
  del df_cp
  del df_match_vtm_unique

# COMMAND ----------

@suite.add_test
def test_remove_strength_unit():
  
  df_input = spark.createDataFrame([
    (1, 'Cisplatin 100mg/1000ml in Sodium chloride 0.9% infusion bags'),
    (2, 'Coal tar solution 10% in Betamethasone valerate 0.025% cream'),
    (3, 'Cordarone X 100'),
    (4, 'Dermacolor body camouflage D52'),
    (5, "Dextran '40' Glucose")
  ], ['epma_id','moiety_all'])
  
  df_expected = spark.createDataFrame([
    (1, 'Cisplatin 100mg/1000ml in Sodium chloride 0.9% infusion bags', 'Cisplatin in Sodium chloride infusion bags'),
    (2, 'Coal tar solution 10% in Betamethasone valerate 0.025% cream', 'Coal tar solution in Betamethasone valerate cream'),
    (3, 'Cordarone X 100', 'Cordarone X'),
    (4, 'Dermacolor body camouflage D52', 'Dermacolor body camouflage D'),
    (5, "Dextran '40' Glucose", "Dextran ' Glucose")
  ], ['epma_id','moiety_all', 'moiety_clean'])

  df_results = df_input.withColumn('moiety_clean', remove_strength_unit('moiety_all'))
  
  assert compare_results(df_results, df_expected, join_columns=['epma_id'])

# COMMAND ----------

@suite.add_test
def test_create_fuzzy_wratio_udf_with_broadcast():
  df_input = spark.createDataFrame([
    (0, 'pog'),
    (1, 'log'),
    (2, 'fog'),
    (3, 'cat'),
    (4, 'dog'),
    (5, 'mob'),
    (6, 'lob'),
    (7, 'play'),
    (8, 'stay'),
    (9, 'may'),
    (10, 'tray'),
    (11, 'bray'),
  ], ['index', 'desc'])

  possible_matches = ['abc', 'def', 'ghi', 'jkl']

  expected_schema = StructType([
    StructField('index', LongType()),
    StructField('desc', StringType()),
    StructField('test_arr', StructType([
      StructField('desc', StringType()),
      StructField('score', LongType()),
    ])),
  ])

  df_expected = spark.createDataFrame([
    (0, 'pog', {'desc': 'ghi', 'score': 33}),
    (1, 'log', {'desc': 'ghi', 'score': 33}),
    (2, 'fog', {'desc': 'def', 'score': 33}),
    (3, 'cat', {'desc': 'abc', 'score': 33}),
    (4, 'dog', {'desc': 'def', 'score': 33}),
    (5, 'mob', {'desc': 'abc', 'score': 33}),
    (6, 'lob', {'desc': 'abc', 'score': 33}),
    (7, 'play', {'desc': 'abc', 'score': 29}),
    (8, 'stay', {'desc': 'abc', 'score': 29}),
    (9, 'may', {'desc': 'abc', 'score': 33}),
    (10, 'tray', {'desc': 'abc', 'score': 29}),
    (11, 'bray', {'desc': 'abc', 'score': 29}),
  ], expected_schema)

  possible_matches_broadcast = sc.broadcast(possible_matches)
  test_func_udf = create_fuzzy_wratio_udf_with_broadcast(possible_matches_broadcast, output_desc_col='desc', output_score_col='score')
  df_actual = df_input.withColumn('test_arr', test_func_udf(col('desc')))

  assert compare_results(df_actual, df_expected, join_columns=['index'])

  assert df_actual.where(col('index') == lit(2)).select('test_arr').collect()[0][0]['desc'] == 'def'

# COMMAND ----------

@suite.add_test
def test_run_wratio_fuzzy_match_to_ref_data():
  
  df_input = spark.createDataFrame([
    (0, 'a'),
    (1, 'b'),
    (2, 'c'),
  ], ['index', 'text_col'])

  df_input_ref_data = spark.createDataFrame([
    ('0', 'aa', 'APID'),
    ('1', 'bb', 'VPID'),
    ('2', 'cc', 'VTMID'),
  ], ['id', 'ref_text_col', 'id_level'])

  df_expected = spark.createDataFrame([
    (0, 'a', 'aa', 50, 'APID', '0'),
    (1, 'b', 'bb', 25, 'VPID', '1'),
    (2, 'c', 'cc', 75, 'VTMID', '2'),
  ], ['index', 'text_col', 'match_term', 'confidence_score', 'id_level', 'match_id'])

  def mock_func(check_matches_broadcast: Broadcast, output_desc_col='epma_description', output_score_col='confidence_score'):
    return_schema = StructType([
      StructField(output_desc_col, StringType(), False),
      StructField(output_score_col, LongType(), False),
    ])
    @F.udf(returnType=return_schema)
    def _func(column_data) -> str:
      assert sorted(check_matches_broadcast.value) == sorted(['aa', 'bb', 'cc'])
      if column_data == 'a':
        return {output_desc_col: 'aa', output_score_col: 50} 
      elif column_data == 'b':
        return {output_desc_col: 'bb', output_score_col: 25} 
      elif column_data == 'c':
        return {output_desc_col: 'cc', output_score_col: 75} 
    return _func

  with FunctionPatch('create_fuzzy_wratio_udf_with_broadcast', mock_func):
    df_actual = run_wratio_fuzzy_match_to_ref_data(df_input,
                                                  df_input_ref_data,
                                                  text_col='text_col',
                                                  confidence_score_col='confidence_score',
                                                  match_term_col='match_term',
                                                  id_level_col='id_level',
                                                  match_id_col='match_id',
                                                  ref_data_id_col='id',
                                                  ref_data_text_col='ref_text_col')

    assert compare_results(df_actual, df_expected, join_columns=['index'])

# COMMAND ----------

@suite.add_test
def test_best_match_fuzzy_wratio():
  df_input = spark.createDataFrame([
    (0, 'a'),
    (1, 'b'),
    (2, 'c'),
  ], ['index', 'text_col'])

  df_input_ref_data_amp = spark.createDataFrame([
    ('0', 'aa', 'APID'),
  ], ['_id', 'text_col', 'id_level'])

  df_input_ref_data_vmp = spark.createDataFrame([
    ('1', 'bb', 'VPID'),
  ], ['_id', 'text_col', 'id_level'])

  df_input_ref_data_vtm = spark.createDataFrame([
    ('2', 'cc', 'VTMID'),
  ], ['_id', 'text_col', 'id_level'])

  df_expected = spark.createDataFrame([
    (1, 'bb', 25, 'bb', 'VPID', '1', 'fuzzy_high_score_test_vmp'),
  ], ['index', 'text_col', 'confidence_score', 'match_term', 'id_level', 'match_id', 'match_level'])

  
  class MockRefDataStore():
    amp_dedup = df_input_ref_data_amp
    vmp = df_input_ref_data_vmp 
    vtm = df_input_ref_data_vtm
    ID_COL = '_id'
    TEXT_COL = 'text_col' 
  
  def mock_run_wratio_fuzzy_match_to_ref_data(df_src_to_match: DataFrame, df_ref_data: DataFrame, text_col: str='epma_description', 
                                              confidence_score_col: str='confidence_score', match_term_col: str='match_term',
                                              id_level_col: str='id_level',  match_id_col: str='match_id', ref_data_id_col: str='id',
                                              ref_data_text_col: str='description'):
    assert compare_results(df_ref_data, df_input_ref_data_vmp, join_columns=['_id'])
    assert text_col == 'text_col'
    return df_expected

  with FunctionPatch('run_wratio_fuzzy_match_to_ref_data', mock_run_wratio_fuzzy_match_to_ref_data):
    with FunctionPatch('ReferenceDataFormatter', MockRefDataStore):
      df_actual = best_match_fuzzy_wratio(MockRefDataStore(),
                                         df_input,
                                         'vmp',
                                         text_col='text_col',
                                         confidence_score_col='confidence_score',
                                         match_term_col='match_term',
                                         match_level_col='match_level',
                                         id_level_col='id_level',
                                         match_id_col='match_id',
                                         match_level_value='fuzzy_high_score_test_vmp')
  
      assert compare_results(df_actual, df_expected, join_columns=['index'])

# COMMAND ----------

@suite.add_test
def test_diluent_best_match_fuzzy_wratio():
  df_input = spark.createDataFrame([
    (0, 'a'),
    (1, 'b'),
    (2, 'c'),
  ], ['index', 'text_col'])

  df_input_ref_data_amp = spark.createDataFrame([
    ('0', 'aa sodium chloride', 'APID', 'fake_VPID'),
    ('1', 'aa', 'APID', 'fake_VPID'),
  ], ['_id', 'description', 'id_level', 'VPID'])

  df_input_ref_data_vmp = spark.createDataFrame([
    ('2', 'bb', 'VPID', 'fake_VTMID'),
    ('3', 'bb sodium chloride', 'VPID', 'fake_VTMID'),
  ], ['_id', 'description', 'id_level', 'VTMID'])

  df_input_ref_data_vtm = spark.createDataFrame([
    ('4', 'sodium chloridecc ', 'VTMID'),
    ('5', 'cc', 'VTMID'),
  ], ['_id', 'description', 'id_level'])

  df_input_ref_data = spark.createDataFrame([
    ('0', 'aa sodium chloride', 'APID'),
    ('3', 'bb sodium chloride', 'VPID'),
    ('4', 'sodium chloridecc ', 'VTMID'),
  ], ['_id', 'description', 'id_level'])

  df_expected = spark.createDataFrame([
    (0, 'aa sodium chloride', 50, 'aa sodium chloride', 'APID', '0', 'fuzzy_diluent'),
    (1, 'bb sodium chloride', 25, 'bb sodium chloride', 'VPID', '3', 'fuzzy_diluent'),
    (2, 'sodium chloridecc ', 75, 'sodium chloridecc ', 'VTMID', '4', 'fuzzy_diluent'),
  ], ['index', 'text_col', 'confidence_score', 'match_term', 'id_level', 'match_id', 'match_level'])

  class MockRefDataStore():
    amp_dedup = df_input_ref_data_amp
    vmp = df_input_ref_data_vmp 
    vtm = df_input_ref_data_vtm
    ID_COL = '_id'
    TEXT_COL = 'description' 

  def mock_run_wratio_fuzzy_match_to_ref_data(df_src_to_match: DataFrame, df_ref_data: DataFrame, text_col: str='epma_description', 
                                              confidence_score_col: str='confidence_score', match_term_col: str='match_term',
                                              id_level_col: str='id_level',  match_id_col: str='match_id', ref_data_id_col: str='id', 
                                              ref_data_text_col: str='description'):
    assert compare_results(df_ref_data, df_input_ref_data, join_columns=['_id'])
    assert text_col == 'text_col'
    return df_expected

  with FunctionPatch('run_wratio_fuzzy_match_to_ref_data', mock_run_wratio_fuzzy_match_to_ref_data):
    df_actual = diluent_best_match_fuzzy_wratio(MockRefDataStore(),
                                                df_input,
                                                search_term='sodium chloride',
                                                text_col='text_col',
                                                confidence_score_col='confidence_score',
                                                match_term_col='match_term',
                                                match_level_col='match_level',
                                                id_level_col='id_level',
                                                match_id_col='match_id')

    assert compare_results(df_actual, df_expected, join_columns=['index'])

# COMMAND ----------

@suite.add_test
def test_diluent_fuzzy_match_step():
  df_input = spark.createDataFrame([
    ('0', 'a and sodium chloride', 'fake_original0', ' ', 'fake_entity_match_id'),
    ('1', 'b', 'fake_original1', ' ', 'fake_entity_match_id'),
    ('2', '+ sodium chloride c', 'fake_original2', ' ', 'fake_entity_match_id'),
    ('2', '+ sodium chloride c', 'fake_original2', ' ', 'fake_entity_match_id'),
    ('4', 'a + glucose', 'fake_original4', ' ', 'fake_entity_match_id'),
    ('5', 'a and glucose', 'fake_original5', ' ', 'fake_entity_match_id'),
    ('6', 'glucose 500mg', 'fake_original6', ' ', 'fake_entity_match_id_1'),
    ('7', 'a and phosphate', 'fake_original7', ' ', 'fake_entity_match_id'),
  ], ['epma_id', 'text_col', 'original_text_col', 'form_in_text_col', 'match_id'])

  df_remaining_expected = spark.createDataFrame([
    ('1', 'b', 'fake_original1', ' ', 'fake_entity_match_id'),
    ('6', 'glucose 500mg', 'fake_original6', ' ', 'fake_entity_match_id_1'),
  ], ['epma_id', 'text_col', 'original_text_col', 'form_in_text_col', 'match_id'])

  timestamp = datetime(2020, 5, 6, 1, 1, 1)

  df_mappable_expected = spark.createDataFrame([
    ('3', 'fake_original3', ' ', 'aa sodium chloride', '0', 'APID', 'fuzzy_diluent', timestamp),
    ('4', 'fake_original4', ' ', 'a + glucose', '7', 'APID', 'fuzzy_diluent', timestamp),
  ], ['epma_id', 'original_text_col', 'form_in_text_col', 'text_col', 'match_id', 'id_level', 'match_level', 'match_datetime'])

  df_unmappable_expected = spark.createDataFrame([
    ('fake_original0', ' ', 'fuzzy_diluent_low_score', timestamp),
    ('fake_original2', ' ', 'fuzzy_diluent_low_score', timestamp),
    ('fake_original5', ' ', 'fuzzy_diluent_low_score', timestamp),
    ('fake_original7', ' ', 'fuzzy_diluent_low_score', timestamp),
  ], ['original_text_col', 'form_in_text_col', 'reason', 'match_datetime'])

  global mock_call_count
  mock_call_count = 0

  def mock_diluent_best_match_fuzzy_wratio(ref_data_dict, df_input, search_term, text_col='text_col', **kwargs):
    df_input_best_match_sodium_chloride_expected = spark.createDataFrame([
      ('0', 'a and sodium chloride', 'fake_original0', ' '),
      ('2', '+ sodium chloride c', 'fake_original2', ' '),
    ], ['epma_id', 'text_col', 'original_text_col', 'form_in_text_col'])

    df_input_best_match_glucose_expected = spark.createDataFrame([
      ('4', 'a + glucose', 'fake_original4', ' '),
      ('5', 'a and glucose', 'fake_original5', ' '),
    ], ['epma_id', 'text_col', 'original_text_col', 'form_in_text_col'])

    df_input_best_match_phosphate_expected = spark.createDataFrame([
      ('7', 'a and phosphate', 'fake_original7', ' '),
    ], ['epma_id', 'text_col', 'original_text_col', 'form_in_text_col'])

    assert text_col == 'text_col'
    global mock_call_count
    if mock_call_count == 0:
      assert compare_results(df_input, df_input_best_match_sodium_chloride_expected, join_columns=['epma_id'])
      mock_call_count +=1
      return spark.createDataFrame([
        ('3', 'fake_original3', ' ', 'aa sodium chloride', 99, 'aa sodium chloride', 'APID', '0', 'fuzzy_diluent'),
        ('2', 'fake_original2', ' ', 'a and sodium chloride', 50, 'a and sodium chloride', 'VTMID', '2', 'fuzzy_diluent'),
      ], ['epma_id', 'original_text_col', 'form_in_text_col', 'text_col', '_confidence_score', 'match_term', 'id_level', 'match_id', 'match_level'])
    elif mock_call_count == 1:
      assert compare_results(df_input, df_input_best_match_glucose_expected, join_columns=['epma_id'])
      mock_call_count +=1
      return spark.createDataFrame([
        ('4', 'fake_original4', ' ', 'a + glucose', 99, 'a + glucose', 'APID', '7', 'fuzzy_diluent'),
        ('5', 'fake_original5', ' ', 'a and glucose', 50, 'a and glucose', 'APID', '3', 'fuzzy_diluent'),
      ], ['epma_id', 'original_text_col', 'form_in_text_col', 'text_col', '_confidence_score', 'match_term', 'id_level', 'match_id', 'match_level'])
    elif mock_call_count == 2:
      assert compare_results(df_input, df_input_best_match_phosphate_expected, join_columns=['epma_id'])
      mock_call_count +=1
      return spark.createDataFrame([
        ('7', 'fake_original7', ' ', 'a and phosphate', 50, 'a and phosphate', 'APID', '8', 'fuzzy_diluent'),
      ], ['epma_id', 'original_text_col', 'form_in_text_col', 'text_col', '_confidence_score', 'match_term', 'id_level', 'match_id', 'match_level'])
    else:
      assert False

  class MockRefDataStore():
    ID_LEVEL_COL = 'id_level' 

  with FunctionPatch('diluent_best_match_fuzzy_wratio', mock_diluent_best_match_fuzzy_wratio):
    step1_output = diluent_fuzzy_match_step(df_input,
                                            MockRefDataStore(),
                                            confidence_threshold=98,
                                            id_col='epma_id',
                                            original_text_col='original_text_col',
                                            text_col='text_col',
                                            form_in_text_col='form_in_text_col',
                                            match_term_col='match_term',
                                            match_level_col='match_level',
                                            id_level_col='id_level',
                                            match_id_col='match_id',
                                            match_datetime_col='match_datetime',
                                            reason_col='reason',
                                            timestamp=timestamp) 

    assert compare_results(step1_output.df_remaining.df, df_remaining_expected, join_columns=['epma_id'])
    assert compare_results(step1_output.df_mappable, df_mappable_expected, join_columns=['epma_id'])    
    assert compare_results(step1_output.df_unmappable, df_unmappable_expected, join_columns=['original_text_col'])    

# COMMAND ----------

@suite.add_test
def test_linked_fuzzy_matching_step():
  df_input = spark.createDataFrame([
    (0, '0', 'doesnt match at ', 'fake_original0', ' ', 'fake_entity_match_id'),
    (1, '1', 'b', 'fake_original1', ' ', 'fake_entity_match_id'),
    (2, '2', 'no strength match', 'no strength match', ' ', 'fake_entity_match_id'),
    (3, '3', 'paracetamol', 'paracetamol', ' ', 'fake_entity_match_id'),
    (4, '4', 'doesnt match at x', 'fake_original4', ' ', 'fake_entity_match_id'),
    (4, '4', 'doesnt match at x', 'fake_original4', ' ', 'fake_entity_match_id'),
    (5, '5', 'very obscure medicine', 'very obscure medicine', ' ', None),
    (5, '5', 'very obscure medicine', 'very obscure medicine', ' ', None),
    (6, '6', 'zidovudine 50 mg in 5 ml syrup', 'zidovudine 50 mg in 5 ml syrup', ' ', 'fake_entity_match_id')
  ], ['index', 'epma_id', 'text_col', 'original_text_col', 'form_in_text_col', 'match_id'])

  df_remaining_expected = spark.createDataFrame([
    (2, '2', 'no strength match', 'no strength match', ' ', 'fake_entity_match_id'),
    (3, '3', 'paracetamol', 'paracetamol', ' ', 'fake_entity_match_id'),
    (5, '5', 'very obscure medicine', 'very obscure medicine', ' ', None),
    (6, '6', 'zidovudine 50 mg in 5 ml syrup', 'zidovudine 50 mg in 5 ml syrup', ' ', 'fake_entity_match_id')
  ], ['index', 'epma_id', 'text_col', 'original_text_col', 'form_in_text_col', 'match_id'])
  timestamp = datetime(2020, 5, 6, 1, 1, 1)

  df_mappable_expected = spark.createDataFrame([
    ('0', 'fake_original0', ' ', 'doesnt match at ', 'fake_entity_match_id', 'VPID', 'fuzzy', timestamp),
    ('1', 'fake_original1', ' ', 'b', 'fake_entity_match_id', 'VPID', 'fuzzy', timestamp),
  ], ['epma_id', 'original_epma_description', 'form_in_text', 'epma_description', 'match_id', 'id_level', 'match_level', 'match_datetime'])

  df_unmappable_expected = spark.createDataFrame([
    ('fake_original4', ' ', 'fuzzy_linked_tied_confidence_score', timestamp)
  ], ['original_epma_description', 'form_in_text', 'reason', 'match_datetime'])

  def mock_best_match_fuzzy_wratio(ref_data_dict, df_input_best_match_fuzzy_wratio, *args, **kwargs):

    df_wratio_expected = spark.createDataFrame([
      (0, '0', 'doesnt match at ', 'fake_original0', ' '),
      (1, '1', 'b', 'fake_original1', ' '),
      (2, '2', 'no strength match', 'no strength match', ' '),
      (3, '3', 'paracetamol', 'paracetamol', ' '),
      (4, '4', 'doesnt match at x', 'fake_original4', ' '),
      (6, '6', 'zidovudine 50 mg in 5 ml syrup', 'zidovudine 50 mg in 5 ml syrup', ' ')
    ], ['index', 'epma_id', 'text_col', 'original_text_col', 'form_in_text_col'])
    assert compare_results(df_input_best_match_fuzzy_wratio, df_wratio_expected, join_columns=['index'])
    return spark.createDataFrame([
      (0, '0', 'doesnt match at ', 'fake_original0', ' ', 99),
      (1, '1', 'b', 'fake_original1', ' ', 10),
      (2, '2', 'no strength match', 'no strength match', ' ', 80),
      (3, '3', 'paracetamol', 'paracetamol', ' ', 1)
    ], ['index', 'epma_id', 'text_col', 'original_text_col', 'form_in_text_col', '_confidence_score'])

  def mock_add_strength_match_check_column(*args, **kwargs):
    return spark.createDataFrame([
      (0, '0', 'doesnt match at ', 'fake_original0', ' ', 99, True, 'fake_entity_match_id', 'fuzzy', 'VPID', 'timestamp'),
      (1, '1', 'b', 'fake_original1', ' ', 10, True, 'fake_entity_match_id', 'fuzzy', 'VPID', 'timestamp'),
      (2, '2', 'no strength match', 'no strength match', ' ', 80, False, 'fake_entity_match_id', 'fuzzy', 'VPID', 'timestamp'),
      (3, '3', 'paracetamol', 'paracetamol', ' ', 1, True, 'fake_entity_match_id', 'fuzzy', 'VPID', 'timestamp'),
      (6, '6', 'zidovudine 50 mg in 5 ml syrup', 'zidovudine 50 mg in 5 ml syrup', ' ', 85, False, 'fake_entity_match_id', 'fuzzy', 'APID', 'timestamp')
    ], ['index', 'epma_id', 'epma_description', 'original_epma_description', 'form_in_text', '_confidence_score', 
        '_has_strength', 'match_id', 'match_level', 'id_level', 'match_datetime'])

  def mock_select_best_match_from_scored_records(df_input_select_best_match, *args, **kwargs):
    df_select_best_match_expected = spark.createDataFrame([
      (1, '1', 'b', 'fake_original1', ' ', 'fake_entity_match_id'),
      (2, '2', 'no strength match', 'no strength match', ' ', 'fake_entity_match_id'),
      (3, '3', 'paracetamol', 'paracetamol', ' ', 'fake_entity_match_id'),
      (4, '4', 'doesnt match at x', 'fake_original4', ' ', 'fake_entity_match_id'),
      (4, '4', 'doesnt match at x', 'fake_original4', ' ', 'fake_entity_match_id'),
      (6, '6', 'zidovudine 50 mg in 5 ml syrup', 'zidovudine 50 mg in 5 ml syrup', ' ', 'fake_entity_match_id')
    ], ['index', 'epma_id', 'text_col', 'original_text_col', 'form_in_text_col', 'match_id'])
    assert compare_results(df_input_select_best_match, df_select_best_match_expected, join_columns=['index'])
    return (
      DFCheckpoint(spark.createDataFrame([
        (1, '1', 'b', 'fake_original1', ' ', 99, 'fake_entity_match_id', 'fuzzy', 'VPID', True),
        (2, '2', 'no strength match', 'no strength match', ' ', 69, 'fake_entity_match_id', 'fuzzy', 'VPID', True),
        (3, '3', 'paracetamol', 'paracetamol', ' ', 1, 'fake_entity_match_id', 'fuzzy', 'VPID', True),
        (6, '6', 'zidovudine 50 mg in 5 ml syrup', 'zidovudine 50 mg in 5 ml syrup', ' ', 85, 'fake_entity_match_id', 'fuzzy', 'APID', False)
      ], ['index', 'epma_id', 'epma_description', 'original_epma_description', 'form_in_text', '_confidence_score', 
          'match_id', 'match_level', 'id_level', '_has_strength'])),
      DFCheckpoint(spark.createDataFrame([
        ('4', 'fake_original4', ' ', 'fuzzy_linked_tied_confidence_score', timestamp)
      ], ['epma_id', 'original_epma_description', 'form_in_text', 'reason', 'match_datetime']))
    )

  with FunctionPatch('select_best_match_from_scored_records', mock_select_best_match_from_scored_records):
    with FunctionPatch('best_match_fuzzy_wratio', mock_best_match_fuzzy_wratio):
      with FunctionPatch('add_strength_match_check_column', mock_add_strength_match_check_column):
        step2_output = linked_fuzzy_matching_step(df_input,
                                                  None,
                                                  confidence_threshold_vtm_direct_match=95,
                                                  confidence_threshold_fuzzy_match=70,
                                                  id_col='epma_id',
                                                  match_id_col='match_id',
                                                  original_text_col='original_epma_description',
                                                  form_in_text_col='form_in_text',
                                                  text_col='epma_description',
                                                  id_level_col='id_level',
                                                  match_level_col='match_level',
                                                  match_term_col='match_term',
                                                  match_datetime_col='match_datetime',
                                                  reason_col='reason',
                                                  timestamp=timestamp)
        
        assert compare_results(df_remaining_expected, step2_output.df_remaining, join_columns=['epma_id'])
        assert compare_results(df_mappable_expected, step2_output.df_mappable.df, join_columns=['epma_id'])    
        assert compare_results(df_unmappable_expected, step2_output.df_unmappable.df, join_columns=['original_epma_description'])
        step2_output.df_mappable.delete()
        step2_output.df_unmappable.delete() 

# COMMAND ----------

@suite.add_test
def test_best_wratio_match_at_all_levels():

  df_step2_remaining = spark.createDataFrame([
    ('0', 'Umeclidinium 55 micrograms Inhalation Powder', ' ', 'umeclidinium 55 micrograms inhalation powder'),
    ('1', 'PARACETAMOL 500 mg in 50mL Intravenous Infusion', ' ', 'paracetamol 500 mg in 50ml intravenous infusion'),
    ('2', 'TOBRAMYCIN (Tobi) 300 mg /5ml Nebuliser Solution', ' ', 'tobramycin (tobi) 300 mg /5ml nebuliser solution'),
    ('3', 'Enoxaparin 80mg in 0.8mL Pre-filled Syringe', ' ', 'enoxaparin 80mg in 0.8ml pre-filled syringe'),
    ('4', 'Medicine which strength match at amp only 300 mg in 50mL', ' ', 'Medicine which strength match at amp only 300 mg in 50mL')
  ], 
    ['epma_id', 'original_epma_description', 'form_in_text', 'epma_description'])

  df_input_ref_data_amp = spark.createDataFrame([
      ('10', 'umeclidinium 55 mg inhalation powder', 'APID'),
      ('11', 'Medicine which strength match at amp only 300 mg in 50mL', 'APID')
    ], ['_id', 'description', 'id_level'])

  df_input_ref_data_vmp = spark.createDataFrame([
    ('12', 'paracetamol 500 mg/50ml intravenous infusion', 'VPID'),
    ('13', 'tobramycin (tobi) 300 mg in 5ml nebuliser solution', 'VPID'),
    ('14', 'Medicine which strength match at amp only 100 mg in 10mL', 'VPID')
  ], ['_id', 'description', 'id_level'])

  df_input_ref_data_vtm = spark.createDataFrame([
    ('15', 'enoxaparin 80mg in 0.8ml', 'VTMID'),
    ('16', 'Medicine which strength match at amp only', 'VTMID')
  ], ['_id', 'description', 'id_level'])

  class MockRefDataStore():
    amp_dedup = df_input_ref_data_amp
    vmp = df_input_ref_data_vmp 
    vtm = df_input_ref_data_vtm
    ID_COL = '_id'
    TEXT_COL = 'description' 

  df_expected = spark.createDataFrame([
    ('1', 'PARACETAMOL 500 mg in 50mL Intravenous Infusion', ' ', 'paracetamol 500 mg in 50ml intravenous infusion', 'VPID', '12', 'paracetamol 500 mg/50ml intravenous infusion', 'fuzzy_non_linked', 97, True),
    ('2', 'TOBRAMYCIN (Tobi) 300 mg /5ml Nebuliser Solution', ' ', 'tobramycin (tobi) 300 mg /5ml nebuliser solution', 'VPID', '13', 'tobramycin (tobi) 300 mg in 5ml nebuliser solution', 'fuzzy_non_linked', 98, True),
    ('4', 'Medicine which strength match at amp only 300 mg in 50mL', ' ', 'Medicine which strength match at amp only 300 mg in 50mL', 'APID', '11', 'Medicine which strength match at amp only 300 mg in 50mL', 'fuzzy_non_linked', 100, True)
  ], 
    ['epma_id', 'original_epma_description', 'form_in_text', 'epma_description', 'id_level', 'match_id', 'match_term', 'match_level', 'confidence_score', 'has_strength'])

  with FunctionPatch('ReferenceDataFormatter', MockRefDataStore):
    direct_matched = best_wratio_match_at_all_levels(MockRefDataStore(),
                                                     df_step2_remaining,
                                                     confidence_threshold = 95,   
                                                     id_col='epma_id',
                                                     text_col='epma_description',
                                                     confidence_score_col='confidence_score',
                                                     match_term_col='match_term',
                                                     match_level_col='match_level',
                                                     id_level_col='id_level',
                                                     match_id_col='match_id',
                                                     original_text_col='original_epma_description',
                                                     form_in_text_col='form_in_text',
                                                     has_strength_col='has_strength') \
                                    .select('epma_id', 'original_epma_description', 'form_in_text', 'epma_description', 'id_level', 'match_id', 'match_term', 'match_level', 'confidence_score', 'has_strength')

  assert compare_results(direct_matched, df_expected, join_columns=['epma_id'], allow_nullable_schema_mismatch=True)

# COMMAND ----------

@suite.add_test
def test_full_matches_fuzzy_matching_step():

  df_step2_remaining = spark.createDataFrame([
      ('0', 'Umeclidinium 55 micrograms Inhalation Powder', ' ', 'umeclidinium 55 micrograms inhalation powder'),
      ('1', 'PARACETAMOL 500 mg in 50mL Intravenous Infusion', ' ', 'paracetamol 500 mg in 50ml intravenous infusion'),
      ('2', 'TOBRAMYCIN (Tobi) 300 mg /5ml Nebuliser Solution', ' ', 'tobramycin (tobi) 300 mg /5ml nebuliser solution'),
      ('3', 'Enoxaparin 80mg in 0.8mL Pre-filled Syringe', ' ', 'enoxaparin 80mg in 0.8ml pre-filled syringe')
    ], 
      ['epma_id', 'original_epma_description', 'form_in_text', 'epma_description'])

  df_input_ref_data_amp = spark.createDataFrame([
      ('10', 'umeclidinium 55 mg inhalation powder', 'APID')
    ], ['epma_id', 'epma_description', 'id_level'])

  df_input_ref_data_vmp = spark.createDataFrame([
    ('12', 'paracetamol 500 mg/50ml intravenous infusion', 'VPID'),
    ('13', 'tobramycin (tobi) 300 mg in 5ml nebuliser solution', 'VPID')
  ], ['epma_id', 'epma_description', 'id_level'])

  df_input_ref_data_vtm = spark.createDataFrame([
    ('14', 'enoxaparin 80mg in 0.8ml', 'VTMID')
  ], ['epma_id', 'epma_description', 'id_level'])

  class MockRefDataStore():
    amp = df_input_ref_data_amp
    vmp = df_input_ref_data_vmp 
    vtm = df_input_ref_data_vtm

  df_remaining_expected = spark.createDataFrame([
      ('0', 'Umeclidinium 55 micrograms Inhalation Powder', ' ', 'umeclidinium 55 micrograms inhalation powder'),
      ('3', 'Enoxaparin 80mg in 0.8mL Pre-filled Syringe', ' ', 'enoxaparin 80mg in 0.8ml pre-filled syringe')
    ], 
      ['epma_id', 'original_epma_description', 'form_in_text', 'epma_description'])

  df_mappable_expected = spark.createDataFrame([
    ('1', 'PARACETAMOL 500 mg in 50mL Intravenous Infusion', ' ', 'paracetamol 500 mg in 50ml intravenous infusion', '12', 'VPID', 'fuzzy_non_linked'),
    ('2', 'TOBRAMYCIN (Tobi) 300 mg /5ml Nebuliser Solution', ' ', 'tobramycin (tobi) 300 mg /5ml nebuliser solution', '13', 'VPID', 'fuzzy_non_linked')  
  ], ['epma_id', 'original_epma_description', 'form_in_text', 'epma_description', 'match_id', 'id_level', 'match_level'])

  def mock_best_wratio_match_at_all_levels(*args, **kwargs):
      return spark.createDataFrame([
      ('1', 'PARACETAMOL 500 mg in 50mL Intravenous Infusion', ' ', 'paracetamol 500 mg in 50ml intravenous infusion', 'VPID', '12', 'paracetamol 500 mg in 50ml intravenous infusion', 'fuzzy_non_linked', 97, True),
      ('2', 'TOBRAMYCIN (Tobi) 300 mg /5ml Nebuliser Solution', ' ', 'tobramycin (tobi) 300 mg /5ml nebuliser solution', 'VPID', '13', 'tobramycin (tobi) 300 mg /5ml nebuliser solution', 'fuzzy_non_linked', 98, True)
    ], 
      ['epma_id', 'original_epma_description', 'form_in_text', 'epma_description', 'id_level', 'match_id', 'match_term', 'match_level', '_confidence_score', 'has_strength'])

  with FunctionPatch('best_wratio_match_at_all_levels', mock_best_wratio_match_at_all_levels):
    step3_output = full_fuzzy_matching_step(MockRefDataStore(),
                                            df_step2_remaining,
                                            confidence_threshold=95,
                                            id_col='epma_id',
                                            original_text_col='original_epma_description',
                                            form_in_text_col='form_in_text',
                                            text_col='epma_description',
                                            match_term_col='match_term',
                                            match_level_col='match_level',
                                            id_level_col='id_level',
                                            match_id_col='match_id',
                                            match_datetime_col='match_datetime')

  assert compare_results(step3_output.df_mappable.df.drop('match_datetime'), df_mappable_expected, join_columns=['epma_id'], allow_nullable_schema_mismatch=True)
  assert compare_results(step3_output.df_remaining, df_remaining_expected, join_columns=['epma_id'], allow_nullable_schema_mismatch=True)
  step3_output.df_mappable.delete()

# COMMAND ----------

@suite.add_test
def test_get_numbers_from_text_udf():
  df_input = spark.createDataFrame([
    ('0', 'abc'),
    ('1', 'Name 0.54'),
    ('2', 'Name 0.54 NameY 1.4578'),
    ('3', 'Name 0.54.67'),
    ('4', 'A0.54B'),
    ('5', '0.5 Name'),
    ('6', 'Name 10000000000000'),
    ('7', 'Name .7'),
    ('8', 'Name 0.5 67'),
  ], ['index', 'value'])

  df_expected = spark.createDataFrame([
    ('0', 'abc', []),
    ('1', 'Name 0.54', ['0.54']),
    ('2', 'Name 0.54 NameY 1.4578', ['0.54', '1.4578']),
    ('3', 'Name 0.54.67', ['0.54.67']),
    ('4', 'A0.54B', ['0.54']),
    ('5', '0.5 Name', ['0.5']),
    ('6', 'Name 10000000000000', ['10000000000000']),
    ('7', 'Name .7', ['0.7']),
    ('8', 'Name 0.5 67', ['0.5', '67']),
  ], ['index', 'value', 'num_arr'])

  df_actual = df_input.withColumn('num_arr', get_numbers_from_text_udf(col('value')))
  assert compare_results(df_actual, df_expected, join_columns=['index'])

# COMMAND ----------

@suite.add_test
def test_add_strength_match_check_column():
  df_input = spark.createDataFrame([
    ('0', 'namea', 'nameb'),
    ('1', 'namea 0.5', 'nameb 0.5'),
    ('2', 'namea 0.5', 'nameb 0.7'),
    ('3', 'namea 0.5', 'nameb'),
    ('4', 'namea', 'nameb 0.9'),
    ('5', 'namea .9', 'nameb 0.9'),
    ('6', '0.6 namea 7.89', '7.89 nameb 0.6'),
    ('7', '0.6 namea 7.89', '7.89 nameb 0.65'),
    ('8', '0.6 namea 7.89 78ml/56g', '7.89 nameb 0.6 78g/56ml'),
    ('9', '0.6 namea 7.89 77ml/56g', '7.89 nameb 0.65 78g/56ml'),
    ('10', 'namea 45', 'nameb 0.9 78 45'),
    ('11', 'namea 45 45', 'nameb 45'),
    ('12', 'namea 45 45 65 65', 'nameb 45 65'),
  ], ['index', 'src_desc', 'ref_desc'])

  df_expected = spark.createDataFrame([
    ('0', 'namea', 'nameb', True),
    ('1', 'namea 0.5', 'nameb 0.5', True),
    ('2', 'namea 0.5', 'nameb 0.7', False),
    ('3', 'namea 0.5', 'nameb', False),
    ('4', 'namea', 'nameb 0.9', False),
    ('5', 'namea .9', 'nameb 0.9', True),
    ('6', '0.6 namea 7.89', '7.89 nameb 0.6', True),
    ('7', '0.6 namea 7.89', '7.89 nameb 0.65', False),
    ('8', '0.6 namea 7.89 78ml/56g', '7.89 nameb 0.6 78g/56ml', True),
    ('9', '0.6 namea 7.89 77ml/56g', '7.89 nameb 0.65 78g/56ml', False),
    ('10', 'namea 45', 'nameb 0.9 78 45', False),
    ('11', 'namea 45 45', 'nameb 45', True),
    ('12', 'namea 45 45 65 65', 'nameb 45 65', True),
  ], ['index', 'src_desc', 'ref_desc', 'is_strength_match'])

  df_actual = add_strength_match_check_column(df_input, text_col='src_desc', match_term_col='ref_desc', has_strength_col='is_strength_match')

  assert compare_results(df_actual, df_expected, join_columns=['index'])

# COMMAND ----------

@suite.add_test
def test_map_amp_to_vmp_if_there_are_amp_desc_duplicates_or_matching_vmp_desc():
  df_input = spark.createDataFrame([
    ('0', 'a', ' ', 'a', '400', 'APID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)), #default, no match
    ('1', 'b', ' ', 'b', '401', 'VPID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)), #not apid match
    ('2', 'c', ' ', 'c', '100', 'APID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)), #default, match
    ('3', 'd', ' ', 'd', '102', 'APID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)), #amp match dup desc
    ('4', 'e', ' ', 'e', '104', 'APID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)), #amp match same vmp desc
    ('5', 'f', ' ', 'f', '105', 'APID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)), #amp match dup desc, same vmp desc
    ('6', 'g', ' ', 'g', '108', 'APID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)), #same amp match
    ('7', 'h', ' ', 'h', '108', 'APID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)),
    ('8', 'i', ' ', 'i', '109', 'APID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)), #same amp match, dup amp desc
    ('9', 'j', ' ', 'j', '109', 'APID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)),
    ('10', 'k', ' ', 'k', '111', 'APID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)), #same amp match, one vmp
    ('11', 'l', ' ', 'l', '111', 'APID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)),
    ('12', 'm', ' ', 'm', '112', 'APID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)), #same amp match, one vmp with same amp desc
    ('13', 'n', ' ', 'n', '112', 'APID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)),
  ], ['id', 'original_text', 'form_in_text', 'text', 'match_id', 'id_level', 'match_level', 'match_datetime'])

  df_amp = spark.createDataFrame([
    ('100', 'aa', '100', '200'), #default
    ('101', 'ab', '101', '201'), #same desc
    ('102', 'ab', '102', '202'),
    ('103', 'ab', '103', '203'),
    ('104', 'ac', '104', '204'), #same vmp desc 
    ('105', 'ad', '105', '205'), #same desc, vmp same desc
    ('106', 'ad', '106', '206'),
    ('107', 'ae', '107', '207'), #no src match
    ('108', 'af', '108', '208'), #dup source match, default
    ('109', 'ag', '109', '209'), #dup source match, dup desc
    ('110', 'ag', '110', '209'),
    ('111', 'ah', '111', '210'), #dup source match
    ('112', 'ai', '112', '211'), #dup source match, vmp same desc
  ], ['ref_id', 'ref_description', 'APID', 'VPID'])
  
  df_vmp = spark.createDataFrame([
    ('200', 'va'), #default
    ('201', 'vb'), #same amp desc
    ('202', 'vc'),
    ('203', 'vd'),
    ('204', 'ac'), #same amp desc
    ('205', 'ad'), #same amp desc, same vmp desc
    ('206', 'ad'),
    ('207', 've'), #no src match
    ('208', 'vf'), #dup source match, default
    ('209', 'vg'), #dup source match, dup amp desc
    ('210', 'vh'), #dup source match
    ('211', 'ai'), #dup source match, same amp desc
    ('300', 'vaa'), #no amp match
  ], ['ref_id', 'ref_description'])
  
  df_expected = spark.createDataFrame([
    ('0', 'a', ' ', 'a', '400', 'APID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)),
    ('1', 'b', ' ', 'b', '401', 'VPID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)),
    ('2', 'c', ' ', 'c', '100', 'APID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)),
    ('3', 'd', ' ', 'd', '202', 'VPID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)),
    ('4', 'e', ' ', 'e', '204', 'VPID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)),
    ('5', 'f', ' ', 'f', '205', 'VPID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)),
    ('6', 'g', ' ', 'g', '108', 'APID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)),
    ('7', 'h', ' ', 'h', '108', 'APID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)),
    ('8', 'i', ' ', 'i', '209', 'VPID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)),
    ('9', 'j', ' ', 'j', '209', 'VPID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)),
    ('10', 'k', ' ', 'k', '111', 'APID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)),
    ('11', 'l', ' ', 'l', '111', 'APID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)),
    ('12', 'm', ' ', 'm', '211', 'VPID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)),
    ('13', 'n', ' ', 'n', '211', 'VPID', 'fuzzy_step', datetime(2021, 1, 2, 3, 4, 5, 6)),
  ], ['id', 'original_text', 'form_in_text', 'text', 'match_id', 'id_level', 'match_level', 'match_datetime'])
  
  class MockRefDataStore():
    amp = df_amp
    vmp = df_vmp
    ID_COL = 'ref_id'
    TEXT_COL = 'ref_description' 
  
  df_actual = map_amp_to_vmp_if_there_are_amp_desc_duplicates_or_matching_vmp_desc(df_input, 
                                                                                   MockRefDataStore(),
                                                                                   id_col='id',
                                                                                   original_text_col='original_text',
                                                                                   form_in_text_col='form_in_text',
                                                                                   text_col='text',
                                                                                   id_level_col='id_level',
                                                                                   match_level_col='match_level',
                                                                                   match_datetime_col='match_datetime',
                                                                                   match_id_col='match_id')
  
  assert compare_results(df_actual, df_expected, join_columns=['id'])

# COMMAND ----------

@suite.add_test
def test_add_match_term():
  df_input = spark.createDataFrame([
    ('0', 'fake_original', ' ', 'benefix factor ix coagulation', '415245003', 'VTMID', 'fake_match_level', 'fake_timestamp'),
    ('1', 'fake_original', ' ', 'cefixime 100 mg in 5ml suspension', '36134411000001101', 'VPID', 'fake_match_level', 'fake_timestamp'),
    ('2', 'fake_original', ' ', 'sumatriptan 50 mg tablets', '10310711000001105', 'APID', 'fake_match_level', 'fake_timestamp'),
  ], ['epma_id', 'original_epma_description', 'form_in_text', 'epma_description', 'match_id', 'id_level', 'match_level', 'match_datetime'])
  
  df_amp = spark.createDataFrame([
    ('10310711000001105', 'Sumatriptan 50mg tablets')
  ], ['ref_id', 'ref_description'])

  df_vmp = spark.createDataFrame([
    ('36134411000001101', 'Cefixime 100mg/5ml oral suspension')
  ], ['ref_id', 'ref_description'])

  df_vtm = spark.createDataFrame([
    ('415245003', 'Nonacog alfa')
  ], ['ref_id', 'ref_description'])
  
  df_expected = spark.createDataFrame([
    ('0', 'fake_original', ' ', 'benefix factor ix coagulation', '415245003', 'Nonacog alfa', 'VTMID', 'fake_match_level', 'fake_timestamp'),
    ('1', 'fake_original', ' ', 'cefixime 100 mg in 5ml suspension', '36134411000001101', 'Cefixime 100mg/5ml oral suspension', 'VPID', 'fake_match_level', 'fake_timestamp'),
    ('2', 'fake_original', ' ', 'sumatriptan 50 mg tablets', '10310711000001105', 'Sumatriptan 50mg tablets', 'APID', 'fake_match_level', 'fake_timestamp'),
  ], ['epma_id', 'original_epma_description', 'form_in_text', 'epma_description', 'match_id', 'match_term', 'id_level', 'match_level', 'match_datetime'])
  
  class MockRefDataStore():
    amp = df_amp
    vmp = df_vmp
    vtm = df_vtm
    ID_COL = 'ref_id'
    TEXT_COL = 'ref_description'

  df_actual = add_match_term(df_input,
                             MockRefDataStore(),
                             id_col='epma_id',
                             original_text_col='original_epma_description',
                             form_in_text_col='form_in_text',
                             text_col='epma_description', 
                             match_id_col='match_id',
                             match_term_col='match_term',
                             id_level_col='id_level',
                             match_level_col='match_level',
                             match_datetime_col='match_datetime')
  
  assert compare_results(df_expected, df_actual, join_columns=['epma_id'])

# COMMAND ----------

@suite.add_test
def test_moiety_sort_ratio_fuzzy_match_step():

  df_input = spark.createDataFrame([
    (1, 'Creon 25,000', ' ', 'creon 25,000'),
    (2, 'adrenaline', ' ', 'adrenaline'),
    (3, 'Uniphyllin Continus', ' ', 'uniphyllin continus'),
    (4, 'Pabrinex Intravenous High Potency', ' ', 'pabrinex intravenous high potency'),
    (5, 'Umeclidinium dry powder inhaler', ' ', 'umeclidinium dry powder inhaler'),
    (6, 'suppository', ' ', 'suppository')
  ], ['epma_id', 'original_epma_description', 'form_in_text', 'epma_description'])

  timestamp = datetime(2020, 5, 6, 1, 1, 1)

  df_mappable_expected = spark.createDataFrame([
    (1, 'Creon 25,000', ' ', 'creon 25,000', '12236006', 'VTMID', 'fuzzy_moiety_vtm', timestamp),
    (2, 'adrenaline', ' ', 'adrenaline', '65502005', 'VTMID', 'fuzzy_moiety_vtm', timestamp),
    (3, 'Uniphyllin Continus', ' ', 'uniphyllin continus', '66493003', 'VTMID', 'fuzzy_moiety_vtm', timestamp)
  ], ['epma_id', 'original_epma_description', 'form_in_text', 'epma_description', 'match_id', 'id_level', 'match_level', 'match_datetime'])

  df_unmappable_expected = spark.createDataFrame([
    ('Pabrinex Intravenous High Potency', ' ', 'fuzzy_moiety_no_unique_vtm', timestamp),
    ('Umeclidinium dry powder inhaler', ' ', 'fuzzy_moiety_low_score', timestamp),
    ('suppository', ' ', 'fuzzy_moiety_no_moieties', timestamp)
  ], ['original_epma_description', 'form_in_text', 'reason', 'match_datetime'])

  parsed_schema = StructType([
   StructField('ref_id', StringType(), True),
   StructField('ref_description', StringType(), True),
   StructField('MOIETY', StringType(), True),
   StructField('MOIETY_2', StringType(), True),
   StructField('MOIETY_3', StringType(), True),
   StructField('MOIETY_4', StringType(), True),
   StructField('MOIETY_5', StringType(), True),
  ])

  df_amp_dedup = spark.createDataFrame([
    ('13857211000001106', 'Creon 10000 gastro-resistant capsules', '13857211000001106', '3437011000001107', 'APID'),
    ('454311000001109', 'Uniphyllin Continus 200mg tablets', '454311000001109', '35917711000001108', 'APID'),
    ('8288311000001104', 'Pabrinex Intramuscular High Potency solution for injection 5ml and 2ml ampoules', '8288311000001104', '8307411000001101', 'APID')
  ], 
    ['ref_id', 'ref_description', 'APID', 'VPID', 'id_level'])

  df_amp_parsed = spark.createDataFrame([
    ('13857211000001106', 'Creon 10000 gastro-resistant capsules', 'Creon 10000', None, None, None, None),
    ('454311000001109', 'Uniphyllin Continus 200mg tablets', 'Uniphyllin Continus', None, None, None, None),
  ], parsed_schema)

  df_vmp = spark.createDataFrame([
    ('3437011000001107', 'Generic Creon 10000 gastro-resistant capsules', '12236006', 'VPID'),
    ('35917711000001108', 'Theophylline 200mg modified-release tablets', '66493003', 'VPID'),
    ('8307411000001101', 'Vitamins B and C high potency intramuscular solution for injection 5ml and 2ml ampoules', None, 'VPID'),
    ('21544511000001106', 'Adrenaline (base) 0.01% eye drops preservative free', '65502005', 'VPID'),
    ('27890611000001109', 'Umeclidinium bromide 65micrograms/dose dry powder inhaler', '27896111000001103', 'VPID')
  ], 
    ['ref_id', 'ref_description', 'VTMID', 'id_level'])

  df_vmp_parsed = spark.createDataFrame([
    ('35917711000001108', 'Theophylline 200mg modified-release tablets', 'Theophylline', None, None, None, None),
    ('21544511000001106', 'Adrenaline (base) 0.01% eye drops preservative free', 'Adrenaline (base)', None, None, None, None),
    ('27890611000001109', 'Umeclidinium bromide 65micrograms/dose dry powder inhaler', 'Umeclidinium bromide', None, None, None, None),
  ], parsed_schema)

  df_vtm = spark.createDataFrame([
    ('12236006', 'Pancreatin', 'VTMID'),
    ('66493003', 'Theophylline', 'VTMID'),
    ('65502005', 'Adrenaline', 'VTMID'),
    ('27896111000001103', 'Umeclidinium bromide', 'VTMID')
  ], 
    ['ref_id', 'ref_description', 'id_level'])

  class MockRefDataStore():
    amp_dedup = df_amp_dedup
    amp_parsed = df_amp_parsed
    vmp = df_vmp 
    vmp_parsed = df_vmp_parsed
    vtm = df_vtm
    ID_COL = 'ref_id'
    TEXT_COL = 'ref_description'
    ID_LEVEL_COL = 'id_level'

  class MockDFCheckpoint():
    pass
    
  def mock_get_non_moiety_words():
    return ['intravenous', 'dry', 'inhaler', 'suppository', 'tablets', 'free', 'eye', 'drops', 'powder', 'capsules'] 

  def mock_get_moieties_only_from_amp_vmp_ref_data(ref_data_store: ReferenceDataFormatter, non_moiety_words, **kwargs):
    df_output = spark.createDataFrame([
      ('13857211000001106', 'Creon 10000 gastro-resistant capsules', 'APID', 'Creon'),
      ('454311000001109', 'Uniphyllin Continus 200mg tablets', 'APID', 'Uniphyllin Continus'),
      ('35917711000001108', 'Theophylline 200mg modified-release tablets', 'VPID', 'Theophylline'),
      ('21544511000001106', 'Adrenaline (base) 0.01% eye drops preservative free', 'VPID', 'Adrenaline (base)'),
      ('27890611000001109', 'Umeclidinium bromide 65micrograms/dose dry powder inhaler', 'VPID', 'Umeclidinium bromide'),
      ('8307411000001101', 'Vitamins B and C high potency intramuscular solution for injection 5ml and 2ml ampoules', 'VPID', 'vitamins b c high potency'),
      ('8288311000001104', 'Pabrinex Intramuscular High Potency solution for injection 5ml and 2ml ampoules', 'APID', 'pabrinex high potency'),
      ('3437011000001107', 'Generic Creon 10000 gastro-resistant capsules', 'VPID', 'generic creonresistant')
    ], ['ref_id', 'ref_description', 'id_level', '_moiety'])
    return df_output

  def mock_map_moiety_ref_data_to_vtm(df_input, ref_data_store: ReferenceDataFormatter, **kwargs):
    df_input_expected = spark.createDataFrame([
      ('13857211000001106', 'Creon 10000 gastro-resistant capsules', 'APID', 'Creon'),
      ('454311000001109', 'Uniphyllin Continus 200mg tablets', 'APID', 'Uniphyllin Continus'),
      ('35917711000001108', 'Theophylline 200mg modified-release tablets', 'VPID', 'Theophylline'),
      ('21544511000001106', 'Adrenaline (base) 0.01% eye drops preservative free', 'VPID', 'Adrenaline (base)'),
      ('27890611000001109', 'Umeclidinium bromide 65micrograms/dose dry powder inhaler', 'VPID', 'Umeclidinium bromide'),
      ('8307411000001101', 'Vitamins B and C high potency intramuscular solution for injection 5ml and 2ml ampoules', 'VPID', 'vitamins b c high potency'),
      ('8288311000001104', 'Pabrinex Intramuscular High Potency solution for injection 5ml and 2ml ampoules', 'APID', 'pabrinex high potency'),
      ('3437011000001107', 'Generic Creon 10000 gastro-resistant capsules', 'VPID', 'generic creonresistant')
    ], ['ref_id', 'ref_description', 'id_level', '_moiety'])
    assert compare_results(df_input, df_input_expected, join_columns=['ref_id'])

    df_output = spark.createDataFrame([
      ('13857211000001106', 'Creon 10000 gastro-resistant capsules', 'APID', 'Creon', 'Pancreatin', '12236006'),
      ('454311000001109', 'Uniphyllin Continus 200mg tablets', 'APID', 'Uniphyllin Continus', 'Theophylline', '66493003'),
      ('35917711000001108', 'Theophylline 200mg modified-release tablets', 'VPID', 'Theophylline', 'Theophylline', '66493003'),
      ('21544511000001106', 'Adrenaline (base) 0.01% eye drops preservative free', 'VPID', 'Adrenaline (base)', 'Adrenaline', '65502005'),
      ('27890611000001109', 'Umeclidinium bromide 65micrograms/dose dry powder inhaler', 'VPID', 'Umeclidinium bromide', 'Umeclidinium bromide', '27896111000001103'),
      ('8307411000001101', 'Vitamins B and C high potency intramuscular solution for injection 5ml and 2ml ampoules', 'VPID', 'vitamins b c high potency', None, None),
      ('8288311000001104', 'Pabrinex Intramuscular High Potency solution for injection 5ml and 2ml ampoules', 'APID', 'pabrinex high potency', None, None),
      ('3437011000001107', 'Generic Creon 10000 gastro-resistant capsules', 'VPID', 'generic creonresistant', 'Pancreatin', '12236006')
    ], ['ref_id', 'ref_description', 'id_level', '_moiety', 'vtm_ref_description', 'VTMID'])
    return df_output

  def mock_get_records_with_moieties(df_input, non_moiety_words, **kwargs):
    df_input_expected = spark.createDataFrame([
      (1, 'Creon 25,000', ' ', 'creon 25,000'),
      (2, 'adrenaline', ' ', 'adrenaline'),
      (3, 'Uniphyllin Continus', ' ', 'uniphyllin continus'),
      (4, 'Pabrinex Intravenous High Potency', ' ', 'pabrinex intravenous high potency'),
      (5, 'Umeclidinium dry powder inhaler', ' ', 'umeclidinium dry powder inhaler'),
      (6, 'suppository', ' ', 'suppository')
    ], ['epma_id', 'original_epma_description', 'form_in_text', 'epma_description'])
    assert compare_results(df_input, df_input_expected, join_columns=['epma_id'])

    df_output1 = spark.createDataFrame([
      (1, 'Creon 25,000', ' ', 'creon 25,000', 'creon'),
      (2, 'adrenaline', ' ', 'adrenaline', 'adrenaline'),
      (3, 'Uniphyllin Continus', ' ', 'uniphyllin continus', 'uniphyllin continus'),
      (4, 'Pabrinex Intravenous High Potency', ' ', 'pabrinex intravenous high potency', 'pabrinex high potency'),
      (5, 'Umeclidinium dry powder inhaler', ' ', 'umeclidinium dry powder inhaler', 'umeclidinium'),
    ], ['epma_id', 'original_epma_description', 'form_in_text', 'epma_description', '_clean_text'])

    df_output2 = spark.createDataFrame([
      (6, 'suppository', ' ', 'suppository')
    ], ['epma_id', 'original_epma_description', 'form_in_text', 'epma_description'])

    return df_output1, df_output2, MockDFCheckpoint()

  def mock_sort_ratio_fuzzy_match(df_input, df_amp_vmp_ref_data_moieties_only, **kwargs):
    df_input_expected = spark.createDataFrame([
        (1, 'Creon 25,000', ' ', 'creon 25,000', 'creon'),
        (2, 'adrenaline', ' ', 'adrenaline', 'adrenaline'),
        (3, 'Uniphyllin Continus', ' ', 'uniphyllin continus', 'uniphyllin continus'),
        (4, 'Pabrinex Intravenous High Potency', ' ', 'pabrinex intravenous high potency', 'pabrinex high potency'),
        (5, 'Umeclidinium dry powder inhaler', ' ', 'umeclidinium dry powder inhaler', 'umeclidinium'),
      ], ['epma_id', 'original_epma_description', 'form_in_text', 'epma_description', '_clean_text'])
    assert compare_results(df_input, df_input_expected, join_columns=['epma_id'])

    df_output = spark.createDataFrame([
      (1, 'Creon 25,000', ' ', 'creon 25,000', 'creon', 'Creon', '100'),
      (2, 'adrenaline', ' ', 'adrenaline', 'adrenaline', 'Adrenaline (base)', '100'),
      (3, 'Uniphyllin Continus', ' ', 'uniphyllin continus', 'uniphyllin continus', 'Uniphyllin Continus', '100'),
      (4, 'Pabrinex Intravenous High Potency', ' ', 'pabrinex intravenous high potency', 'pabrinex high potency', 'pabrinex high potency', '100'),
      (5, 'Umeclidinium dry powder inhaler', ' ', 'umeclidinium dry powder inhaler', 'umeclidinium', 'Umeclidinium bromide', '75')
    ], ['epma_id', 'original_epma_description', 'form_in_text', 'epma_description', '_clean_text', '_moiety', '_confidence_score'])
    return df_output

  def mock_split_by_unique_vtm(df_input, df_moiety_ref_data_with_vtm, **kwargs):
    df_input_expected = spark.createDataFrame([
      (1, 'Creon 25,000', ' ', 'creon 25,000', 'creon', 'Creon', '100'),
      (2, 'adrenaline', ' ', 'adrenaline', 'adrenaline', 'Adrenaline (base)', '100'),
      (3, 'Uniphyllin Continus', ' ', 'uniphyllin continus', 'uniphyllin continus', 'Uniphyllin Continus', '100'),
      (4, 'Pabrinex Intravenous High Potency', ' ', 'pabrinex intravenous high potency', 'pabrinex high potency', 'pabrinex high potency', '100'),
      (5, 'Umeclidinium dry powder inhaler', ' ', 'umeclidinium dry powder inhaler', 'umeclidinium', 'Umeclidinium bromide', '75')
    ], ['epma_id', 'original_epma_description', 'form_in_text', 'epma_description', '_clean_text', '_moiety', '_confidence_score'])
    assert compare_results(df_input, df_input_expected, join_columns=['epma_id'])

    df_output1 = DFCheckpoint(spark.createDataFrame([
      ('Creon', 1, 'Creon 25,000', ' ', 'creon 25,000', 'creon', '100', '13857211000001106', \
       'Creon 10000 gastro-resistant capsules', 'pancreatin', '12236006', 'VTMID'),
      ('Adrenaline (base)', 2, 'adrenaline', ' ', 'adrenaline', 'adrenaline', '100', '21544511000001106', \
       'Adrenaline (base) 0.01% eye drops preservative free', 'adrenaline', '65502005', 'VTMID'),
      ('Uniphyllin Continus', 3, 'Uniphyllin Continus', ' ', 'uniphyllin continus', 'uniphyllin continus', '100', '454311000001109', \
       'Uniphyllin Continus 200mg tablets', 'theophylline', '66493003', 'VTMID'),
      ('Umeclidinium bromide', 5, 'Umeclidinium dry powder inhaler', ' ', 'umeclidinium dry powder inhaler', 'umeclidinium', '75', '27890611000001109', \
       'Umeclidinium bromide 65micrograms/dose dry powder inhaler', 'umeclidinium bromide', '27896111000001103', 'VTMID')
    ], ['_moiety', 'epma_id', 'original_epma_description', 'form_in_text', 'epma_description', '_clean_text', '_confidence_score', 'ref_id', 'ref_description', 'vtm_ref_description', 'VTMID', 'id_level']))

    df_output2 = spark.createDataFrame([
      ('pabrinex high potency', 4, 'Pabrinex Intravenous High Potency', ' ', 'pabrinex intravenous high potency', 'pabrinex high potency', 
       '100', '8288311000001104', 'pabrinex intramuscular high potency solution for injection 5ml and 2ml ampoules', 'null')
    ], ['_moiety', 'epma_id', 'original_epma_description', 'form_in_text', 'epma_description', '_clean_text', '_confidence_score', 'ref_id', 'ref_description', 'VTMID'])
    return df_output1, df_output2, MockDFCheckpoint()

  with FunctionPatch('get_non_moiety_words', mock_get_non_moiety_words):
    with FunctionPatch('get_moieties_only_from_amp_vmp_ref_data', mock_get_moieties_only_from_amp_vmp_ref_data):
      with FunctionPatch('map_moiety_ref_data_to_vtm', mock_map_moiety_ref_data_to_vtm):
        with FunctionPatch('get_records_with_moieties', mock_get_records_with_moieties):
          with FunctionPatch('sort_ratio_fuzzy_match', mock_sort_ratio_fuzzy_match):
            with FunctionPatch('split_by_unique_vtm', mock_split_by_unique_vtm):
              
              step4_output = moiety_sort_ratio_fuzzy_match_step(df_input,
                                                               MockRefDataStore(),
                                                               confidence_threshold=90,
                                                               original_text_col='original_epma_description',
                                                               form_in_text_col='form_in_text',
                                                               text_col='epma_description',
                                                               id_col='epma_id',
                                                               id_level_col='id_level',
                                                               match_level_col='match_level',
                                                               match_datetime_col='match_datetime',
                                                               match_id_col='match_id',
                                                               reason_col='reason',
                                                               timestamp=timestamp)
              
              assert compare_results(df_mappable_expected, step4_output.df_mappable.df, join_columns=['original_epma_description'])
              assert compare_results(df_unmappable_expected, step4_output.df_unmappable.df, join_columns=['original_epma_description'])
              step4_output.df_mappable.delete()
              step4_output.df_unmappable.delete()  

# COMMAND ----------

@suite.add_test
def test_select_best_match_from_scored_records():  

  df_records_to_score = spark.createDataFrame(
  [ 
    ('9', 'tacrolimus 0.03 % ointment (30g)', 'tacrolimus 0.03 % ointment (30g)', 'tacrolimus 0.03 % ointment', 'VPID', '407889001', 'ointment'),
    ('101', 'ESOMEPRAZOLE (Unlicensed Palliative use only) 40mg Injection', 'esomeprazole unlicensed palliative use only 40mg injection', 'esomeprazole 40mg powder for solution for injection vials', 'VPID', 'v17631411000001100', 'power'),
    ('101', 'ESOMEPRAZOLE (Unlicensed Palliative use only) 40mg Injection', 'esomeprazole unlicensed palliative use only 40mg injection', 'esomeprazole 40mg powder for solution for injection vials', 'VPID', 'v317334001', 'power'),
    ('101', 'ESOMEPRAZOLE (Unlicensed Palliative use only) 40mg Injection', 'esomeprazole unlicensed palliative use only 40mg injection', 'esomeprazole 40mg powder for solution for injection vials', 'APID', 'a20372111000001102', 'power'),
    ('101', 'ESOMEPRAZOLE (Unlicensed Palliative use only) 40mg Injection', 'esomeprazole unlicensed palliative use only 40mg injection', 'esomeprazole 40mg powder for solution for injection vials', 'APID', 'a34752611000001109', 'power')
  ],
  ['epma_id', 'original_epma_description', 'epma_description', 'match_term', 'match_level', 'match_id', 'form_in_text']
  )


  df_match_table_expected = spark.createDataFrame([
    ('9', 'tacrolimus 0.03 % ointment (30g)', 'ointment', 'tacrolimus 0.03 % ointment (30g)', '109129008', 'VTMID', 'tacrolimus 0.03 % ointment', 'fuzzy_linked', 95, False,
     'tacrolimus', 'tacrolimus', 'Strength mismatch so map to VTMID'),
  ], ['epma_id', 'original_epma_description', 'form_in_text', 'epma_description', 'match_id', 'id_level', 'match_term', 'match_level', 'confidence_score', 'has_strength', 'NM_VTMID',
      'match_term_debug', 'match_reason_debug'])
  
  def mock_calculate_max_confidence_score(df_records_to_score, **kwargs):
    return spark.createDataFrame(
      [
        ('9', 'tacrolimus 0.03 % ointment (30g)', 'tacrolimus 0.03 % ointment (30g)', 'tacrolimus 0.03 % ointment', 'VPID', '407889001', 'ointment', 95, 95),
      ('101', 'ESOMEPRAZOLE (Unlicensed Palliative use only) 40mg Injection', 'esomeprazole unlicensed palliative use only 40mg injection', 'esomeprazole 40mg powder for solution for injection vials', 'VPID', 'v17631411000001100', 'powder', 65, 65),
      ('101', 'ESOMEPRAZOLE (Unlicensed Palliative use only) 40mg Injection', 'esomeprazole unlicensed palliative use only 40mg injection', 'esomeprazole 40mg powder for solution for injection vials', 'VPID', 'v317334001', 'powder', 65, 65),
      ('101', 'ESOMEPRAZOLE (Unlicensed Palliative use only) 40mg Injection', 'esomeprazole unlicensed palliative use only 40mg injection', 'esomeprazole 40mg powder for solution for injection vials', 'APID', 'a20372111000001102', 'powder', 65, 65),
      ('101', 'ESOMEPRAZOLE (Unlicensed Palliative use only) 40mg Injection', 'esomeprazole unlicensed palliative use only 40mg injection', 'esomeprazole 40mg powder for solution for injection vials', 'APID', 'a34752611000001109', 'powder', 65, 65)
      ], 
      ['epma_id', 'original_epma_description', 'epma_description', 'match_term', 'match_level', 'match_id', 'form_in_text', 'confidence_score', 'max_confidence'])

  def mock_APID_match_candidates(df_records_with_confidence_score, RefDataStore, **kwargs):
    schema_apid_matches = StructType([StructField('epma_id', StringType()), 
                                    StructField('original_epma_description', StringType()), 
                                    StructField('form_in_text', StringType()),  
                                    StructField('epma_description', StringType()),
                                    StructField('ref_data_id', StringType()), 
                                    StructField('id_level', StringType()), 
                                    StructField('match_term', StringType()),
                                    StructField('confidence_score', LongType()),
                                    StructField('VTMID', StringType()), 
                                    StructField('reason', StringType())
                                   ])
    return spark.createDataFrame([], schema_apid_matches)

  def mock_VPID_match_candidates(VPID_records_to_match, RefDataStore, **kwargs):
    return spark.createDataFrame(
      [
        ('9', 'tacrolimus 0.03 % ointment (30g)', 'ointment', 'tacrolimus 0.03 % ointment (30g)', '407889001', 'VPID', 'tacrolimus 0.03 % ointment', 95, '109129008', 'unique max confidence VPID')
      ], 
      ['epma_id', 'original_epma_description', 'form_in_text', 'epma_description', 'ref_data_id', 'id_level', 'match_term', 'confidence_score', 'VTMID', 'reason'])

  with FunctionPatch('calculate_max_confidence_score', mock_calculate_max_confidence_score):
    with FunctionPatch('APID_match_candidates', mock_APID_match_candidates):
      with FunctionPatch('VPID_match_candidates', mock_VPID_match_candidates):
        df_match_table, df_non_match_table = select_best_match_from_scored_records(df_records_to_score,
                                                                                   RefDataStore,
                                                                                   confidence_score_col='confidence_score',
                                                                                   original_text_col='original_epma_description',
                                                                                   form_in_text_col='form_in_text',
                                                                                   text_col='epma_description',
                                                                                   match_term_col='match_term',
                                                                                   reason_col='reason',
                                                                                   id_col='epma_id', 
                                                                                   id_level_col='id_level',
                                                                                   match_id_col='match_id',
                                                                                   match_datetime_col='match_datetime',
                                                                                   match_level_col='match_level',
                                                                                   has_strength_col='has_strength')
        assert compare_results(df_match_table_expected, df_match_table.df, join_columns=['epma_id'])
        assert df_non_match_table.df.count() == 1  
        del df_match_table
        del df_non_match_table

# COMMAND ----------

@suite.add_test
def test_wratio_lite():
  result1 = wratio_lite('calogen extra shots neutral', 'calogen extra shots emulsion neutral')
  result2 = wratio_lite('calogen extra neutral shots', 'calogen extra shots neutral')
  result3 = wratio_lite('calgen extra neutral shots', 'calogen extra neutral shots')
  
  assert result1 == 95
  assert result2 == 95
  assert result3 == 98

# COMMAND ----------

@suite.add_test
def test_extract_one_wratio_lite():
  query = 'calgen extra shots neutral'
  choices = ['calogen extra shots emulsion neutral',
             'calogen extra shots neutral',
             'paracemtemol 500mg']
  result = extract_one_wratio_lite(query, choices)
  
  assert result == ('calogen extra shots neutral', 98)

# COMMAND ----------

@suite.add_test
def test_extract_one_wratio_lite_empty():
  query = 'calgen extra shots neutral'
  choices = []
  result = extract_one_wratio_lite(query, choices)
  
  assert result == None

# COMMAND ----------

suite.run()