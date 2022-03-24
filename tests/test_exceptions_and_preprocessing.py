# Databricks notebook source
dbutils.widgets.text('db', 'test_epma_autocoding', 'Test Database.')
DB = dbutils.widgets.get('db')
assert DB

# COMMAND ----------

# MAGIC %run ../notebooks/_modules/epma_global/function_test_suite

# COMMAND ----------

# MAGIC %run ../notebooks/_modules/epma_global/test_helpers

# COMMAND ----------

# MAGIC %run ../notebooks/0_exceptions_and_preprocessing/functions/exceptions_and_preprocessing_functions

# COMMAND ----------

import re
from datetime import datetime

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import Row
from pyspark.sql.functions import col

# COMMAND ----------

suite = FunctionTestSuite()

# COMMAND ----------

@suite.add_test
def test_drop_null_in_medication_col_pass():

  df_input = spark.createDataFrame([
      ('0', 'luticasone-salmeterol 500 mcg-50 mcg/inh inhalation powder'),
      ('1', 'Trelegy Ellipta 92 mcg-55 mcg-22 mcg/inh inhalation powder'),
      ('2', None)
    ], ['id', 'Drug'])

  df_output = drop_null_in_medication_col(df_input, 'Drug')

  df_expected = spark.createDataFrame([
      ('0', 'luticasone-salmeterol 500 mcg-50 mcg/inh inhalation powder'),
      ('1', 'Trelegy Ellipta 92 mcg-55 mcg-22 mcg/inh inhalation powder')
    ], ['id', 'Drug'])

  assert compare_results(df_output, df_expected, join_columns=['id'])

# COMMAND ----------

@suite.add_test
def test_drop_null_in_medication_col_fail():

  df_input = spark.createDataFrame([
    ('0', 'luticasone-salmeterol 500 mcg-50 mcg/inh inhalation powder'),
    ('1', None),
    ('2', None)
  ], ['id', 'Drug'])

  df_expected = spark.createDataFrame([
      ('0', 'luticasone-salmeterol 500 mcg-50 mcg/inh inhalation powder')
    ], ['id', 'Drug'])

  with warnings.catch_warnings(record = True) as w:
    df_output = drop_null_in_medication_col(df_input, 'Drug')
    assert len(w) == 1
    assert issubclass(w[-1].category, RuntimeWarning)
    assert compare_results(df_output, df_expected, join_columns=['id'])

# COMMAND ----------

@suite.add_test
def test_filter_user_curated_unmappables() -> bool:

  df_input = spark.createDataFrame([
    (0, 'luticasone-salmeterol 500 mcg-50 mcg/inh inhalation powder'),
    (1, 'Trelegy Ellipta 92 mcg-55 mcg-22 mcg/inh inhalation powder'),
    (2, 'heparinoid-salicylic acid 0.2%-2% topical cream'),
    (3, 'Vitamin svrthv78 oil'),
    (4, 'vitamin svrthv78 blah-oil'),
    (5, None),
    (6, ''),
  ], ['index', 'text'])
  
  df_expected_remaining = spark.createDataFrame([
    (1, 'Trelegy Ellipta 92 mcg-55 mcg-22 mcg/inh inhalation powder'),
    (3, 'Vitamin svrthv78 oil'),
    (5, None),
    (6, ''),
  ], ['index', 'text'])

  df_expected_unmappable = spark.createDataFrame([
    (0, 'luticasone-salmeterol 500 mcg-50 mcg/inh inhalation powder'),
    (2, 'heparinoid-salicylic acid 0.2%-2% topical cream'),
    (4, 'vitamin svrthv78 blah-oil'),
  ], ['index', 'text'])

  df_actual_remaining, df_actual_unmappable = filter_user_curated_unmappables(
    df_input, text_col='text', unmappable_regexes=['sal.....ol', '[0-9].?[0-9]?%-[0-9].?[0-9]?'])

  assert compare_results(df_actual_remaining, df_expected_remaining, join_columns=['index'])
  assert compare_results(df_actual_unmappable, df_expected_unmappable, join_columns=['index'])

# COMMAND ----------

@suite.add_test
def test_select_record_batch_to_process_no_batch() -> bool:
  df_input = spark.createDataFrame([
    (0, 'a'),
    (1, 'b'),
    (2, 'c'),
    (3, 'd'),
    (4, 'e'),
  ], ['index', 'name'])
  
  df_match_lookup_final = spark.createDataFrame([
    (2,),
    (4,),
  ], ['index'])
  
  df_input_unmappables = spark.createDataFrame([
    (3,),
  ], ['index'])
  
  df_expected = spark.createDataFrame([
    (0, 'a'),
    (1, 'b'),
  ], ['index', 'name'])
  
  df_cp_actual = select_record_batch_to_process(df_input, df_match_lookup_final, df_input_unmappables, 
                                                batch_size=df_input.count(), join_cols=['index'])
  
  assert compare_results(df_cp_actual.df, df_expected, join_columns=['index'])


@suite.add_test
def test_select_record_batch_to_process_with_batch() -> bool:
  df_input = spark.createDataFrame([
    (0, 'a'),
    (1, 'b'),
    (2, 'c'),
    (3, 'd'),
    (4, 'e'),
  ], ['index', 'name'])

  df_match_lookup_final = spark.createDataFrame([
    (2,),
    (4,),
  ], ['index'])

  df_unmappable = spark.createDataFrame([
    (3,)
  ], ['index'])

  df_cp_actual = select_record_batch_to_process(df_input, df_match_lookup_final, df_unmappable, batch_size=2, join_cols=['index'])

  assert df_cp_actual.df.count() == 2
  
@suite.add_test
def test_select_record_batch_to_process_on_multiple_cols() -> bool:
  df_input = spark.createDataFrame([
    (0, 'a', 'a'),
    (1, 'a', 'b'),
    (2, 'c', 'c'),
    (3, 'd', 'd'),
    (4, 'e', 'e'),
  ], ['index', 'name1', 'name2'])
  
  df_match_lookup_final = spark.createDataFrame([
    (0, 'a', 'a'),
    (4, 'e', 'e'),
  ], ['index', 'name1', 'name2'])
  
  df_input_unmappables = spark.createDataFrame([
    (3, 'd', 'd'),
  ], ['index', 'name1', 'name2'])
  
  df_expected = spark.createDataFrame([
    (1, 'a', 'b'),
    (2, 'c', 'c'),
  ], ['index', 'name1', 'name2'])
  
  df_cp_actual = select_record_batch_to_process(df_input, df_match_lookup_final, df_input_unmappables, 
                                                batch_size=df_input.count(), join_cols=['name1', 'name2'])
  
  assert compare_results(df_cp_actual.df.select('index', 'name1', 'name2'), df_expected, join_columns=['index'])

# COMMAND ----------

@suite.add_test
def test_select_record_batch_to_process_on_multiple_cols_with_nulls() -> bool:
  df_input = spark.createDataFrame([
    (0, 'a', 'a'),
    (1, 'a', 'b'),
    (2, 'c', 'c'),
    (3, 'd', 'd'),
    (4, 'e', None),
  ], ['index', 'name1', 'name2'])
  
  df_match_lookup_final = spark.createDataFrame([
    (0, 'a', 'a'),
    (4, 'e', None),
  ], ['index', 'name1', 'name2'])
  
  df_input_unmappables = spark.createDataFrame([
    (3, 'd', 'd'),
  ], ['index', 'name1', 'name2'])
  
  df_expected = spark.createDataFrame([
    (1, 'a', 'b'),
    (2, 'c', 'c'),
  ], ['index', 'name1', 'name2'])
  
  df_cp_actual = select_record_batch_to_process(df_input, df_match_lookup_final, df_input_unmappables, 
                                                batch_size=df_input.count(), join_cols=['name1', 'name2'])
  
  assert compare_results(df_cp_actual.df.select('index', 'name1', 'name2'), df_expected, join_columns=['index'])

# COMMAND ----------

@suite.add_test
def test_select_distinct_descriptions() -> bool:
  input_schema = StructType([
    StructField("id", StringType(), True),
    StructField("Drug", StringType(), True),
    StructField("form_in_text", StringType(), True),
  ])
  
  df_input_epmawspc2 = spark.createDataFrame([
    Row("1", "vitamin c", " "),
    Row("5", "Freetext medication", " "),
    Row("123", "Butrenoside", " "),
    Row("12133", "butrenoside", " "),
    Row("2", "paracetamol", " "),
    Row("73423", "Rovespatrol", " "),
    Row("1204000", "Oxygen 100ml canister", " "),
  ], input_schema)
  
  df_actual = select_distinct_descriptions(df_input_epmawspc2, src_medication_col='Drug', original_text_col='original_epma_description', form_in_text_col='form_in_text', id_col='epma_id')
  
  expected_schema = StructType([
    StructField('original_epma_description', StringType(), True),
  ])
  
  df_expected = spark.createDataFrame([
    Row("vitamin c"),
    Row("freetext medication"),
    Row("butrenoside"),
    Row("paracetamol"),
    Row("rovespatrol"),
    Row("oxygen 100ml canister"),
  ], expected_schema)
  
  assert compare_results(df_actual.select('original_epma_description'), df_expected, join_columns=['original_epma_description'])

# COMMAND ----------

@suite.add_test
def test_standardise_doseform():
  input_df = spark.createDataFrame([
     ('capsule (gastro-resistant)', '1'),
     ('capsule (modified release)', '2'),
     ('tablet (chewable)', '3'),
     ('tablet (gastro-resistant)', '4'),
     ('tablet (soluble)', '5'),
     ('capsule(gastro-resistant)', '6'),
     ('capsule(modified release)', '7'),
     ('tablet(chewable)', '8'),
     ('tablet(gastro-resistant)', '9'),
     ('tablet(soluble)', '10'),
     (None, '11'), 
     ('', '12'),
     ('something capsule (gastro-resistant)', '13'),
     ('capsule(modified release) something', '14'),
     ('tablert (soluble)', '15'),
     ('tablet  (chewable)', '16'),
     ('sodium chloride nebuliser solution', '17'),
     ('medication mr capsules', '18')
   ], [
     'medication_name_value', 'id'
   ])

  expected_df = spark.createDataFrame([
     ('gastro-resistant tablet', '1'),
     ('modified-release capsule', '2'),
     ('chewable tablet', '3'),
     ('gastro-resistant tablet', '4'),
     ('soluble tablet', '5'),
     ('gastro-resistant tablet', '6'),
     ('modified-release capsule', '7'),
     ('chewable tablet', '8'),
     ('gastro-resistant tablet', '9'),
     ('soluble tablet', '10'),
     (None, '11'), 
     ('', '12'), 
     ('something gastro-resistant tablet', '13'),
     ('modified-release capsule something', '14'),
     ('tablert (soluble)', '15'),
     ('tablet (chewable)', '16'),
     ('sodium chloride nebuliser liquid', '17'),
     ('medication modified-release capsules', '18')
  ], [
     'medication_name_value', 'id'
   ])

  results_df = input_df.withColumn('medication_name_value', standardise_doseform(col('medication_name_value')))

  assert compare_results(results_df, expected_df, join_columns=["id"])

# COMMAND ----------

@suite.add_test
def test_standardise_doseform_replace_in_with_slash():
  input_df = spark.createDataFrame([
     ('latanoprost 50mcg in 1ml eye drops', '1'),
     ('morphine 10 mg in 1ml injection', '2'),
     ('dinoprostone 10 mg in 24 hours pessaries', '3'),
     ('nicotine 7mg in 24hr patches', '4'),
     ('morphine 10 mg in1ml injection', '5'),
     ('morphine 10 in 1ml injection', '6')
   ], [
     'medication_name_value', 'id'
   ])
  
  expected_df = spark.createDataFrame([
     ('latanoprost 50mcg / 1ml eye drops', '1'),
     ('morphine 10 mg / 1ml injection', '2'),
     ('dinoprostone 10 mg in 24 hours pessaries', '3'),
     ('nicotine 7mg in 24hr patches', '4'),
     ('morphine 10 mg in1ml injection', '5'),
     ('morphine 10 in 1ml injection', '6')
  ], [
     'medication_name_value', 'id'
  ])
  
  results_df = input_df.withColumn('medication_name_value', standardise_doseform(col('medication_name_value')))
  
  assert compare_results(results_df, expected_df, join_columns=["id"])

# COMMAND ----------

@suite.add_test
def test_standardise_drug_name():
  input_df = spark.createDataFrame([
     ('adcal d3', '1'),
     ('adcal d3 with stuff', '2'),
     ('stuff withadcal d3', '3'),
     ('betnovate c', '4'),
     ('betnovate calcium', '5'),
     (None, '6'), 
     ('', '7'),
   ], [
     'medication_name_value', 'id'
   ])
  
  expected_df = spark.createDataFrame([
     ('adcal-d3', '1'),
     ('adcal-d3 with stuff', '2'),
     ('stuff withadcal d3', '3'),
     ('betnovate-c', '4'),
     ('betnovate calcium', '5'),
     (None, '6'), 
     ('', '7'), 
  ], [
     'medication_name_value', 'id'
   ])
  
  results_df = input_df.withColumn('medication_name_value', standardise_drug_name(col('medication_name_value')))
  
  assert compare_results(results_df, expected_df, join_columns=["id"])

# COMMAND ----------

@suite.add_test
def test_vitamin_regex_pattern() -> bool:
  
  match_values = [
    'vitamin a 0a',
    'Vitamin a 0a',
    'vitamin ab 0a',
    'vitamin abAb 0a',
    'vitamin a0 0a',
    'vitamin a 0abc',
    'vitamin a oil',
    'vitamin a oil-blah',
    'vitamin a oils',
    'vitamin a 0oil',
    'vitamin a compound',
    'vitamin a compounds',
    'vitamin a 0compound',
    'Vitamin svrthv78 oil',
  ]

  non_matched_values = [
    'vitamina 0a',
    'vitamin 0a',
    'vitamin a00a',
    'vitamin a abc',
    'vitamin a boil',
    'vitamin a (oil)',
    'vitamin a blah-oil',
    'vitamin a flompound',
    'vitamin a (compound)',
    'luticasone-salmeterol 500 mcg-50 mcg/inh inhalation powder',
    'Trelegy Ellipta 92 mcg-55 mcg-22 mcg/inh inhalation powder',
    'heparinoid-salicylic acid 0.2%-2% topical cream',
    'vitamin svrthv78 blah-oil',
    '',
  ]

  for val in match_values:
    assert bool(re.match(vitamin_regex_pattern(), val)) is True, val
    
  for val in non_matched_values:
    assert bool(re.match(vitamin_regex_pattern(), val)) is False, val

# COMMAND ----------

@suite.add_test
def test_replace_hyphens_between_dosages_with_slashes() -> bool:
  df_input = spark.createDataFrame([
    ('0', 'luticasone-salmeterol 500 mcg-50 mcg/inh inhalation powder'),
    ('1', 'Trelegy Ellipta 92 mcg-55 mcg-22 mcg/inh inhalation powder'),
    ('2', 'heparinoid-salicylic acid 0.2%-2% topical cream'),
    ('3', 'Botox'),
    ('4', 'neomycin 2%-0.1%-0.5% ear spray'),
    ('5', '0a-0a'),
    ('6', '01a-0a'),
    ('7', '0 a-0a'),
    ('8', '0  a-0a'),
    ('9', '0abc-0a'),
    ('10', '0%-0a'),
    ('11', '0%%-0a'),
    ('12', '0a -0a'),
    ('13', '0a--0a'),
    ('14', '0a---0a'),
    ('15', '0a-0ab'),
    ('16', '0a-0 a'),
    ('17', '0a-0  a'),
    ('18', '0a-0.a'),
    ('19', '0a-0..a'),
    ('20', '0a-0abc'),
    ('21', '0a-0%'),
    ('22', '0a-0%%'),
    ('23', '0a-0abc%%'),
    ('24', '0a- 0a'),
    ('25', '0a -0a'),
    ('26', '0a - 0a'),
    ('27', '0a-0a.'), #this matches because the extra character on the end is outside the matched string
    ('28', 'first 0a-0a second 01a-0a third 0 a-0a fourth 0  a-0a'),
    ('29', 'Medicine 1 mcg-2 mcg'),
    #add an extra term to each connsecutive test. Note that because of pairing behaviour, any number of terms can be replaced 
    #with only two runs of the regex.
    ('30', 'Medicine 1 mcg-2 mcg-3 mcg'),
    ('31', 'Medicine 1 mcg-2 mcg-3 mcg-4 mcg'),
    ('32', 'Medicine 1 mcg-2 mcg-3 mcg-4 mcg-5 mcg'),
    ('33', 'Medicine 1 mcg-2 mcg-3 mcg-4 mcg-5 mcg-6 mcg'),
    ('34', 'Medicine 1 mcg-2 mcg-3 mcg-4 mcg-5 mcg-6 mcg-7 mcg'),
    ('35', 'Medicine 1 mcg-2 mcg-3 mcg-4 mcg-5 mcg-6 mcg-7 mcg-8 mcg'),
    ('36', 'Medicine 1 mcg-2 mcg-3 mcg-4 mcg-5 mcg-6 mcg-7 mcg-8 mcg-9 mcg'),
    ('37', 'Medicine 1 mcg-2mcg-3 g- 4 abc'),
  ], ['index', 'desc'])
  
  df_expected = spark.createDataFrame([
    ('0','luticasone-salmeterol 500 mcg/50 mcg/inh inhalation powder'),
    ('1', 'Trelegy Ellipta 92 mcg/55 mcg/22 mcg/inh inhalation powder'),
    ('2', 'heparinoid-salicylic acid 0.2%/2% topical cream'),
    ('3', 'Botox'),
    ('4', 'neomycin 2%/0.1%/0.5% ear spray'),
    ('5', '0a/0a'),
    ('6', '01a/0a'),
    ('7', '0 a/0a'),
    ('8', '0  a/0a'),
    ('9', '0abc/0a'),
    ('10', '0%/0a'),
    ('11', '0%%/0a'),
    ('12', '0a/0a'),
    ('13', '0a/0a'),
    ('14', '0a/0a'),
    ('15', '0a/0ab'),
    ('16', '0a/0 a'),
    ('17', '0a/0  a'),
    ('18', '0a/0.a'),
    ('19', '0a/0..a'),
    ('20', '0a/0abc'),
    ('21', '0a/0%'),
    ('22', '0a/0%%'),
    ('23', '0a/0abc%%'),
    ('24', '0a/0a'),
    ('25', '0a/0a'),
    ('26', '0a/0a'),
    ('27', '0a/0a.'),
    ('28', 'first 0a/0a second 01a/0a third 0 a/0a fourth 0  a/0a'),
    ('29', 'Medicine 1 mcg/2 mcg'),
    ('30', 'Medicine 1 mcg/2 mcg/3 mcg'),
    ('31', 'Medicine 1 mcg/2 mcg/3 mcg/4 mcg'),
    ('32', 'Medicine 1 mcg/2 mcg/3 mcg/4 mcg/5 mcg'),
    ('33', 'Medicine 1 mcg/2 mcg/3 mcg/4 mcg/5 mcg/6 mcg'),
    ('34', 'Medicine 1 mcg/2 mcg/3 mcg/4 mcg/5 mcg/6 mcg/7 mcg'),
    ('35', 'Medicine 1 mcg/2 mcg/3 mcg/4 mcg/5 mcg/6 mcg/7 mcg/8 mcg'),
    ('36', 'Medicine 1 mcg/2 mcg/3 mcg/4 mcg/5 mcg/6 mcg/7 mcg/8 mcg/9 mcg'),
    ('37', 'Medicine 1 mcg/2mcg/3 g/4 abc'),
  ], ['index', 'desc'])
  
  df_actual = df_input.withColumn('desc', replace_hyphens_between_dosages_with_slashes(col('desc')))
  
  assert compare_results(df_actual, df_expected, join_columns=['index'])
  

# COMMAND ----------

@suite.add_test
def test_correct_common_unit_errors():

  df_input = spark.createDataFrame([
    ('0', 'units/1ml'),
    ('1', 'medicine units/1ml medicine'),
    ('2', 'medicine mcg/ml medicine'),
    ('3', 'medicine mcg/hr medicine'),
    ('4', 'medicine mcg medicine'),
    ('5', 'medicine modified-release patch medicine'),
    ('6', 'medicine patch'),
    ('7', 'medicine patch  medicine'),
    ('8', 'medicine patch\n medicine'),
    ('9', 'medicine tablet'),
    ('10', 'medicine tablet  medicine'),
    ('11', 'medicine tablet\n medicine'),
    ('12', 'medicine suppository'),
    ('13', 'medicine suppository  medicine'),
    ('14', 'medicine suppository\n medicine'),
    ('15', 'medicine capsule'),
    ('16', 'medicine capsule  medicine'),
    ('17', 'medicine capsule\n medicine'),
    ('18', 'medicine 1000ml medicine'),
    ('19', 'medicine 1000 ml medicine'),
    ('20', 'medicine 1000  ml medicine'),
    ('21', 'medicine 1,000ml medicine'),
    ('22', 'medicine 1,000 ml medicine'),
    ('23', 'medicine 1,000  ml medicine'),
    ('24', 'medicine 2000ml medicine'),
    ('25', 'medicine 2000 ml medicine'),
    ('26', 'medicine 2000  ml medicine'),
    ('27', 'medicine 2,000ml medicine'),
    ('28', 'medicine 2,000 ml medicine'),
    ('29', 'medicine 2,000  ml medicine'),
  ], 
    ['index', 'desc'])

  df_expected = spark.createDataFrame([
    ('0', 'unit/ml'),
    ('1', 'medicine unit/ml medicine'),
    ('2', 'medicine microgram/ml medicine'),
    ('3', 'medicine microgram/hour medicine'),
    ('4', 'medicine microgram medicine'),
    ('5', 'medicine patches medicine'),
    ('6', 'medicine patches '),
    ('7', 'medicine patches  medicine'),
    ('8', 'medicine patches  medicine'),
    ('9', 'medicine tablets '),
    ('10', 'medicine tablets  medicine'),
    ('11', 'medicine tablets  medicine'),
    ('12', 'medicine suppositories '),
    ('13', 'medicine suppositories  medicine'),
    ('14', 'medicine suppositories  medicine'),
    ('15', 'medicine capsules '),
    ('16', 'medicine capsules  medicine'),
    ('17', 'medicine capsules  medicine'),
    ('18', 'medicine 1litre medicine'),
    ('19', 'medicine 1litre medicine'),
    ('20', 'medicine 1litre medicine'),
    ('21', 'medicine 1litre medicine'),
    ('22', 'medicine 1litre medicine'),
    ('23', 'medicine 1litre medicine'),
    ('24', 'medicine 2litre medicine'),
    ('25', 'medicine 2litre medicine'),
    ('26', 'medicine 2litre medicine'),
    ('27', 'medicine 2litre medicine'),
    ('28', 'medicine 2litre medicine'),
    ('29', 'medicine 2litre medicine'),
  ], 
    ['index', 'desc'])

  df_actual = df_input.withColumn('desc', correct_common_unit_errors(col('desc')))

  assert compare_results(df_actual, df_expected, join_columns=['index'])

# COMMAND ----------

suite.run()