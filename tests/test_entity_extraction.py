# Databricks notebook source
# MAGIC %run ../notebooks/_modules/epma_global/functions

# COMMAND ----------

# MAGIC %run ../notebooks/2_entity_extraction/functions/entity_extraction_functions

# COMMAND ----------

# MAGIC %run ../notebooks/_modules/epma_global/function_test_suite

# COMMAND ----------

import warnings

from pyspark.sql.types import StringType, IntegerType, BooleanType, StructType, StructField, Row

# COMMAND ----------

dose_form_list_bc = sc.broadcast(['cream'])

# COMMAND ----------

suite = FunctionTestSuite()

# COMMAND ----------

@suite.add_test
def test_check_for_dose_form():
  test_cases = {
    "powder": True,
    "cReam": True,
    "4mg hydrocortisone": False,
    "50ml ampoule of ketamine": False
  }
  dose_form_desc = ["powder", "balm", "Cream"]
  
  for test_case, expected in test_cases.items():
    assert check_for_dose_form(test_case, dose_form_desc) == expected

# COMMAND ----------

@suite.add_test
def test_extract_strength_unit():
  test_cases = {
    'Furosemide 240 mg in 50ml sodium chloride': FormattedStrengthUnitValues(SVN='240', SVU='mg', SVD='50', SDU='ml'), # known limitation 
    'Furosemide 240 mg in 50ml cream': FormattedStrengthUnitValues(SVN='240', SVU='mg', SVN_2='50', SVU_2='ml'),
    'null': FormattedStrengthUnitValues(),
    'linezolid 600 mg in 300ml (2mg in 1ml) cream': FormattedStrengthUnitValues(SVN='600', SVU='mg', SVN_2='300', SVU_2='ml', SVN_3='2', SVU_3='mg'),
    'dexamethasone/framycetin/gramicidin 12.5%-1%-0.005%': FormattedStrengthUnitValues(SVN='12.5', SVU='%', SVN_2='1', SVU_2='%', SVN_3='0.005', SVU_3='%')
  }
  dose_form_desc = ["powder", "powder", "balm", "Cream"]
  
  for test_case, expected in test_cases.items():
    assert extract_strength_unit(test_case, dose_form_desc) == expected

# COMMAND ----------

@suite.add_test
def test_find_all_strength_unit_pairs():
  test_cases = {
    'dexamethasone/framycetin/gramicidin 12.5%-1%-0.005%': FormattedStrengthUnitValues(SVN='12.5', SVU='%', SVN_2='1', SVU_2='%', SVN_3='0.005', SVU_3='%'),
    'betamethasone-calcipotriol 0.05%-0.005% topical foam': FormattedStrengthUnitValues(SVN='0.05', SVU='%', SVN_2='0.005', SVU_2='%'),
    'dexamethasone/framycetin 0.1% eye': FormattedStrengthUnitValues(SVN='0.1', SVU='%'),
    'null': FormattedStrengthUnitValues(),
    '50ml-1g': FormattedStrengthUnitValues(SVN='50', SVU='ml')
  }
  
  for test_case, expected in test_cases.items():
    assert find_all_strength_unit_pairs(test_case) == expected

# COMMAND ----------

@suite.add_test
def test_find_all_strength_unit_pairs_bad_labels():
  
  with warnings.catch_warnings(record=True) as w:
      result = find_all_strength_unit_pairs(['folic acid 2.5mg', ' in ', '5ml (100micrograms', ' in ', '0.2ml) syrup'])
      assert len(w) == 1
      assert issubclass(w[-1].category, RuntimeWarning)
      assert result == FormattedStrengthUnitValues()

# COMMAND ----------

@suite.add_test
def test_find_strength_unit_pairs_regex():
  test_cases = {
    "dexamethasone/framycetin/gramicidin 12.5%-1%-0.005%": [(' 12.5', '.5', '%'), ('-1', '-1', '%'), ('-0.005', '-0.005', '%')],
    "paracetamol 5mg in cream 7 ml": [(' 5', ' 5', 'mg'), (' 7', ' 7', 'ml')],
    "54 ml": [('54', '54', 'ml')],
    "54": [('54', '54', '')],
  }
  
  for test_case, expected in test_cases.items():
    assert find_strength_unit_pairs_regex(test_case) == expected

# COMMAND ----------

@suite.add_test
def test_split_denominators():
  test_cases = {
    "dexamethasone 1mg +framycetin 0.1 %": ['dexamethasone 1mg ', 'framycetin 0.1 %'], # limitation
    "as/as 5mg/2mg": ['as', '/', 'as 5mg', '/', '2mg'],
    "paracetamol 5mg in cream 7 ml": ['paracetamol 5mg in cream 7 ml'],
    "paracetamol 5mg in radasvoil 7 mg": ['paracetamol 5mg', ' in ', 'radasvoil 7 mg'],
    "null": ['null'],
    "Paracetamol 5ml O/D-3rd": ['Paracetamol 5ml O', '/', 'D-']
  }
  dose_form_desc = ["powder", "powder", "balm", "cream"]
  
  for test_case, expected in test_cases.items():
    assert split_denominators(test_case, dose_form_desc) == expected

# COMMAND ----------

@suite.add_test
def test_remove_special_characters():
  test_cases = {
    "my( test) ": "my test",
    "5mg --promethazine": "5mg promethazine",
    "paracetamol": "paracetamol",
  }
  
  for test_case, expected in test_cases.items():
    assert remove_special_characters(test_case) == expected

# COMMAND ----------

@suite.add_test
def test_three_moieties_separated_by_slashes():
  assert three_moieties_separated_by_slashes("dexamethasone/framycetin/gramicidin 0.05%-0.5%-0.005%")
  assert three_moieties_separated_by_slashes("dexamethasone/framycetin/gramicidin")
  assert not three_moieties_separated_by_slashes("dexamethasone/framycetin /gramicidin 0.05%-0.5%-0.005%")
  assert not three_moieties_separated_by_slashes("dexamethasone/framycetin 0.05%-0.5%")
  assert not three_moieties_separated_by_slashes("")

# COMMAND ----------

@suite.add_test
def test_two_moieties_separated_by_slashes():
  assert two_moieties_separated_by_slashes("dexamethasone/framycetin/gramicidin 0.05%-0.5%-0.005%")
  assert two_moieties_separated_by_slashes("dexamethasone/framycetin 0.05%-0.5%")
  assert two_moieties_separated_by_slashes("dexamethasone/framycetin")
  assert not two_moieties_separated_by_slashes("dexamethasone")
  assert not two_moieties_separated_by_slashes("")

# COMMAND ----------

@suite.add_test
def test_moiety_separated_by_dash():
  assert moiety_separated_by_dash("dexamethasone-framycetin-gramicidin 0.05%-0.5%-0.005%")
  assert moiety_separated_by_dash("dexamethasone-framycetin 0.05%-0.5%")
  assert not moiety_separated_by_dash("dexamethasone framycetin 0.05%-0.5%")
  assert not moiety_separated_by_dash("dexamethasone/framycetin 0.05%-0.5%")
  assert not moiety_separated_by_dash("")

# COMMAND ----------

@suite.add_test
def test_join_on_columns_and_filter_max_moieties():
  schema_df_input = StructType([StructField("id", StringType(), False),
                                StructField("epma_description", StringType(), False)])
  df_input = spark.createDataFrame([
    Row('1', 'Trelegy Ellipta 92 mcg-55 mcg-22 mcg/inh inhalation powder'),
    Row('2', 'Vitamin svrthv78 oil')
  ], schema=schema_df_input)


  schema_df_refdata = StructType([
    StructField("MOIETY", StringType(), True),
    StructField("MOIETY_2", StringType(), True),
    StructField("MOIETY_3", StringType(), True),
    StructField("MOIETY_4", StringType(), True),
    StructField("MOIETY_5", StringType(), True),
  ])
  df_refdata = spark.createDataFrame([
    Row("Trelegy", None, None, None, None),
    Row("trelegy", "ellipta", None, None, None),
    Row("Paracetamol", None, None, None, None),
    Row("Vitamin", None, None, None, None),
    Row("vitamin", None, None, None, None),
    Row("Trelegy", "Paracetamol", None, None, None),
  ], schema=schema_df_refdata)

  schema_df_expected = StructType([
    StructField("id", StringType(), False),
    StructField("epma_description", StringType(), False),
    StructField("MOIETY", StringType(), True),
    StructField("MOIETY_2", StringType(), True),
    StructField("MOIETY_3", StringType(), True),
    StructField("MOIETY_4", StringType(), True),
    StructField("MOIETY_5", StringType(), True)
  ])
  df_expected = spark.createDataFrame([
    ('1', 'Trelegy Ellipta 92 mcg-55 mcg-22 mcg/inh inhalation powder', "trelegy", "ellipta", None, None, None),
    ('2', 'Vitamin svrthv78 oil', "Vitamin", None, None, None, None),
    ('2', 'Vitamin svrthv78 oil', "vitamin", None, None, None, None),
  ], schema=schema_df_expected)

  df_actual = join_on_columns_and_filter_max_moieties(
      df_input,
      moiety_join_cols=['MOIETY', 'MOIETY_2', 'MOIETY_3', 'MOIETY_4', 'MOIETY_5'],
      df_refdata=df_refdata,
      id_col='id',
      text_col='epma_description'
    )

  assert compare_results(df_actual, df_expected, join_columns=['epma_description', 'MOIETY'])

# COMMAND ----------

@suite.add_test
def test_partial_entity_match():
  schema_df_input = StructType([
  StructField("epma_id", StringType(), False),
  StructField("original_epma_description", StringType(), False),
  StructField("form_in_text", StringType(), False),
  StructField("epma_description", StringType(), False)
  ])
  df_input = spark.createDataFrame([
    Row('123', 'Trelegy Ellipta 92 mcg inhalation powder', ' ', 'Trelegy Ellipta 92 mcg inhalation powder'),
    Row('456', 'Vitamin 78 mg oil', ' ', 'Vitamin 78 mg oil'),
    Row('467', 'not matching', ' ', 'not matching'),
    Row('724', 'not matching again', ' ', 'not matching again')
  ], schema=schema_df_input)

  schema_df_refdata_vpid = StructType([
    StructField("text_col", StringType(), True),
    StructField("MOIETY", StringType(), True),
    StructField("MOIETY_2", StringType(), True),
    StructField("MOIETY_3", StringType(), True),
    StructField("MOIETY_4", StringType(), True),
    StructField("MOIETY_5", StringType(), True),
    StructField("SVN", StringType(), True),
    StructField("SVU", StringType(), True),
    StructField("_id", StringType(), False),
    StructField("DOSEFORM", StringType(), False),
  ])

  schema_df_refdata_apid = StructType([
    StructField("text_col", StringType(), True),
    StructField("MOIETY", StringType(), True),
    StructField("MOIETY_2", StringType(), True),
    StructField("MOIETY_3", StringType(), True),
    StructField("MOIETY_4", StringType(), True),
    StructField("MOIETY_5", StringType(), True),
    StructField("SVN", StringType(), True),
    StructField("SVU", StringType(), True),
    StructField("SVN2", StringType(), True),
    StructField("SVU2", StringType(), True),
    StructField("SVD", StringType(), True),
    StructField("SDU", StringType(), True),
    StructField("SVN_2", StringType(), True),
    StructField("SVU_2", StringType(), True),
    StructField("SVD_2", StringType(), True),
    StructField("SDU_2", StringType(), True),
    StructField("SVN_3", StringType(), True),
    StructField("SVU_3", StringType(), True),
    StructField("SITE", StringType(), True),
    StructField("_id", StringType(), False),
    StructField("DOSEFORM", StringType(), False),
  ])

  df_refdata_vpid = spark.createDataFrame([
    Row("Trelegy cream", "Trelegy", None, None, None, None, None, None, "7536", "cream"),
    Row("Trelegy ellipta oil", "trelegy", "ellipta", None, None, None, "20", "ml", "120", "oil"),
    Row("Trelegy ellipta balm", "trelegy", "ellipta", None, None, None, "92", "mcg", "66", "balm"),
  ], schema=schema_df_refdata_vpid)

  df_refdata_apid = spark.createDataFrame([
    Row("Vitamin C tablet", "Vitamin", None, None, None, None, "78", "mg", None, None, None, None, None, None, None, None, None, None, None, "8264", "tablet"),
    Row("Vitamin D tablet", "vitamin", None, None, None, None, "78", "mg", None, None, None, None, None, None, None, None, None, None, None, "8348", "cream"),
    Row("Paracetamol solution", "Paracetamol", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, "13412", "solution"),
    Row("Trelegy/Paracetamol balm", "Trelegy", "Paracetamol", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, "9574", "balm"),
  ], schema=schema_df_refdata_apid)

  schema_df_expected = StructType([
    StructField("epma_id", StringType(), True),
    StructField("original_epma_description", StringType(), True),
    StructField("form_in_text", StringType(), True),
    StructField("epma_description", StringType(), True),
    StructField("match_level", StringType(), True),
    StructField("match_id", StringType(), True),
    StructField("match_term", StringType(), True),
  ])

  df_expected = spark.createDataFrame([
    Row('123', 'Trelegy Ellipta 92 mcg inhalation powder', ' ', 'Trelegy Ellipta 92 mcg inhalation powder', 'VPID', '120', "Trelegy ellipta oil"),
    Row('456', 'Vitamin 78 mg oil', ' ', 'Vitamin 78 mg oil', 'APID', '8264', "Vitamin C tablet"),
    Row('123', 'Trelegy Ellipta 92 mcg inhalation powder', ' ', 'Trelegy Ellipta 92 mcg inhalation powder', 'VPID', '66', "Trelegy ellipta balm"),
    Row('456', 'Vitamin 78 mg oil', ' ', 'Vitamin 78 mg oil', 'APID', '8348', "Vitamin D tablet")
  ], schema=schema_df_expected)

  df_expected_na = spark.createDataFrame([
    Row('467', 'not matching', ' ', 'not matching', None, None, None),
    Row('724', 'not matching again', ' ', 'not matching again', None, None, None)
  ], schema=schema_df_expected)

  df_result, df_unmappable = partial_entity_match(df_input, df_refdata_apid, df_refdata_vpid, id_col='epma_id', original_text_col='original_epma_description',
                                   form_in_text_col='form_in_text', text_col='epma_description', match_id_col='match_id', match_level_col='match_level',
                                   match_term_col='match_term', ref_id_col='_id', ref_text_col='text_col')

  assert compare_results(df_result.where(col('match_id').isNotNull()), df_expected, join_columns=['epma_description', 'match_id'])
  assert compare_results(df_result.where(col('match_id').isNull()), df_expected_na, join_columns=['epma_description'])

# COMMAND ----------

@suite.add_test
def test_partial_entity_match_exclude_multiples():
  schema_df_input = StructType([
  StructField("epma_id", StringType(), False),
  StructField("original_epma_description", StringType(), False),
  StructField("form_in_text", StringType(), False),
  StructField("epma_description", StringType(), False)
  ])
  df_input = spark.createDataFrame([
    Row('123', 'Paracetamol 500mg in Sodium Chloride', ' ', 'paracetamol 500mg in sodium chloride'),
    Row('456', 'Sodium Chloride 500mg', ' ', 'sodium chloride 500mg'),
    Row('467', 'Glucose 500mg', ' ', 'glucose 500mg'),
    Row('724', 'Adcal and Calcium', ' ', 'adcal and calcium'),
    Row('739', 'Adcal-D3 500mg', ' ', 'adcal-d3 500mg'),
    Row('770', 'Adcal and Paracetamol', ' ', 'adcal and paracetamol'),
    Row('885', 'Calcium Carbonate 500mg', ' ', 'calcium carbonate 500mg'),
    Row('999', 'Adcal (Calcium) 500mg', ' ', 'adcal (calcium) 500mg')
  ], schema=schema_df_input)

  schema_df_refdata_vpid = StructType([
    StructField("text_col", StringType(), True),
    StructField("MOIETY", StringType(), True),
    StructField("MOIETY_2", StringType(), True),
    StructField("MOIETY_3", StringType(), True),
    StructField("MOIETY_4", StringType(), True),
    StructField("MOIETY_5", StringType(), True),
    StructField("SVN", StringType(), True),
    StructField("SVU", StringType(), True),
    StructField("_id", StringType(), False),
    StructField("DOSEFORM", StringType(), False),
  ])

  schema_df_refdata_apid = StructType([
    StructField("text_col", StringType(), True),
    StructField("MOIETY", StringType(), True),
    StructField("MOIETY_2", StringType(), True),
    StructField("MOIETY_3", StringType(), True),
    StructField("MOIETY_4", StringType(), True),
    StructField("MOIETY_5", StringType(), True),
    StructField("SVN", StringType(), True),
    StructField("SVU", StringType(), True),
    StructField("SVN2", StringType(), True),
    StructField("SVU2", StringType(), True),
    StructField("SVD", StringType(), True),
    StructField("SDU", StringType(), True),
    StructField("SVN_2", StringType(), True),
    StructField("SVU_2", StringType(), True),
    StructField("SVD_2", StringType(), True),
    StructField("SDU_2", StringType(), True),
    StructField("SVN_3", StringType(), True),
    StructField("SVU_3", StringType(), True),
    StructField("SITE", StringType(), True),
    StructField("_id", StringType(), False),
    StructField("DOSEFORM", StringType(), False),
  ])

  df_refdata_vpid = spark.createDataFrame([
    Row("Adcal and Paracetamol", "Adcal", "Paracetamol", None, None, None, None, None, "1", "tablet"),
    Row("Adcal", "Adcal", None, None, None, None, None, None, "2", "tablet"),
    Row("Adcal-D3", "Adcal-D3", None, None, None, None, None, None, "3", "tablet"),
    Row("Calcium Carbonate", "Calcium Carbonate", None, None, None, None, None, None, "4", "tablet"),
    Row("Sodium Chloride", "Sodium Chloride", None, None, None, None, None, None, "6", "tablet"),
    Row("Glucose", "Glucose", None, None, None, None, None, None, "7", "tablet"),
    Row("Calcium", "Calcium", None, None, None, None, None, None, "8", "tablet"),
    Row("Paracetamol", "Paracetamol", None, None, None, None, None, None, "9", "tablet"),
  ], schema=schema_df_refdata_vpid)

  df_refdata_apid = spark.createDataFrame([
    Row("Adcal and Paracetamol", "Adcal", "Paracetamol", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, "11", "tablet"),
    Row("Adcal", "Adcal", None,  None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,"12", "tablet"),
    Row("Adcal-D3", "Adcal-D3", None,  None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,"13", "tablet"),
    Row("Calcium Carbonate", "Calcium Carbonate", None,  None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, "14", "tablet"),
    Row("Sodium Chloride", "Sodium Chloride", None,  None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, "16", "tablet"),
    Row("Glucose", "Glucose", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,"17", "tablet"),
    Row("Calcium", "Calcium",  None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, "18", "tablet"),
    Row("Paracetamol", "Paracetamol", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, "19", "tablet"),
  ], schema=schema_df_refdata_apid)

  schema_df_expected = StructType([
    StructField("epma_id", StringType(), True),
    StructField("original_epma_description", StringType(), True),
    StructField("form_in_text", StringType(), True),
    StructField("epma_description", StringType(), True),
    StructField("match_level", StringType(), True),
    StructField("match_id", StringType(), True),
    StructField("match_term", StringType(), True),
  ])

  schema_df_expected_unmappable = StructType([
    StructField("original_epma_description", StringType(), True),
    StructField("form_in_text", StringType(), True)
  ])

  df_expected = spark.createDataFrame([
    Row('770', 'Adcal and Paracetamol', ' ', 'adcal and paracetamol', 'APID', '11', 'Adcal and Paracetamol'),
    Row('467', 'Glucose 500mg', ' ', 'glucose 500mg', 'APID', '17', 'Glucose'),
    Row('456', 'Sodium Chloride 500mg', ' ', 'sodium chloride 500mg', 'APID', '16', 'Sodium Chloride'),
    Row('739', 'Adcal-D3 500mg', ' ', 'adcal-d3 500mg', 'APID', '13', 'Adcal-D3'),
    Row('123', 'Paracetamol 500mg in Sodium Chloride', ' ', 'paracetamol 500mg in sodium chloride', 'APID', '19', 'Paracetamol'),
    Row('885', 'Calcium Carbonate 500mg', ' ', 'calcium carbonate 500mg', 'APID', '14', 'Calcium Carbonate'),
    Row('999', 'Adcal (Calcium) 500mg', ' ', 'adcal (calcium) 500mg', 'APID', '12', 'Adcal'),
    Row('770', 'Adcal and Paracetamol', ' ', 'adcal and paracetamol', 'VPID', '1', 'Adcal and Paracetamol'),
    Row('467', 'Glucose 500mg', ' ', 'glucose 500mg', 'VPID', '7', 'Glucose'),
    Row('456', 'Sodium Chloride 500mg', ' ', 'sodium chloride 500mg', 'VPID', '6', 'Sodium Chloride'),
    Row('739', 'Adcal-D3 500mg', ' ', 'adcal-d3 500mg', 'VPID', '3', 'Adcal-D3'),
    Row('123', 'Paracetamol 500mg in Sodium Chloride', ' ', 'paracetamol 500mg in sodium chloride', 'VPID', '9', 'Paracetamol'),
    Row('885', 'Calcium Carbonate 500mg', ' ', 'calcium carbonate 500mg', 'VPID', '4', 'Calcium Carbonate'), 
    Row('999', 'Adcal (Calcium) 500mg', ' ', 'adcal (calcium) 500mg', 'VPID', '2', 'Adcal')
  ], schema=schema_df_expected)

  df_expected_unmappable = spark.createDataFrame([
    Row('Adcal and Calcium', ' '),
  ], schema=schema_df_expected_unmappable)

  df_result, df_unmappable = partial_entity_match(df_input, df_refdata_apid, df_refdata_vpid, id_col='epma_id', original_text_col='original_epma_description',
                                   form_in_text_col='form_in_text', text_col='epma_description', match_id_col='match_id', match_level_col='match_level',
                                   match_term_col='match_term', ref_id_col='_id', ref_text_col='text_col')

  assert compare_results(df_result, df_expected, join_columns=['original_epma_description', 'match_id'])
  assert compare_results(df_unmappable, df_expected_unmappable, join_columns=['original_epma_description'])

# COMMAND ----------

@suite.add_test
def test_entity_match():
  schema_df_input = StructType([
    StructField("epma_id", StringType(), False),
    StructField("original_epma_description", StringType(), False),
    StructField("form_in_text", StringType(), False),
    StructField("epma_description", StringType(), False)
  ])

  df_input = spark.createDataFrame([
    Row('123', 'Trelegy Ellipta 92 mcg inhalation powder', ' ', 'Trelegy Ellipta 92 mcg inhalation powder'),
    Row('456', 'Vitamin 78 mg oil', ' ', 'Vitamin 78 mg oil')
  ], schema=schema_df_input)

  schema_df_refdata = StructType([
    StructField("MOIETY", StringType(), True),
    StructField("MOIETY_2", StringType(), True),
    StructField("MOIETY_3", StringType(), True),
    StructField("MOIETY_4", StringType(), True),
    StructField("MOIETY_5", StringType(), True),
    StructField("SITE", StringType(), True),
    StructField("DOSEFORM", StringType(), True),
    StructField("SVN", StringType(), True),
    StructField("SVU", StringType(), True),
    StructField("SVN2", StringType(), True),
    StructField("SVU2", StringType(), True),
    StructField("SVD", StringType(), True),
    StructField("SDU", StringType(), True),
    StructField("SVN_2", StringType(), True),
    StructField("SVU_2", StringType(), True),
    StructField("SVD_2", StringType(), True),
    StructField("SDU_2", StringType(), True),
    StructField("SVN_3", StringType(), True),
    StructField("SVU_3", StringType(), True),
    StructField("ref_id", StringType(), False),
  ])

  df_refdata = spark.createDataFrame([
    Row("Trelegy", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, "7536"),
    Row("trelegy", "ellipta", None, None, None, None, None, "20", "ml", None, None, None, None, None, None, None, None, None, None, "120"),
    Row("trelegy", "ellipta", None, None, None, None, None, "92", "mcg", None, None, None, None, None, None, None, None, None, None, "66"),
    Row("Vitamin", None, None, None, None, None, None, "78", "mg", None, None, None, None, None, None, None, None, None, None, "8264"),
    Row("vitamin", None, None, None, None, None, None, "78", "mg", None, None, None, None, None, None, None, None, None, None, "8348"),
    Row("Paracetamol", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, "13412"),
    Row("Trelegy", "Paracetamol", None, None, None, None,None, None,  None, None, None, None, None, None, None, None, None, None, None, "9574"),
  ], schema=schema_df_refdata)
  
  schema_df_expected = StructType([
    StructField("epma_id", StringType(), False),
    StructField("original_epma_description", StringType(), False),
    StructField("form_in_text", StringType(), False),
    StructField("epma_description", StringType(), False),
    StructField("match_id", StringType(), False),
    StructField("id_level", StringType(), False),
    StructField("match_level", StringType(), False),
  ])

  df_expected = spark.createDataFrame([
    ('123', 'Trelegy Ellipta 92 mcg inhalation powder', ' ', 'Trelegy Ellipta 92 mcg inhalation powder', '66', 'VPID', "entity"),
  ], schema=schema_df_expected)

  df_actual = entity_match(df_input, df_refdata=df_refdata, ref_id_level="VPID", dose_form_list_bc=dose_form_list_bc,
                           id_col='epma_id', original_text_col='original_epma_description', form_in_text_col='form_in_text', text_col='epma_description',
                           match_id_col='match_id', id_level_col='id_level', match_level_col='match_level', match_datetime_col='match_datetime', ref_id_col='ref_id') \
              .drop('match_datetime')

  assert compare_results(df_actual, df_expected, join_columns=['match_id'])

# COMMAND ----------

@suite.add_test
def test_remove_substrings():

  # note: the prefix "_dif" is used here in the column names, but don't use "_diff" as this prefix is used in compare_results.
  
  schema_df_input = StructType([StructField('id', StringType(), False),
                                StructField('_dif_moieties', ArrayType(StringType()), False)])

  df_input = spark.createDataFrame([
    ('1', ['adcal', 'adcal-d3'],),
    ('2', ['deep heat', 'deep'],),
    ('3', ['adcal', 'adcal-d3', 'calcium'],)
  ], schema_df_input)

  schema_df_expected = StructType([StructField('id', StringType(), False),
                                   StructField('_dif_moieties', ArrayType(StringType()), False),
                                   StructField('_dif_moieties_except_substrings', ArrayType(StringType()), False)])

  df_expected = spark.createDataFrame([
    ('1', ['adcal', 'adcal-d3'], ['adcal-d3']),
    ('2', ['deep heat', 'deep'], ['deep heat']),
    ('3', ['adcal', 'adcal-d3', 'calcium'], ['adcal-d3', 'calcium'])
  ], schema=schema_df_expected)

  df_output = df_input.withColumn('_dif_moieties_except_substrings', remove_substrings_udf(F.col('_dif_moieties')))

  assert compare_results(df_output, df_expected, join_columns=['id'])

# COMMAND ----------

suite.run()