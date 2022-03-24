# Databricks notebook source
# MAGIC %run ../notebooks/_modules/epma_global/ref_data_lib

# COMMAND ----------

# MAGIC %run ../notebooks/_modules/epma_global/functions

# COMMAND ----------

# MAGIC %run ../notebooks/_modules/epma_global/function_test_suite

# COMMAND ----------

# MAGIC %run ../notebooks/_modules/epma_global/test_helpers

# COMMAND ----------

from enum import Enum
from uuid import uuid4

# COMMAND ----------

suite = FunctionTestSuite()

# COMMAND ----------

@suite.add_test
def test_amp():  
  df_input = spark.createDataFrame([
    ('0', '0', '0', 'a', 'a', None),
    ('1', '1', '1', 'b', 'b', 1),
    ('2', '2', '2', 'c', 'c', 0),
    ('3', '3', '3', 'e', 'd', None),
    ('4', '4', '4', 'F', 'F', None),
    ('5', '5', '5', 'pARACETAMOL', 'pARACETAMOL', None),
    ('6', '7', '8', 'g', 'g', None),
  ], ['index', 'APID', 'VPID', 'NM', 'NM_PREV', 'INVALID'])

  temp_table_name = f'_tmp_{uuid4().hex}'
  df_input.createOrReplaceGlobalTempView(temp_table_name)

  df_expected = spark.createDataFrame([
    ('0', 'a', '0', '0', 'APID'),
    ('3', 'e', '3', '3', 'APID'),
    ('4', 'f', '4', '4', 'APID'),
    ('5', 'paracetamol', '5', '5', 'APID'),
    ('7', 'g', '7', '8', 'APID'),
  ], ['ref_id', 'ref_description', 'APID', 'VPID', 'id_level'])

  class MockRefDataset(Enum):
    AMP = temp_table_name

  with FunctionPatch('RefDataset', MockRefDataset):
    rdf = ReferenceDataFormatter('global_temp')

    assert rdf._amp is None
    assert compare_results(rdf.amp, df_expected, join_columns=['ref_id'])
    assert not rdf._amp is None

# COMMAND ----------

@suite.add_test
def test_amp_prev():  
  df_input = spark.createDataFrame([
    ('0', '0', '0', 'a', 'a', None),
    ('1', '1', '1', 'b', 'b', 1),
    ('2', '2', '2', 'c', 'D', None),
    ('3', '4', '5', 'c', 'D', None),
  ], ['index', 'APID', 'VPID', 'NM', 'NM_PREV', 'INVALID'])

  temp_table_name = f'_tmp_{uuid4().hex}'
  df_input.createOrReplaceGlobalTempView(temp_table_name)

  df_expected = spark.createDataFrame([
    ('0', 'a', '0', '0', 'APID'),
    ('2', 'd', '2', '2', 'APID'),
    ('4', 'd', '4', '5', 'APID'),
  ], ['ref_id', 'ref_description', 'APID', 'VPID', 'id_level'])

  class MockRefDataset(Enum):
    AMP = temp_table_name

  with FunctionPatch('RefDataset', MockRefDataset):
    rdf = ReferenceDataFormatter('global_temp')
    
    assert rdf._amp_prev is None
    assert compare_results(rdf.amp_prev, df_expected, join_columns=['ref_id'])
    assert not rdf._amp_prev is None

# COMMAND ----------

@suite.add_test
def test_amp_dedup():  
  df_input = spark.createDataFrame([
    ('0', '0', '0', 'a', 'a', None),
    ('1', '1', '1', 'b', 'b', 1),
    ('2', '2', '2', 'c', 'c', None), #two choices, pick first
    ('3', '3', '2', 'c', 'c', None),
    ('4', '5', '4', 'd', 'd', None), #three choices, pick middle
    ('4', '4', '4', 'd', 'd', None),
    ('4', '6', '4', 'd', 'd', None),
    ('7', '7', '9', 'text1', None, None), #duplicate text
    ('8', '8', '10', 'text1', None, None),
    ('11', '11', '12', 'text2', None, None), #same vpid, diferent text
    ('13', '13', '12', 'text3', None, None),
    ('14', '14', '12', 'text4', None, None),
  ], ['index', 'APID', 'VPID', 'NM', 'NM_PREV', 'INVALID'])

  temp_table_name = f'_tmp_{uuid4().hex}'
  df_input.createOrReplaceGlobalTempView(temp_table_name)

  df_expected = spark.createDataFrame([
    ('0', 'a', '0', '0', 'APID'),
    ('2', 'c', '2', '2', 'APID'),
    ('4', 'd', '4', '4', 'APID'),
    ('7', 'text1', '7', '9', 'APID'),
    ('11', 'text2', '11', '12', 'APID'),
    ('13', 'text3', '13', '12', 'APID'),
    ('14', 'text4', '14', '12', 'APID'),
  ], ['ref_id', 'ref_description', 'APID', 'VPID', 'id_level'])

  class MockRefDataset(Enum):
    AMP = temp_table_name

  with FunctionPatch('RefDataset', MockRefDataset):
    rdf = ReferenceDataFormatter('global_temp')

    assert rdf._amp_dedup is None
    assert compare_results(rdf.amp_dedup, df_expected, join_columns=['ref_id', 'ref_description'])
    assert not rdf._amp_dedup is None

# COMMAND ----------

@suite.add_test
def test_vmp():  
  df_input = spark.createDataFrame([
    ('0', '0', '0', 'a', 'a', None),
    ('1', '1', '1', 'b', 'b', 1),
    ('2', '2', '2', 'c', 'c', 0),
    ('3', '3', '3', 'e', 'd', None),
    ('4', '4', '4', 'F', 'F', None),
    ('5', '5', '5', 'pARACETAMOL', 'pARACETAMOL', None),
    ('6', '7', '8', 'g', 'g', None),
  ], ['index', 'VPID', 'VTMID', 'NM', 'NMPREV', 'INVALID'])

  temp_table_name = f'_tmp_{uuid4().hex}'
  df_input.createOrReplaceGlobalTempView(temp_table_name)

  df_expected = spark.createDataFrame([
    ('0', 'a', '0', 'VPID'),
    ('3', 'e', '3', 'VPID'),
    ('4', 'f', '4', 'VPID'),
    ('5', 'paracetamol', '5', 'VPID'),
    ('7', 'g', '8', 'VPID'),
  ], ['ref_id', 'ref_description', 'VTMID', 'id_level'])

  class MockRefDataset(Enum):
    VMP = temp_table_name

  with FunctionPatch('RefDataset', MockRefDataset):
    rdf = ReferenceDataFormatter('global_temp')
    
    assert rdf._vmp is None
    assert compare_results(rdf.vmp, df_expected, join_columns=['ref_id'])
    assert not rdf._vmp is None

# COMMAND ----------

@suite.add_test
def test_vmp_duplicates():  
  df_input = spark.createDataFrame([
    ('0', '0', '0', 'a', 'a', None),
    ('1', '1', '0', 'a', 'b', None),
    ('2', '2', '2', 'c', 'c', 1),
  ], ['index', 'VPID', 'VTMID', 'NM', 'NMPREV', 'INVALID'])

  temp_table_name = f'_tmp_{uuid4().hex}'
  df_input.createOrReplaceGlobalTempView(temp_table_name)

  class MockRefDataset(Enum):
    VMP = temp_table_name

  with FunctionPatch('RefDataset', MockRefDataset):
    rdf = ReferenceDataFormatter('global_temp')
    try:
      rdf.vmp
      raise Exception
    except AssertionError:
      assert True

# COMMAND ----------

@suite.add_test
def test_vmp_prev():  
  df_input = spark.createDataFrame([
    ('0', '0', '0', 'a', 'a', None, None),
    ('1', '1', '1', 'b', 'b', None, 1),
    ('2', '2', '2', 'c', 'D', None, None),
    ('3', '4', '5', 'c', 'D', None, None),
    ('6', '6', '6', 'e', None, '7', None),
    ('8', '8', '8', 'f', 'g', '9', None),
    ('10', '10', '10', 'h', None, None, None),
  ], ['index', 'VPID', 'VTMID', 'NM', 'NMPREV', 'VPIDPREV', 'INVALID'])

  temp_table_name = f'_tmp_{uuid4().hex}'
  df_input.createOrReplaceGlobalTempView(temp_table_name)

  df_expected = spark.createDataFrame([
    ('0', 'a', '0', 'a', None, 'VPID'),
    ('2', 'c', '2', 'd', None, 'VPID'),
    ('4', 'c', '5', 'd', None, 'VPID'),
    ('6', 'e', '6', None, '7', 'VPID'),
    ('8', 'f', '8', 'g', '9', 'VPID'),
  ], ['ref_id', 'ref_description', 'VTMID', 'NMPREV', 'VPIDPREV', 'id_level'])

  class MockRefDataset(Enum):
    VMP = temp_table_name

  with FunctionPatch('RefDataset', MockRefDataset):
    rdf = ReferenceDataFormatter('global_temp')

    assert rdf._vmp_prev is None
    assert compare_results(rdf.vmp_prev, df_expected, join_columns=['ref_id'])
    assert not rdf._vmp_prev is None

# COMMAND ----------

@suite.add_test
def test_vtm():  
  df_input = spark.createDataFrame([
    ('0', '0', 'a', '0', None),
    ('1', '1', 'b', '1', 1),
    ('2', '2', 'c', '2', 0),
    ('3', '3', 'e', '3', None),
    ('4', '4', 'F', '4', None),
    ('5', '5', 'pARACETAMOL', 'pARACETAMOL', None),
    ('6', '7', 'g', '6', None),
  ], ['index', 'VTMID', 'NM', 'VTMIDPREV', 'INVALID'])

  temp_table_name = f'_tmp_{uuid4().hex}'
  df_input.createOrReplaceGlobalTempView(temp_table_name)

  df_expected = spark.createDataFrame([
    ('0', 'a', 'VTMID'),
    ('3', 'e', 'VTMID'),
    ('4', 'f', 'VTMID'),
    ('5', 'paracetamol', 'VTMID'),
    ('7', 'g', 'VTMID'),
  ], ['ref_id', 'ref_description', 'id_level'])

  class MockRefDataset(Enum):
    VTM = temp_table_name

  with FunctionPatch('RefDataset', MockRefDataset):
    rdf = ReferenceDataFormatter('global_temp')
    
    assert rdf._vtm is None
    assert compare_results(rdf.vtm, df_expected, join_columns=['ref_id'])
    assert not rdf._vtm is None

# COMMAND ----------

@suite.add_test
def test_vtm_duplicates():  
  df_input = spark.createDataFrame([
    ('0', '0', 'a', '0', None),   
    ('0', '0', 'a', '0', None),   
    ('1', '1', 'b', '1', 1),   
  ], ['index', 'VTMID', 'NM', 'VTMIDPREV', 'INVALID'])

  temp_table_name = f'_tmp_{uuid4().hex}'
  df_input.createOrReplaceGlobalTempView(temp_table_name)

  class MockRefDataset(Enum):
    VTM = temp_table_name

  with FunctionPatch('RefDataset', MockRefDataset):
    rdf = ReferenceDataFormatter('global_temp')
    try:
      rdf.vtm
      raise Exception
    except AssertionError:
      assert True

# COMMAND ----------

@suite.add_test
def test_vtm_prev():  
  df_input = spark.createDataFrame([
    ('0', '0', 'a', '0', None),
    ('1', '1', 'b', '1', 1),
    ('2', '2', 'c', '2', None),
    ('3', '4', 'd', '3', None),
    ('5', '5', 'e', '6', None),
  ], ['index', 'VTMID', 'NM', 'VTMIDPREV', 'INVALID'])

  temp_table_name = f'_tmp_{uuid4().hex}'
  df_input.createOrReplaceGlobalTempView(temp_table_name)

  df_expected = spark.createDataFrame([
    ('0', 'a', '0', 'VTMID'),
    ('2', 'c', '2', 'VTMID'),
    ('3', 'd', '4', 'VTMID'),
    ('6', 'e', '5', 'VTMID'),
  ], ['ref_id', 'ref_description', 'VTMID', 'id_level'])

  class MockRefDataset(Enum):
    VTM = temp_table_name

  with FunctionPatch('RefDataset', MockRefDataset):
    rdf = ReferenceDataFormatter('global_temp')

    assert rdf._vtm_prev is None
    assert compare_results(rdf.vtm_prev, df_expected, join_columns=['ref_id'])
    assert not rdf._vtm_prev is None

# COMMAND ----------

@suite.add_test
def test_parsed_datasets():
  df_amp_parsed_input = spark.createDataFrame([
    ('0', '0', 'a', 'a'),
  ], ['index', 'APID', 'TERM', 'MOIETY'])

  temp_amp_table_name = f'_tmp_amp_{uuid4().hex}'
  df_amp_parsed_input.createOrReplaceGlobalTempView(temp_amp_table_name)

  df_amp_parsed_expected = spark.createDataFrame([
    ('0', 'a', '0', 'a'),
  ], ['index', 'MOIETY', 'ref_id', 'ref_description'])
  
  df_vmp_parsed_input = spark.createDataFrame([
    ('1', '1', 'b', 'b'),
  ], ['index', 'VPID', 'TERM', 'MOIETY'])

  temp_vmp_table_name = f'_tmp_vmp_{uuid4().hex}'
  df_vmp_parsed_input.createOrReplaceGlobalTempView(temp_vmp_table_name)

  df_vmp_parsed_expected = spark.createDataFrame([
    ('1', 'b', '1', 'b'),
  ], ['index', 'MOIETY', 'ref_id', 'ref_description'])
  
  class MockRefDataset(Enum):
    AMP_PARSED = temp_amp_table_name
    VMP_PARSED = temp_vmp_table_name

  with FunctionPatch('RefDataset', MockRefDataset):
    rdf = ReferenceDataFormatter('global_temp')

    assert rdf._amp_parsed is None
    assert compare_results(rdf.amp_parsed, df_amp_parsed_expected, join_columns=['index'])
    assert not rdf._amp_parsed is None

    assert rdf._vmp_parsed is None
    assert compare_results(rdf.vmp_parsed, df_vmp_parsed_expected, join_columns=['index'])
    assert not rdf._vmp_parsed is None  

# COMMAND ----------

suite.run()