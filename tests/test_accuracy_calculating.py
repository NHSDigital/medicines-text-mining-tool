# Databricks notebook source
# MAGIC %run ../notebooks/_modules/epma_global/function_test_suite

# COMMAND ----------

# MAGIC %run ../notebooks/_modules/epma_global/functions

# COMMAND ----------

# MAGIC %run ../notebooks/_modules/epma_global/test_helpers

# COMMAND ----------

# MAGIC %run ../notebooks/4_accuracy_calculating/functions/accuracy_calculating_functions

# COMMAND ----------

# MAGIC %run ../notebooks/_modules/epma_global/ref_data_lib

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import col
import pyspark.sql.functions as F

# COMMAND ----------

suite = FunctionTestSuite()

# COMMAND ----------

@suite.add_test
def test_combine_ref_data():
  
  df_amp = spark.createDataFrame([
    ('apid3', 'Palifermin 6.25mg powder for solution for injection vials'),
    ('apid4', 'Bismuth subnitrate powder')
  ], 
    ['_id', 'text_col'])

  df_vmp = spark.createDataFrame([
      ('vpid3', 'Desmopressin 120microgram oral lyophilisates sugar free'),
      ('vpid4', 'Ferric chloride solution strong BPC 1973'),
    ], 
      ['_id', 'text_col'])

  df_vtm = spark.createDataFrame([
    ('vtmid3', 'Urokinase 10,000'),
    ('vtmid4', 'Vancomycin powder'),
  ], 
    ['_id', 'text_col'])

  df_vmp_prev = spark.createDataFrame([
      ('vpid5', 'Desmopressin 240microgram', 'vpidprev5')
    ], 
      ['_id', 'text_col', 'VPIDPREV'])

  df_vtm_prev = spark.createDataFrame([
    ('vtmid5', 'Vancomycin lotion', 'vtmidprev5')
  ], 
    ['_id', 'text_col', 'VTMIDPREV'])  
  
  class MockRefDataStore():
    amp = df_amp
    vmp = df_vmp
    vtm = df_vtm
    vmp_prev = df_vmp_prev
    vtm_prev = df_vtm_prev
    ID_COL = '_id'
    TEXT_COL = 'text_col'
    ID_LEVEL_COL = 'id_level'

  df_expected = spark.createDataFrame([
    ('apid3', 'Palifermin 6.25mg powder for solution for injection vials', 'AMP'),
    ('apid4', 'Bismuth subnitrate powder', 'AMP'),
    ('vpid3', 'Desmopressin 120microgram oral lyophilisates sugar free', 'VMP'),
    ('vpid4', 'Ferric chloride solution strong BPC 1973', 'VMP'),
    ('vtmid3', 'Urokinase 10,000', 'VTM'),
    ('vtmid4', 'Vancomycin powder', 'VTM'),
    ('vpidprev5', 'Desmopressin 240microgram', 'VMP'),
    ('vtmid5', 'Vancomycin lotion', 'VTM')
  ], ['_id', 'text_col', 'id_level'])

  with FunctionPatch('ReferenceDataFormatter', MockRefDataStore):
    df_amp_vmp_vtm = combine_ref_data(MockRefDataStore())
    
    assert compare_results(df_amp_vmp_vtm, df_expected, join_columns=['_id'])

# COMMAND ----------

@suite.add_test
def test_add_dss_to_lookup():
  
  df_amp_vmp_vtm = spark.createDataFrame([
    ('apid3', 'Palifermin 6.25mg powder for solution for injection vials'),
    ('apid4', 'Bismuth subnitrate powder'),
    ('vpid3', 'Desmopressin 120microgram oral lyophilisates sugar free'),
    ('vpid4', 'Ferric chloride solution strong BPC 1973'),
    ('vtmid3', 'Urokinase 10,000'),
    ('vtmid4', 'Vancomycin powder')
  ], 
    ['_id', 'text_col'])

  df_lookup = spark.createDataFrame([
    ('001', 'Bismuth powder', 'apid4'),
    ('002', 'Urokinase 10,000 unit liquid', 'vtmid3'),
    ('003', 'Desmopressin 120microgram', 'vpid3')
  ], 
    ['epma_id', 'epma_description', 'our_id'])

  df_lookup_dss = add_dss_to_lookup(df_amp_vmp_vtm,
                                    df_lookup,
                                    ref_data_id_col='_id',
                                    ref_data_text_col='text_col',
                                    new_text_col='our_name',
                                    lookup_id_col='our_id')

  df_expected = spark.createDataFrame([
    ('001', 'Bismuth powder', 'apid4', 'Bismuth subnitrate powder'),
    ('002', 'Urokinase 10,000 unit liquid', 'vtmid3', 'Urokinase 10,000'),
    ('003', 'Desmopressin 120microgram', 'vpid3', 'Desmopressin 120microgram oral lyophilisates sugar free')
  ], 
    ['epma_id', 'epma_description', 'our_id', 'our_name'])

  assert compare_results(df_lookup_dss, df_expected, join_columns=['epma_id'])

# COMMAND ----------

@suite.add_test
def test_ground_truth_add_id_level():
  
  df_amp_vmp_vtm = spark.createDataFrame([
    ('ws001', 'AMP'),
    ('ws002', 'VMP'),
    ('ws003', 'VTM')
  ], 
    ['_id', 'id_level'])

  df_ground_truth = spark.createDataFrame([
    ('Bismuth Powder', 'ws001'),
    ('Urokinase 10,000 Unit Liquid', 'ws002'),
    ('Desmopressin 120microgram', 'ws003')
  ], 
    ['epma_description', 'ws_id'])

  df_ground_truth_add_id_level= ground_truth_add_id_level(df_amp_vmp_vtm,
                                                          df_ground_truth, 
                                                          ref_data_id_col='_id', 
                                                          ref_data_id_level_col='id_level', 
                                                          new_id_level_col='ws_id_level',
                                                          ground_truth_id_col='ws_id')

  df_expected = spark.createDataFrame([
    ('Bismuth Powder', 'ws001', 'AMP'),
    ('Urokinase 10,000 Unit Liquid', 'ws002', 'VMP'),
    ('Desmopressin 120microgram', 'ws003','VTM')
  ], 
    ['epma_description', 'ws_id', 'ws_id_level'])

  assert compare_results(df_ground_truth_add_id_level, df_expected, join_columns=['epma_description'])

# COMMAND ----------

@suite.add_test
def test_add_ground_truth_to_lookup():
  
  df_lookup = spark.createDataFrame([
    ('001', 'Bismuth powder', 'apid4', 'Bismuth subnitrate powder'),
    ('002', 'Urokinase 10,000 unit liquid', 'vtmid3', 'Urokinase 10,000'),
    ('003', 'Desmopressin 120microgram', 'vpid3', 'Desmopressin 120microgram oral lyophilisates sugar free')
  ], 
    ['epma_id', 'original_epma_description', 'our_id', 'our_name'])

  df_ground_truth = spark.createDataFrame([
    ('Bismuth Powder', 'ws001', 'AMP'),
    ('Urokinase 10,000 Unit Liquid', 'ws002', 'VMP'),
    ('Desmopressin 120microgram', 'ws003', 'VTM')
  ], 
    ['original_epma_description', 'ws_id', 'ws_id_level'])

  groud_truth_lookup_join_col='original_epma_description'
  
  df_lookup_ref_ground_truth = add_ground_truth_to_lookup(df_lookup, df_ground_truth, join_col='original_epma_description')
  
  df_expected = spark.createDataFrame([
    ('001', 'Bismuth powder', 'apid4', 'Bismuth subnitrate powder', 'ws001', 'AMP'),
    ('002', 'Urokinase 10,000 unit liquid', 'vtmid3', 'Urokinase 10,000', 'ws002', 'VMP'),
    ('003', 'Desmopressin 120microgram', 'vpid3', 'Desmopressin 120microgram oral lyophilisates sugar free', 'ws003', 'VTM')
  ], 
    ['epma_id', 'original_epma_description', 'our_id', 'our_name', 'ws_id', 'ws_id_level'])
  
  assert compare_results(df_lookup_ref_ground_truth, df_expected, join_columns=['epma_id'])
  

# COMMAND ----------

suite.run() 