# Databricks notebook source
# MAGIC %run ./functions

# COMMAND ----------

import traceback
from uuid import uuid4
import warnings 

# COMMAND ----------

INT_TEST_DEFAULT_SOURCE_DATASET = 'source_a'
INT_TEST_SOURCE_DATASETS = {INT_TEST_DEFAULT_SOURCE_DATASET, 'source_b'}
INT_TEST_UPLIFT_NOTEBOOK = "./notebooks/uplifts/fake_uplift"

# COMMAND ----------

class IntegrationTestConfig():
  '''
  Class to facilitate integration testing. It creates all the necessary tables for a run of the integration test (i.e. the end-to-end pipeline) 
  and drops all the tables at the end of the pipeline.
  
  Each table is created in the given database with a random temporary name, so it won't collide with other on-going tests.
  
  This class is a context manager.
  '''
  
  TABLE_ATTRS = [   
    '_inter_preprocessed_inputs',
    '_inter_exact_non_match',
    '_inter_entity_non_match',
    '_inter_match_lookup',
    'match_lookup_final_table',
    'unmappable_table',
    '_cache_fuzzy_non_linked',
    '_cache_fuzzy_non_linked_non_match',    
    'accuracy_table',
    'source_b_accuracy_table',
    'uplift_table'
  ]

  def __init__(self, target_db='test_epma_autocoding', source_dataset_name=INT_TEST_DEFAULT_SOURCE_DATASET, uplift_notebook_name= INT_TEST_UPLIFT_NOTEBOOK, attrs_to_save=None):
    self.target_db = target_db
    self._tables = {}
    self.uplift_notebook = uplift_notebook_name
    self._attrs_to_save = attrs_to_save or []
    
    if not source_dataset_name in INT_TEST_SOURCE_DATASETS:
      raise AssertionError(f'Given source dataset {source_dataset_name} is not a valid choice (choices: {INT_TEST_SOURCE_DATASETS}).')
    
    self._source_dataset_name = source_dataset_name
    
  def __enter__(self):
    self._create_tables()
    return self

  def __exit__(self, exc_type, exc_value, tb):
    if exc_type is not None:
      traceback.print_exception(exc_type, exc_value, tb)
    self._drop_tables()
    
  def _add_table(self, attr_name: str):
    random_table_name = f'_tmp_integration_{uuid4().hex}'
    random_asset_name = f'{self.target_db}.{random_table_name}'
    self._tables[attr_name] = random_table_name
    setattr(self, attr_name, random_asset_name)
    print(f'INFO: Created table attr {attr_name} with value {random_asset_name}.')
    
  def _create_tables(self):
    for table_attr in self.TABLE_ATTRS:
      self._add_table(table_attr)
      
  def _drop_tables(self):
    do_all_tables_exist = True
    
    for table_attr in self.TABLE_ATTRS:
      table_name = self._tables[table_attr]  
      
      if table_attr in self._attrs_to_save:
        warnings.warn(f'INFO: Table attr {table_attr} with table name {table_name} will not be dropped. You should drop this manually.')
        continue
        
      if not table_exists(self.target_db, table_name):
        do_all_tables_exist = False
        warnings.warn(f'WARN: Table attr {table_attr} was never created, and thus will not be dropped (table name: {table_name}).')
      else:
        drop_table(self.target_db, table_name)    
    
    if do_all_tables_exist is False:
      warnings.warn(f'At least one table does not exist. It (They) must have been dropped during the test.')
      