# Databricks notebook source
import copy

# COMMAND ----------

# MAGIC %run ../notebooks/_modules/epma_global/function_test_suite

# COMMAND ----------

# MAGIC %run ../notebooks/_modules/epma_global/functions

# COMMAND ----------

# MAGIC %run ../notebooks/_modules/epma_global/test_helpers

# COMMAND ----------

# MAGIC %run ../notebooks/_pipeline_execution/pipeline_exec

# COMMAND ----------

suite = FunctionTestSuite()

# COMMAND ----------

PIPELINE_CONFIG = [
  { # Raw data inputs. Must be the zeroth stage
    'test_row': 'input_table'   
  },
  {
    'stage_id': 'exceptions_and_preprocessing',
    'notebook_location': 'test path',
    'raw_data_required': True,
    'execute': True
  },
  {
    'stage_id': 'exact_match',
    'notebook_location': 'test_path',
    'raw_data_required': True,                   
    'execute': True
  },
  {
    'stage_id': 'entity_matching',
    'notebook_location': 'test_path',
    'raw_data_required': True,
     'execute': True
  },
  {
    'stage_id': 'fuzzy_matching',
    'notebook_location': 'test_path',
    'raw_data_required': True,
     'execute': True   
  }
]

# COMMAND ----------

@suite.add_test
def test_run_all_and_stage_count():
  LOCAL_PIPE_CONFIG = copy.deepcopy(PIPELINE_CONFIG)
  expected = 3 
  with MockDBUtilsContext(dbutils) as manager:
    stage_count, stages = run_pipeline(LOCAL_PIPE_CONFIG)    
  assert expected == stage_count
  
  
@suite.add_test
def test_key_conflict():
  KEY_PIPELINE_CONFIG = [
  { # Raw data inputs. Must be the zeroth stage
  'test_row': 'input_table'   
  },
  {
  'stage_id': 'exceptions_and_preprocessing',
  'notebook_location': 'test path',
  'raw_data_required': True,
  'test_row': 'test'
  }]

  try:
    run_pipeline(KEY_PIPELINE_CONFIG)
  except KeyError:
    assert True
    
  
@suite.add_test
def test_run_check_all_stage_description():
  LOCAL_PIPE_CONFIG = copy.deepcopy(PIPELINE_CONFIG)
  expected = ['exceptions_and_preprocessing', 'exact_match', 'entity_matching', 'fuzzy_matching']
  with MockDBUtilsContext(dbutils) as manager:
    stage_count, stages= run_pipeline(LOCAL_PIPE_CONFIG)
  actual=[stage['stage_id'] for stage in stages]
  assert expected == actual
  

@suite.add_test
def test_raw_data_required():

  PIPELINE_CONFIG = [
  { # Raw data inputs. Must be the zeroth stage
  'test_row': 'input_table'   
  },
  {
  'stage_id': 'exceptions_and_preprocessing',
  'notebook_location': 'test path',
  'raw_data_required': True,
  'execute': True
  }]

  with MockDBUtilsContext(dbutils) as manager:
    stage_count, stages = run_pipeline(PIPELINE_CONFIG)
  assert ('test_row' in stages[0])  is True
  

@suite.add_test
def test_raw_data_not_required():

  PIPELINE_CONFIG = [
  { # Raw data inputs. Must be the zeroth stage
  'test_row': 'input_table'   
  },
  {
  'stage_id': 'exceptions_and_preprocessing',
  'notebook_location': 'test path',
  'raw_data_required': False,
  'execute': True
  }]

  with MockDBUtilsContext(dbutils) as manager:
    stage_count, stages = run_pipeline(PIPELINE_CONFIG) 
  assert ('test_row' in stages[0])  is False



# COMMAND ----------

suite.run()