# Databricks notebook source
# MAGIC %run ../notebooks/_modules/epma_global/integration_test_helpers

# COMMAND ----------

# MAGIC %run ../notebooks/_pipeline_execution/run_matching_pipeline

# COMMAND ----------

dbutils.widgets.dropdown('source_dataset', INT_TEST_DEFAULT_SOURCE_DATASET, list(INT_TEST_SOURCE_DATASETS))
SOURCE_DATASET = dbutils.widgets.get('source_dataset')
assert SOURCE_DATASET

# COMMAND ----------

dbutils.widgets.text('uplift_notebook', INT_TEST_UPLIFT_NOTEBOOK, 'uplift_notebook')
UPLIFT_NOTEBOOK = dbutils.widgets.get('uplift_notebook')
assert UPLIFT_NOTEBOOK

# COMMAND ----------

if SOURCE_DATASET == 'source_a':
  RAW_INPUT_TABLE = 'epma.epmawspc2'
elif SOURCE_DATASET == 'source_b':
  RAW_INPUT_TABLE = 'test_epma_autocoding.source_b_sample_3'
else:
  raise AttributeError('SOURCE_DATASET must be specified')
assert RAW_INPUT_TABLE

GROUND_TRUTH_TABLE = 'epma.epmawspc2'
assert GROUND_TRUTH_TABLE

SOURCE_B_GROUND_TRUTH = 'test_epma_autocoding.source_b_gt'
assert SOURCE_B_GROUND_TRUTH

SOURCE_B_ACCURACY_BASELINE = 'test_epma_autocoding.source_b_baseline_2021_10_27'
assert SOURCE_B_ACCURACY_BASELINE

# COMMAND ----------

DB = 'test_epma_autocoding'
#ATTRS_TO_SAVE = ['match_lookup_final_table', 'unmappable_table']
ATTRS_TO_SAVE = None

# COMMAND ----------

def init_schemas_integration_test(db, uplift_notebook_asset, match_lookup_final_asset, unmappable_table_asset, accuracy_table_asset, uplift_table_asset):
  match_lookup_final_table_name = match_lookup_final_asset.split('.')[1]
  unmappable_table_name = unmappable_table_asset.split('.')[1]
  accuracy_table_name = accuracy_table_asset.split('.')[1]
  uplift_notebook_name = uplift_notebook_asset.split('.')[1]
  uplift_table_name = uplift_table_asset.split('.')[1] 
 
  dbutils.notebook.run('./../init_schemas', 0, {
    'db' : db,
    'match_lookup_final_table_name': match_lookup_final_table_name,
    'unmappable_table_name': unmappable_table_name,
    'accuracy_table_name': accuracy_table_name,
    'uplift_notebook': uplift_notebook_name,
    'uplift_table': uplift_table_name    
  })

# COMMAND ----------

with IntegrationTestConfig(DB, SOURCE_DATASET, UPLIFT_NOTEBOOK, ATTRS_TO_SAVE) as ct:
 
  init_schemas_integration_test(ct.target_db, ct.uplift_notebook, ct.match_lookup_final_table, ct.unmappable_table, ct.accuracy_table, ct.uplift_table) 
  
  if not table_exists(ct.target_db, ct.uplift_table.split('.')[1]):
       raise AssertionError(f'INFO: Uplift table {ct.uplift_table} does not exist')

  PIPELINE_CONFIG = [
    { # Raw data inputs. Must be the zeroth stage
      'epma_table': RAW_INPUT_TABLE,    
      'vtm_table': 'dss_corporate.vtm', 
      'vmp_table': 'dss_corporate.vmp',
      'amp_table': 'dss_corporate.amp',
      'parsed_vtm_table': '',
      'parsed_vmp_table': 'dss_corporate.vmp_parsed',
      'parsed_amp_table': 'dss_corporate.amp_parsed',
    },
    {
      'stage_id': 'exceptions_and_preprocessing',
      'notebook_location': '../notebooks/0_exceptions_and_preprocessing/drivers/exceptions_and_preprocessing_driver',
      'raw_data_required': True,
      'source_dataset': SOURCE_DATASET,
      'unmappable_table': ct.unmappable_table,
      'output_table': ct._inter_preprocessed_inputs,
      'match_lookup_final_table': ct.match_lookup_final_table,
      'batch_size': '10',
      'run_id': '666',
      'execute': True,
    },
    {
      'stage_id': 'exact_match',
      'notebook_location': '../notebooks/1_exact_match/drivers/exact_match_driver',
      'raw_data_required': True,
      'input_table': ct._inter_preprocessed_inputs,
      'output_table': ct._inter_exact_non_match,
      'match_table': ct._inter_match_lookup,
      'execute': True,
    },
    {
      'stage_id': 'entity_matching',
      'notebook_location': '../notebooks/2_entity_extraction/drivers/entity_extraction_driver',
      'raw_data_required': True,
      'input_table': ct._inter_exact_non_match,
      'match_table': ct._inter_match_lookup,
      'output_table': ct._inter_entity_non_match,
      'unmappable_table': ct.unmappable_table,
      'run_id': '666',
      'execute': True,
    },
    {
      'stage_id': 'fuzzy_matching',
      'notebook_location': '../notebooks/3_fuzzy_matching/drivers/fuzzy_match_driver',
      'raw_data_required': True,
      'input_table': ct._inter_entity_non_match,
      'output_table': ct._inter_match_lookup,
      'match_lookup_final_table': ct.match_lookup_final_table,
      'unmappable_table': ct.unmappable_table,
      'fuzzy_non_linked': ct._cache_fuzzy_non_linked,
      'fuzzy_nonlinked_non_match_output': ct._cache_fuzzy_non_linked_non_match,
      'match_lookup_final_version': 'integration_test_version',
      'run_id': '666',
      'execute': True,
    },
    {
      'stage_id': 'accuracy_calculating',
      'notebook_location': '../notebooks/4_accuracy_calculating/drivers/accuracy-calculating-driver',
      'raw_data_required': True,
      'input_table': ct.match_lookup_final_table,
      'output_table': ct.accuracy_table,
      'ground_truth_table': GROUND_TRUTH_TABLE,
      'execute': True
    },
    {
      'stage_id': 'source_b_accuracy',
      'notebook_location': '../notebooks/5_source_b_accuracy/drivers/source_b_accuracy_driver',
      'raw_data_required': True,
      'input_table': ct.match_lookup_final_table,
      'unmappable_table': ct.unmappable_table,
      'output_table': ct.source_b_accuracy_table,
      'baseline_table': SOURCE_B_ACCURACY_BASELINE,
      'ground_truth_table': SOURCE_B_GROUND_TRUTH,
      'execute': True if SOURCE_DATASET == 'source_b' else False
    }
  ]

  run_matching_pipeline(PIPELINE_CONFIG)