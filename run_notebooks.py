# Databricks notebook source
dbutils.widgets.dropdown('source_dataset', 'source_a', ['source_a', 'source_b'])
dbutils.widgets.text('raw_input_table', '', 'raw_input_table')
dbutils.widgets.text('db', 'test_epma_autocoding', 'db')
dbutils.widgets.text('batch_size', '10', 'batch_size')
dbutils.widgets.text('notebook_root', '', 'notebook_root')

# COMMAND ----------

# MAGIC %run ./notebooks/_modules/epma_global/functions

# COMMAND ----------

MATCH_LOOKUP_FINAL_VERSION = find_git_hash_regex(dbutils.widgets.get('notebook_root'))
assert MATCH_LOOKUP_FINAL_VERSION

SOURCE_DATASET = dbutils.widgets.get('source_dataset')
assert SOURCE_DATASET

DB = dbutils.widgets.get('db')
assert DB

BATCH_SIZE = dbutils.widgets.get('batch_size')
assert BATCH_SIZE

RAW_INPUT_TABLE = dbutils.widgets.get('raw_input_table')

if RAW_INPUT_TABLE == '':
  if SOURCE_DATASET == 'source_b':
    RAW_INPUT_TABLE = 'test_epma_autocoding.source_b_sample_2'
  if SOURCE_DATASET == 'source_a':
    RAW_INPUT_TABLE = 'epma.epmawspc2'
assert RAW_INPUT_TABLE

GROUND_TRUTH_TABLE = 'epma.epmawspc2'
assert GROUND_TRUTH_TABLE

# COMMAND ----------

# Constants - Exceptions and preprocessing
MATCH_LOOKUP_FINAL_TABLE = f'{DB}.match_lookup_final'
UNMAPPABLE_TABLE = f'{DB}.unmappable'
PREPROCESSING_OUTPUT_TABLE = f'{DB}._inter_preprocessed_inputs'
  
# Constants - Exact matching
EXACT_MATCH_OUTPUT_TABLE = f'{DB}._inter_exact_non_match'      
EXACT_MATCH_MATCH_TABLE = f'{DB}._inter_match_lookup'

# Constants - Exact matching   
ENTITY_MATCH_OUTPUT_TABLE = f'{DB}._inter_entity_non_match'
ENTITY_MATCH_MATCH_TABLE = f'{DB}._inter_match_lookup'                    
 
# Constants - fuzzy matching 
FUZZY_MATCH_OUPUT_TABLE = f'{DB}._inter_match_lookup'
FUZZY_NONLINKED_TABLE = f'{DB}._cache_fuzzy_non_linked'
FUZZY_NON_LINKED_NON_MATCH_TABLE = f'{DB}._cache_fuzzy_non_linked_non_match'   

# Constants - accuracy calculating
ACCURACY_TABLE = f'{DB}.accuracy'

# Constants - run id
RUN_ID = get_new_run_id(MATCH_LOOKUP_FINAL_TABLE, 'match_id')

# COMMAND ----------

# MAGIC %run ./notebooks/_pipeline_execution/run_matching_pipeline

# COMMAND ----------

PIPELINE_CONFIG = [
  { # Raw data inputs. Must be the zeroth stage
    'epma_table': RAW_INPUT_TABLE,
    'vtm_table': 'dss_corporate.vtm',
    'vmp_table': 'dss_corporate.vmp',
    'amp_table': 'dss_corporate.amp',
    'parsed_vtm_table': '', # parsed vtm data doesn't exist                          
    'parsed_vmp_table': 'dss_corporate.vmp_parsed',
    'parsed_amp_table': 'dss_corporate.amp_parsed'
  },
  {
    'stage_id': 'exceptions_and_preprocessing',
    'notebook_location': './notebooks/0_exceptions_and_preprocessing/drivers/exceptions_and_preprocessing_driver',
    'raw_data_required': True,
    'source_dataset': SOURCE_DATASET,
    'unmappable_table': UNMAPPABLE_TABLE,
    'output_table':PREPROCESSING_OUTPUT_TABLE, # Table to write non-match output to.
    'match_lookup_final_table': MATCH_LOOKUP_FINAL_TABLE,
    'run_id': RUN_ID,
    'batch_size': BATCH_SIZE,
    'execute': True
  },
  {
    'stage_id': 'exact_match',
    'notebook_location': './notebooks/1_exact_match/drivers/exact_match_driver',
    'raw_data_required': True,
    'input_table': PREPROCESSING_OUTPUT_TABLE,               # Table to read previous stage non-match input from.
    'output_table': EXACT_MATCH_OUTPUT_TABLE ,               # Table to write stage non-match output to.
    'match_table':EXACT_MATCH_MATCH_TABLE ,                   # Table to write stage matched output to.
    'execute': True
  },
  {
    'stage_id': 'entity_matching',
    'notebook_location': './notebooks/2_entity_extraction/drivers/entity_extraction_driver',
    'raw_data_required': True, 
    'input_table':EXACT_MATCH_OUTPUT_TABLE,    # Table to read previous stage non-match input from.
    'match_table':ENTITY_MATCH_MATCH_TABLE,    # Table to write matched output to.
    'output_table':ENTITY_MATCH_OUTPUT_TABLE,
    'unmappable_table': UNMAPPABLE_TABLE,
    'run_id': RUN_ID,
    'execute': True
  },
  {
    'stage_id': 'fuzzy_matching',
    'notebook_location': './notebooks/3_fuzzy_matching/drivers/fuzzy_match_driver',
    'raw_data_required': True,
    'input_table':ENTITY_MATCH_OUTPUT_TABLE,    # Table to read previous stage non-match input from
    'output_table': FUZZY_MATCH_OUPUT_TABLE,
    'match_lookup_final_table': MATCH_LOOKUP_FINAL_TABLE,  
    'unmappable_table': UNMAPPABLE_TABLE,
    'fuzzy_non_linked': FUZZY_NONLINKED_TABLE,
    'fuzzy_nonlinked_non_match_output':FUZZY_NON_LINKED_NON_MATCH_TABLE,
    'match_lookup_final_version': MATCH_LOOKUP_FINAL_VERSION,
    'run_id': RUN_ID,
    'execute': True
  },
  {
    'stage_id': 'accuracy_calculating',
    'notebook_location': './notebooks/4_accuracy_calculating/drivers/accuracy-calculating-driver',
    'raw_data_required': True,
    'input_table': MATCH_LOOKUP_FINAL_TABLE, # Table to read previous stage non-match input from
    'output_table': ACCURACY_TABLE,
    'ground_truth_table': GROUND_TRUTH_TABLE,
    'execute': True
  }
]

# COMMAND ----------

run_matching_pipeline(PIPELINE_CONFIG)