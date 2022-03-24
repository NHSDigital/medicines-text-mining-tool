# Databricks notebook source
dbutils.widgets.text('db', '', 'db')
DB = dbutils.widgets.get('db')
assert DB

dbutils.widgets.text('uplift_notebook', '/notebooks/uplifts/dea_417', 'uplift_notebook')
UPLIFT_NOTEBOOK = dbutils.widgets.get('uplift_notebook')

dbutils.widgets.text('uplift_table', 'match_lookup_final', 'uplift_table')
UPLIFT_TABLE = dbutils.widgets.get('uplift_table')

dbutils.widgets.text('match_lookup_final_table_name', 'match_lookup_final', 'match_lookup_final_table_name')
MATCH_LOOKUP_FINAL_TABLE_NAME = dbutils.widgets.get('match_lookup_final_table_name')
assert MATCH_LOOKUP_FINAL_TABLE_NAME

dbutils.widgets.text('unmappable_table_name', 'unmappable', 'unmappable_table_name')
UNMAPPABLE_TABLE_NAME = dbutils.widgets.get('unmappable_table_name')
assert UNMAPPABLE_TABLE_NAME

dbutils.widgets.text('accuracy_table_name', 'accuracy', 'accuracy_table_name')
ACCURACY_TABLE_NAME = dbutils.widgets.get('accuracy_table_name')
assert ACCURACY_TABLE_NAME

# COMMAND ----------

# MAGIC %run ./notebooks/_modules/epma_global/functions

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, LongType

# COMMAND ----------

OVERWRITE = True if DB == 'test_epma_autocoding' else False

# COMMAND ----------

if UPLIFT_NOTEBOOK != '':
  dbutils.notebook.run('.' + UPLIFT_NOTEBOOK, 0, {'db': DB, 
                                                  'uplift_table_name': DB + '.' + UPLIFT_TABLE,
                                                  'match_lookup_final_table_name': MATCH_LOOKUP_FINAL_TABLE_NAME,
                                                  'unmappable_table_name': UNMAPPABLE_TABLE_NAME,
                                                  'accuracy_table_name': ACCURACY_TABLE_NAME})

# COMMAND ----------

schema_match_lookup_final = StructType([
  StructField('original_epma_description', StringType(), True, {'comment': 'Input description'}),
  StructField('form_in_text', StringType()),
  StructField('match_id', StringType(), True, {'comment': 'ID of matched dm+d record'}),
  StructField('match_term', StringType(), True),
  StructField('id_level', StringType(), True, {'comment': 'Level of dm+d match ie VMP, AMP, or VPID'}),
  StructField('match_level', StringType(), True),
  StructField('match_datetime', TimestampType(), True, {'comment': 'Date and time matched'}),
  StructField('version_id', StringType(), True),
  StructField('run_id', StringType(), True),
])

create_table_from_schema(schema_match_lookup_final, DB, table=MATCH_LOOKUP_FINAL_TABLE_NAME, overwrite=OVERWRITE)

# COMMAND ----------

schema_unmappable = StructType([
  StructField('original_epma_description', StringType(), True, {'comment': 'Input description'}),
  StructField('form_in_text', StringType()),
  StructField('reason', StringType(), True),
  StructField('match_datetime', TimestampType(), True),
  StructField('run_id', StringType(), True),
])

create_table_from_schema(schema_unmappable, DB, table=UNMAPPABLE_TABLE_NAME, overwrite=OVERWRITE)

# COMMAND ----------

schema_accuracy = StructType([
  StructField('pipeline_match_id_level', StringType(), True, {'comment': 'Pipeline match id level'}),
  StructField('pipeline_match_level', StringType(), True, {'comment': 'Pipeline match level'}),
  StructField('source_match_id_level', StringType(), True, {'comment': 'Source match id level'}),
  StructField('pipeline_mismatch', StringType(), True, {'comment': 'Reason for pipeline mismatch'}),
  StructField('total_match_count', LongType(), True, {'comment': 'Total match count'}),
  StructField('match_datetime', TimestampType(), False, {'comment': 'Date and time matched'}),
  StructField('run_id', StringType(), False)
])

create_table_from_schema(schema_accuracy, DB, table=ACCURACY_TABLE_NAME, overwrite=OVERWRITE, allow_nullable_schema_mismatch=True)