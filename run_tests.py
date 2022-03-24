# Databricks notebook source
dbutils.widgets.text('db', 'test_epma_autocoding', 'Test Database.')
DB = dbutils.widgets.get('db')
assert DB

# COMMAND ----------

import os

# COMMAND ----------

ENV = os.environ['env']

# COMMAND ----------

dbutils.notebook.run("./tests/test_functions", 0, {'db': DB})
dbutils.notebook.run("./tests/test_test_helpers", 0, {'db': DB})
dbutils.notebook.run("./tests/test_ref_data_lib", 0, {'db': DB})
dbutils.notebook.run("./tests/test_pipeline_exec", 0, {'db': DB})
dbutils.notebook.run("./tests/test_exceptions_and_preprocessing", 0, {'db': DB})
dbutils.notebook.run("./tests/test_exact_match", 0, {'db': DB})
dbutils.notebook.run("./tests/test_entity_extraction", 0, {'db': DB})

# The merge doesn't use the correct type of cluster needed by pandas UDFs, so skip them.
if ENV == 'ref':
  dbutils.notebook.run("./tests/test_fuzzy_matching", 0, {'db': DB})
  dbutils.notebook.run("./tests/test_fuzzy_wratio_udf", 0, {'db': DB})

dbutils.notebook.run("./tests/test_accuracy_calculating", 0, {'db': DB})