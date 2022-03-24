# Databricks notebook source
dbutils.widgets.text('db', 'test_epma_autocoding', 'Test Database.')
DB = dbutils.widgets.get('db')
assert DB

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType

# COMMAND ----------

# MAGIC %run ../notebooks/_modules/epma_global/function_test_suite

# COMMAND ----------

# MAGIC %run ../notebooks/_modules/epma_global/test_helpers

# COMMAND ----------

suite = FunctionTestSuite()

# COMMAND ----------

@suite.add_test
def test_temporary_table_df():
  df_input = spark.createDataFrame([(1, 2)], ['v1', 'v2'])
  with TemporaryTable(df_input, db=DB) as tmp_table:
    db = tmp_table.db
    table = tmp_table.name
    assert table_exists(db, table)
  
  assert table_exists(db, table) is False  

 
@suite.add_test
def test_temporary_table_schema():
  input_schema = StructType([
    StructField('v1', IntegerType(), True),
  ])
 
  with TemporaryTable(input_schema, db=DB) as tmp_table:
    db = tmp_table.db
    table = tmp_table.name
    assert table_exists(db, table)
  
  assert table_exists(db, table) is False  
  
  
@suite.add_test
def test_temporary_table_df_dont_create():
  df_input = spark.createDataFrame([(1, 2)], ['v1', 'v2'])
  with TemporaryTable(df_input, db=DB, create=False) as tmp_table:
    db = tmp_table.db
    table = tmp_table.name
    assert table_exists(db, table) is False
    df_input.write.saveAsTable(f'{db}.{table}')
    assert table_exists(db, table)
  
  assert table_exists(db, table) is False 

# COMMAND ----------

def test_function_patch_real_func():
  return 'real1'

@suite.add_test
def test_function_patch():
  
  assert test_function_patch_real_func() == 'real1'
  
  def mock_func():
    return 'mock1'
  
  with FunctionPatch('test_function_patch_real_func', mock_func):
    assert test_function_patch_real_func() == 'mock1' 
    
  assert test_function_patch_real_func() == 'real1'

# COMMAND ----------

suite.run()