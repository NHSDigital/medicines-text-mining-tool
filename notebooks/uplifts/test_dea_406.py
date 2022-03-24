# Databricks notebook source
# MAGIC %run ./dea_406

# COMMAND ----------

# MAGIC %run ../_modules/epma_global/function_test_suite

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, LongType

dbutils.widgets.text('db', 'epma_autocoding', 'db')
DB = dbutils.widgets.get('db')
assert DB

# COMMAND ----------

suite = FunctionTestSuite()

@suite.add_test
def test_add_fields_to_existing_tables():
  df_input = spark.createDataFrame([('vtmid3', 'Urokinase 10,000'), ('vtmid4', 'Vancomycin powder'),], ['_id', 'text_col'])

  asset_name = f'{DB}.test_add_field_to_existing_tables'
  create_table(df_input, asset_name, overwrite=True)
  add_field_to_existing_table(DB, 'test_add_field_to_existing_tables')
  
  schema_match_lookup_final = StructType([
    StructField('_id', StringType(), True),
    StructField('text_col', StringType(), True),  
    StructField('run_id', StringType(), True),
  ])
  assert schema_match_lookup_final==spark.table(asset_name).schema
  
  drop_table(DB, 'test_add_field_to_existing_tables')

suite.run()