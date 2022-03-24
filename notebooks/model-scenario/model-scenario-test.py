# Databricks notebook source
# MAGIC %run ../../notebooks/_modules/epma_global/FunctionTestSuite

# COMMAND ----------

from pyspark.sql.functions import col,when,lit
import pyspark.sql.functions as F
from dsp.validation.validator import compare_results
from pyspark.sql.types import *

# COMMAND ----------

suite = FunctionTestSuite()

# COMMAND ----------

@suite.add_test
def test_device():

  df_example = spark.createDataFrame([
    ('101', 'omalizumab 75mg', '401', 'VTMID', 'fuzzy_matching'),
    ('102', 'pomalidomide 3 mg capsules', '402', 'VPID', 'entity'),
    ('103', 'velphoro 500mg chewable tablets', '403', 'APID', 'exact_by_name')
  ],
    ['epma_id', 'epma_description', 'match_id', 'id_level', 'match_level']) 

  df_invalid = spark.createDataFrame([
    ('401', 1),
    ('402', None),
    ('403', 1)
  ],
    StructType([
      StructField('match_id', StringType()), \
      StructField('INVALID', IntegerType())
    ])
  )

  df_example_invalid = df_example.join(df_invalid, 'match_id', 'inner') \
  .filter(col('INVALID')==1)

  df_example_invalid_report = df_example_invalid.select('epma_id', 'epma_description', 'match_id', 'id_level', 'match_level', 'INVALID')

  df_expected = spark.createDataFrame([
    ('101', 'omalizumab 75mg', '401', 'VTMID', 'fuzzy_matching'),
    ('103', 'velphoro 500mg chewable tablets', '403', 'APID', 'exact_by_name')
  ],
    ['epma_id', 'epma_description', 'match_id', 'id_level', 'match_level']) \
  .withColumn('INVALID', F.lit(1))
  
  assert compare_results(df_example_invalid_report, df_expected, join_columns=['epma_id'])

# COMMAND ----------

@suite.add_test
def test_device():

  df_dss_prev = spark.createDataFrame([
    ('401', '501'),
    ('403', '503'),
    ('408', '508')
  ],
    ['match_id', 'match_id_update'])

  df_lookup = spark.createDataFrame([
      ('101', 'omalizumab 75mg', '401', 'VTMID', 'fuzzy_matching'),
      ('102', 'pomalidomide 3 mg capsules', '402', 'VPID', 'entity'),
      ('103', 'velphoro 500mg chewable tablets', '403', 'APID', 'exact_by_name')
    ],
      ['epma_id', 'epma_description', 'match_id', 'id_level', 'match_level']) 

  df_lookup_dss_prev_update = df_lookup.join(df_dss_prev, 'match_id', 'inner') \
  .withColumn('match_id', F.col('match_id_update')) \
  .drop('match_id_update')


  df_expected = spark.createDataFrame([
      ('101', 'omalizumab 75mg', '501', 'VTMID', 'fuzzy_matching'),
      ('103', 'velphoro 500mg chewable tablets', '503', 'APID', 'exact_by_name')
    ],
      ['epma_id', 'epma_description', 'match_id', 'id_level', 'match_level']) 
  
  assert compare_results(df_lookup_dss_prev_update, df_expected, join_columns=['epma_id'])

# COMMAND ----------

suite.run() 