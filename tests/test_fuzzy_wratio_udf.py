# Databricks notebook source
# MAGIC %run ../notebooks/3_fuzzy_matching/functions/fuzzy_match_functions

# COMMAND ----------

# MAGIC %run ../notebooks/_modules/epma_global/test_helpers

# COMMAND ----------

# MAGIC %run ../notebooks/_modules/epma_global/function_test_suite

# COMMAND ----------

suite = FunctionTestSuite()

# COMMAND ----------

@suite.add_test
def test_fuzzy_wratio_udf():
  df_input = spark.createDataFrame([
    ('0', 'abc', 'abc'),
    ('1', 'abc', 'def'),
    ('2', 'rjk', 'lmn'),
  ], ['index', 'value', 'match'])
  
  df_expected = spark.createDataFrame([
    ('0', 'abc', 'abc', 25),
    ('1', 'abc', 'def', 50),
    ('2', 'rjk', 'lmn', 75),
  ], ['index', 'value', 'match', 'score'])

  def mock_wratio(src_text, match_text):
    if src_text == 'abc' and match_text == 'abc':
      return 25
    elif src_text == 'abc' and match_text == 'def':
      return 50
    elif src_text == 'rjk' and match_text == 'lmn':
      return 75
  
  with FunctionPatch('WRatio', mock_wratio):
    df_actual = df_input.withColumn('score', fuzzy_wratio_udf(col('value'), col('match')))
    assert compare_results(df_actual, df_expected, join_columns=['index'])

# COMMAND ----------

suite.run()