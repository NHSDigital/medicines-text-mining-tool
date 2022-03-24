# Databricks notebook source
dbutils.widgets.text('db', 'test_epma_autocoding', 'Test Database.')
DB = dbutils.widgets.get('db')
assert DB

# COMMAND ----------

from uuid import uuid4
import warnings

from pyspark.sql.functions import col, lit
from pyspark.sql.types import *
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run ../notebooks/_modules/epma_global/function_test_suite

# COMMAND ----------

# MAGIC %run ../notebooks/_modules/epma_global/functions

# COMMAND ----------

# MAGIC %run ../notebooks/_modules/epma_global/test_helpers

# COMMAND ----------

suite = FunctionTestSuite()

# COMMAND ----------

@suite.add_test
def test_get_new_run_id():
  df_expected = spark.createDataFrame([
    ('0', 'v1', '1'), 
    ('1', 'v2', None),
    ('2', 'v3', '43'),
    ('3', 'v4', '2'),
  ], ['index', 'name', 'run_id'])
  
  df_nulls = spark.createDataFrame([
    ('0', 'v1', None), 
    ('1', 'v2', None),
  ], StructType([
    StructField('index', StringType(), False), 
    StructField('name', StringType(), False),
    StructField('run_id', StringType(), True),
  ])
  )
  
  with TemporaryTable(df_expected, db=DB) as tmp_table:
    asset_name = f'{tmp_table.db}.{tmp_table.name}'
    assert get_new_run_id(asset_name, "run_id") == 44

  with TemporaryTable(df_nulls, db=DB) as tmp_table:
    asset_name = f'{tmp_table.db}.{tmp_table.name}'
    assert get_new_run_id(asset_name, "run_id") == 1

# COMMAND ----------

@suite.add_test
def test_find_git_hash_regex():
  test_cases_positive = {
    "notebooks/1961+20210809165643.gitfeb8c2ed/run_notebooks": "1961+20210809165643.gitfeb8c2ed",
  }

  test_cases_negative = ["run_notebooks"]
  
  for input, expected in test_cases_positive.items():
    assert find_git_hash_regex(input) == expected
  
  for input in test_cases_negative:
    assert not find_git_hash_regex(input)

# COMMAND ----------

@suite.add_test
def test_get_data():
  df_input = spark.createDataFrame([('0', 'v1'), ('1', 'v2'),], ['index', 'name'])

  with TemporaryTable(df_input, db=DB) as tmp_table:
    df_actual = get_data(tmp_table.db, tmp_table.name)
    
    assert compare_results(df_actual, df_input, join_columns=['index'])
 

@suite.add_test
def test_get_data_asset_name():
  df_input = spark.createDataFrame([('0', 'v1'), ('1', 'v2'),], ['index', 'name'])

  with TemporaryTable(df_input, db=DB) as tmp_table:
    df_actual = get_data(f'{tmp_table.db}.{tmp_table.name}')
    
    assert compare_results(df_actual, df_input, join_columns=['index'])
  

# COMMAND ----------

@suite.add_test
def test_flatten_struct_columns():

  nested_schema = StructType([
      StructField('index', StringType(), True),
      StructField('detail', StructType([
        StructField('name', StringType(), True),
        StructField('city', StringType(), True),
      ])),
    ])

  df_nested = spark.createDataFrame([
    ('0', {'name': 'abc', 'city': 'Lon'}), 
    ('1', {'name': 'efg', 'city': 'Man'})
  ], nested_schema)

  df_actual = flatten_struct_columns(df_nested)

  expected_schema = StructType([
      StructField('index', StringType(), True),
      StructField('detail_name', StringType(), False),
      StructField('detail_city', StringType(), False)
      ])

  df_expected = spark.createDataFrame([
    ('0', 'abc', 'Lon'), 
    ('1', 'efg', 'Man')], expected_schema)

  assert check_schemas_match(df_actual, df_expected, True)
  
  df_nested = spark.createDataFrame([
    ('0', 'abc', 'Lon'), 
    ('1', 'efg', 'Man')], 
    ['index', 'name', 'city'])

  df_actual = flatten_struct_columns(df_nested)

  df_expected = spark.createDataFrame([
    ('0', 'abc', 'Lon'), 
    ('1', 'efg', 'Man')], 
    ['index', 'name', 'city'])
  
  assert check_schemas_match(df_actual, df_expected)  
  

# COMMAND ----------

@suite.add_test
def test_check_schemas_match():
  dfa = spark.createDataFrame([('0', 'v1'), ('1', 'v2'),], ['index', 'name'])
  dfb = spark.createDataFrame([('2', 'v3'), ('3', 'v4'),], ['index', 'name'])
  
  assert check_schemas_match(dfa, dfb)
  
  dfa = spark.createDataFrame([('0', 'v1'), ('1', 'v2'),], ['index', 'name'])
  dfb = spark.createDataFrame([('1', 'v3'), ('3', 'v4'),], ['index', 'name'])
  
  assert check_schemas_match(dfa, dfb)  
  

@suite.add_test
def test_check_schemas_match_name_mismatch():
  
  dfa = spark.createDataFrame([('0', 'v1'), ('1', 'v2'),], ['index', 'name'])
  dfb = spark.createDataFrame([('2', 'v3'), ('3', 'v4'),], ['index', 'name2'])
  
  assert check_schemas_match(dfa, dfb) is False

  
@suite.add_test
def test_check_schemas_match_type_mismatch():
  
  dfa = spark.createDataFrame([('0', '1'), ('1', '2'),], ['index', 'name'])
  dfb = spark.createDataFrame([('2', 3), ('3', 4),], ['index', 'name'])

  assert check_schemas_match(dfa, dfb) is False
  

@suite.add_test
def test_check_schemas_match_nullable_mismatch():
  a_schema = StructType([
    StructField('index', StringType(), True),
    StructField('name', StringType(), True),
  ])
  b_schema = StructType([
    StructField('index', StringType(), True),
    StructField('name', StringType(), False),
  ])
  
  dfa = spark.createDataFrame([('0', 1), ('1', 2),], a_schema)
  dfb = spark.createDataFrame([('2', 3), ('3', 4),], b_schema)

  assert check_schemas_match(dfa, dfb) is False
  assert check_schemas_match(dfa, dfb, allow_nullable_schema_mismatch=True) is True


# COMMAND ----------

@suite.add_test
def test_append_to_table():
  df_existing = spark.createDataFrame([('0', 'v1'), ('1', 'v2'),], ['index', 'name'])
  df_input = spark.createDataFrame([('2', 'v3'), ('3', 'v4'),], ['index', 'name'])

  df_expected = spark.createDataFrame([
    ('0', 'v1'), 
    ('1', 'v2'),
    ('2', 'v3'),
    ('3', 'v4'),
  ], ['index', 'name'])
  
  with TemporaryTable(df_existing, db=DB) as tmp_table:
    asset_name = f'{tmp_table.db}.{tmp_table.name}'
    append_to_table(df_input, ['index'], asset_name)
    df_actual = spark.table(asset_name)
    
    assert compare_results(df_actual, df_expected, join_columns=['index'])
    

@suite.add_test
def test_append_to_table_id_match():
  df_existing = spark.createDataFrame([('0', 'v1'), ('1', 'v2'),], ['index', 'name'])
  df_input = spark.createDataFrame([('1', 'v3'), ('3', 'v4'),], ['index', 'name'])

  with TemporaryTable(df_existing, db=DB) as tmp_table:
    try:
      append_to_table(df_input, ['index'], f'{tmp_table.db}.{tmp_table.name}')
      raise Exception
    except AssertionError:
      assert True

      
@suite.add_test
def test_append_to_table_schema_mismatch_name():
  df_existing = spark.createDataFrame([('0', 'v1'), ('1', 'v2'),], ['index', 'name'])
  df_input = spark.createDataFrame([('2', 'v3'), ('3', 'v4'),], ['index', 'name2'])

  with TemporaryTable(df_existing, db=DB) as tmp_table:
    try:
      append_to_table(df_input, ['index'], f'{tmp_table.db}.{tmp_table.name}')
      raise Exception
    except AssertionError:
      assert True
      

@suite.add_test
def test_append_to_table_schema_mismatch_type():
  df_existing = spark.createDataFrame([('0', '1'), ('1', '2'),], ['index', 'name'])
  df_input = spark.createDataFrame([('2', 3), ('3', 4),], ['index', 'name'])

  with TemporaryTable(df_existing, db=DB) as tmp_table:
    try:
      append_to_table(df_input, 'index', f'{tmp_table.db}.{tmp_table.name}')
      raise Exception
    except AssertionError:
      assert True
      

@suite.add_test
def test_append_to_table_schema_mismatch_nullable():
  existing_schema = StructType([
    StructField('index', StringType(), True),
    StructField('name', StringType(), True),
  ])
  input_schema = StructType([
    StructField('index', StringType(), True),
    StructField('name', StringType(), False),
  ])
  
  df_existing = spark.createDataFrame([('0', 1), ('1', 2),], existing_schema)
  df_input = spark.createDataFrame([('2', 3), ('3', 4),], input_schema)

  with TemporaryTable(df_existing, db=DB) as tmp_table:
    try:
      append_to_table(df_input, 'index', f'{tmp_table.db}.{tmp_table.name}', allow_nullable_schema_mismatch=False)
      raise Exception
    except AssertionError:
      assert True

@suite.add_test
def test_append_to_table_on_two_cols():
  df_existing = spark.createDataFrame([('0', 'a', 'v1'), ('1', 'a', 'v2'),], ['index1', 'index2', 'name'])
  df_input = spark.createDataFrame([('2', 'a', 'v3'), ('0', 'b', 'v4'),], ['index1', 'index2', 'name'])
  
  df_expected = spark.createDataFrame([
    ('0', 'a', 'v1'), 
    ('1', 'a', 'v2'),
    ('2', 'a', 'v3'),
    ('0', 'b', 'v4'),
  ], ['index1', 'index2', 'name'])
  
  with TemporaryTable(df_existing, db=DB) as tmp_table:
    asset_name = f'{tmp_table.db}.{tmp_table.name}'
    append_to_table(df_input, ['index1', 'index2'], asset_name)
    df_actual = spark.table(asset_name)
    
    assert compare_results(df_actual, df_expected, join_columns=['index1', 'index2'])

    
@suite.add_test
def test_append_to_table_on_two_cols_id_match():
  df_existing = spark.createDataFrame([('0', 'a', 'v1'), ('1', 'a', 'v2'),], ['index1', 'index2', 'name'])
  df_input = spark.createDataFrame([('0', 'a', 'v3'), ('0', 'b', 'v4'),], ['index1', 'index2', 'name'])
    
  with TemporaryTable(df_existing, db=DB) as tmp_table:
    try:
      append_to_table(df_input, ['index1', 'index2'], f'{tmp_table.db}.{tmp_table.name}')
      raise Exception
    except AssertionError:
      assert True  

# COMMAND ----------

@suite.add_test
def test_df_checkpoint_context_manager():
  df_input = spark.createDataFrame([(1, 2,)], ['v1', 'v2'])
 
  assert df_input.storageLevel.useMemory is False
 
  with DFCheckpoint(df_input) as df_cp:
    assert df_cp._df_before.storageLevel.useMemory is True
    assert df_cp.df.storageLevel.useMemory is True
    df_after = df_cp.df
 
  assert df_after.storageLevel.useMemory is False

  
@suite.add_test
def test_df_checkpoint_del():
  df_input = spark.createDataFrame([(1, 2,)], ['v1', 'v2'])

  assert df_input.storageLevel.useMemory is False

  df_cp = DFCheckpoint(df_input)

  assert df_cp._df_before.storageLevel.useMemory is True
  assert df_cp.df.storageLevel.useMemory is True
  df_after = df_cp.df

  del df_cp

  assert df_after.storageLevel.useMemory is False


# COMMAND ----------

@suite.add_test
def test_table_exists():
  df_input = spark.createDataFrame([(1, 2,)], ['v1', 'v2'])
  input_name = f'_tmp_{uuid4().hex}'
  df_input.createOrReplaceGlobalTempView(input_name)
  
  assert table_exists('global_temp', input_name)
  assert table_exists('global_temp', f'_tmp_{uuid4().hex}') is False

  
@suite.add_test
def tests_drop_table():
  df_input = spark.createDataFrame([(1, 2,)], ['v1', 'v2'])
  input_name = f'_tmp_{uuid4().hex}'
  df_input.createOrReplaceGlobalTempView(input_name)
 
  assert table_exists('global_temp', input_name)
 
  drop_table('global_temp', input_name)
 
  assert table_exists('global_temp', input_name) is False
  
  
@suite.add_test
def test_create_table():
  input_name = f'_tmp_{uuid4().hex}'
  asset_name = f'{DB}.{input_name}'
  df_input = spark.createDataFrame([(1, 2,)], ['v1', 'v2'])
 
  try:
    create_table(df_input, DB, input_name, overwrite=True)
    assert table_exists(DB, input_name)
  
  finally:
    drop_table(DB, input_name)
  
  
@suite.add_test
def test_create_table_from_schema():
  input_name = f'_tmp_{uuid4().hex}'
  asset_name = f'{DB}.{input_name}'
  
  input_schema = StructType([
    StructField('index', IntegerType()),
    StructField('name', StringType()),
    StructField('meta', StructType([
      StructField('id', IntegerType())
    ]))
  ])
 
  try:
    create_table_from_schema(input_schema, DB, table=input_name)
    assert table_exists(DB, input_name)
    assert spark.table(asset_name).schema == input_schema
  
  finally:
    drop_table(DB, input_name)

    
@suite.add_test
def test_create_table_from_schema_overwrite():
  input_name = f'_tmp_{uuid4().hex}'

  existing_schema = StructType([
    StructField('index', IntegerType()),
  ])
  
  input_schema = StructType([
    StructField('index', IntegerType()),
    StructField('name', StringType()),
    StructField('meta', StructType([
      StructField('id', IntegerType())
    ]))
  ])
 
  try:
    create_table_from_schema(existing_schema, DB, table=input_name)
    assert table_exists(DB, input_name)  
    create_table_from_schema(input_schema, DB, table=input_name, overwrite=True)
    assert table_exists(DB, input_name)  
    assert spark.table(f'{DB}.{input_name}').schema == input_schema
    
  finally:
    drop_table(DB, input_name)
    
    
@suite.add_test
def test_create_table_from_schema_overwrite_false():
  input_name = f'_tmp_{uuid4().hex}'

  existing_schema = StructType([
    StructField('index', IntegerType()),
  ])
  
  input_schema = StructType([
    StructField('index', IntegerType()),
    StructField('name', StringType()),
    StructField('meta', StructType([
      StructField('id', IntegerType())
    ]))
  ])
 
  try:
    create_table_from_schema(existing_schema, DB, table=input_name)
    assert table_exists(DB, input_name)  
    try: 
      create_table_from_schema(input_schema, DB, table=input_name)
      raise Exception
    except AssertionError:
      assert True
    
  finally:
    drop_table(DB, input_name)

    
@suite.add_test
def test_create_table_from_schema_asset_name():
  input_name = f'_tmp_{uuid4().hex}'
  asset_name = f'{DB}.{input_name}'
  
  input_schema = StructType([
    StructField('index', IntegerType()),
    StructField('name', StringType()),
    StructField('meta', StructType([
      StructField('id', IntegerType())
    ]))
  ])
 
  try:
    create_table_from_schema(input_schema, asset_name)
    assert table_exists(DB, input_name)
    assert spark.table(asset_name).schema == input_schema
  
  finally:
    drop_table(DB, input_name)

    
@suite.add_test
def test_create_table_from_schema_nullable():
  input_name = f'_tmp_{uuid4().hex}'
  asset_name = f'{DB}.{input_name}'
  
  existing_schema = StructType([
    StructField('index', StringType(), True),
    StructField('name', StringType(), True),
  ])
  input_schema = StructType([
    StructField('index', StringType(), True),
    StructField('name', StringType(), False),
  ])
  
  create_table_from_schema(existing_schema, DB, table=input_name)
  assert table_exists(DB, input_name)  
  try:
    create_table_from_schema(input_schema, DB, table=input_name, allow_nullable_schema_mismatch=False)
    raise Exception
  except AssertionError:
    assert True
      
  finally:
    drop_table(DB, input_name)

# COMMAND ----------

@suite.add_test
def test_check_content_match():
  
  schema = StructType([
    StructField('index', IntegerType(), True),
    StructField('indexs', ShortType(), True),
    StructField('indexl', LongType(), True),
    StructField('name', StringType(), True),
    StructField('value', FloatType(), True),
  ])
  
  df1 = spark.createDataFrame([
    (0, 0, 0, 'a', 0.0),
    (1, 1, 1, 'b', 0.1),
  ], schema)
  
  df2 = spark.createDataFrame([
    (0, 0, 0, 'a', 0.0),
    (1, 1, 1, 'b', 0.1),
  ], schema)
  
  assert _check_content_match(df1, df2, join_col=['index'])
  
  
@suite.add_test
def test_check_content_match_mismatch():
  
  schema = StructType([
    StructField('index', IntegerType(), True),
    StructField('indexs', ShortType(), True),
    StructField('indexl', LongType(), True),
    StructField('name', StringType(), True),
    StructField('value', FloatType(), True),
  ])
  
  df1 = spark.createDataFrame([
    (0, 0, 0, 'a', 0.0),
    (1, 1, 1, 'b', 0.1),
  ], schema)
  
  df2 = spark.createDataFrame([
    (0, 0, 0, 'a', 0.0),
    (1, 2, 1, 'b', 0.1),
  ], schema)
  
  assert _check_content_match(df1, df2, join_col=['index']) is False
  
@suite.add_test
def test_check_content_match_struct():
  
  schema = StructType([
    StructField('index', IntegerType(), True),
    StructField('indexs', ShortType(), True),
    StructField('indexl', LongType(), True),
    StructField('name', StringType(), True),
    StructField('value', FloatType(), True),
    StructField('nested', StructType([
      StructField('item', StringType(), True),
      StructField('score', FloatType(), True),
    ]), True),
    
  ])
  
  df1 = spark.createDataFrame([
    (0, 0, 0, 'a', 0.0, {'item': 'A', 'score': 0.0}),
    (1, 1, 1, 'b', 0.1, {'item': 'B', 'score': 0.11}),
  ], schema)
  
  df2 = spark.createDataFrame([
    (0, 0, 0, 'a', 0.0, {'item': 'A', 'score': 0.0}),
    (1, 1, 1, 'b', 0.1, {'item': 'B', 'score': 0.11}),
  ], schema)
  
  assert _check_content_match(df1, df2, join_col=['index'])

  
@suite.add_test
def test_check_content_match_struct_mismatch():
  
  schema = StructType([
    StructField('index', IntegerType(), True),
    StructField('indexs', ShortType(), True),
    StructField('indexl', LongType(), True),
    StructField('name', StringType(), True),
    StructField('value', FloatType(), True),
    StructField('nested', StructType([
      StructField('item', StringType(), True),
      StructField('score', FloatType(), True),
    ]), True),
    
  ])
  
  df1 = spark.createDataFrame([
    (0, 0, 0, 'a', 0.0, {'item': 'A', 'score': 0.0}),
    (1, 1, 1, 'b', 0.1, {'item': 'B', 'score': 0.11}),
  ], schema)
  
  df2 = spark.createDataFrame([
    (0, 0, 0, 'a', 0.0, {'item': 'A', 'score': 0.0}),
    (1, 1, 1, 'b', 0.1, {'item': 'B', 'score': 0.22}),
  ], schema)
  
  assert _check_content_match(df1, df2, join_col=['index']) is False
  

@suite.add_test
def test_check_content_match_two_joins():
  
  schema = StructType([
    StructField('index', IntegerType(), True),
    StructField('indexs', ShortType(), True),
    StructField('indexl', LongType(), True),
    StructField('name', StringType(), True),
    StructField('value', FloatType(), True),
  ])
  
  df1 = spark.createDataFrame([
    (0, 0, 0, 'a', 0.0),
    (1, 1, 1, 'b', 0.1),
  ], schema)
  
  df2 = spark.createDataFrame([
    (0, 0, 0, 'a', 0.0),
    (1, 1, 1, 'b', 0.1),
  ], schema)
  
  assert _check_content_match(df1, df2, join_col=['index', 'indexs'])
  

@suite.add_test
def test_check_content_match_two_joins_mismatch():
  
  schema = StructType([
    StructField('index', IntegerType(), True),
    StructField('indexs', ShortType(), True),
    StructField('indexl', LongType(), True),
    StructField('name', StringType(), True),
    StructField('value', FloatType(), True),
  ])
  
  df1 = spark.createDataFrame([
    (0, 0, 0, 'a', 0.0),
    (1, 1, 1, 'b', 0.1),
  ], schema)
  
  df2 = spark.createDataFrame([
    (0, 0, 0, 'a', 0.0),
    (1, 1, 2, 'b', 0.1),
  ], schema)
  
  assert _check_content_match(df1, df2, join_col=['index', 'indexs']) is False
  
@suite.add_test
def test_check_content_match_join_mismatch():
  
  schema = StructType([
    StructField('index', IntegerType(), True),
    StructField('indexs', ShortType(), True),
    StructField('indexl', LongType(), True),
    StructField('name', StringType(), True),
    StructField('value', FloatType(), True),
  ])
  
  df1 = spark.createDataFrame([
    (0, 0, 0, 'a', 0.0),
    (1, 1, 1, 'b', 0.1),
  ], schema)
  
  df2 = spark.createDataFrame([
    (0, 0, 0, 'a', 0.0),
    (2, 1, 1, 'b', 0.1),
  ], schema)
  
  assert _check_content_match(df1, df2, join_col=['index']) is False
  
@suite.add_test
def test_check_content_match_df1_dup():

  schema = StructType([
    StructField('index', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('value', FloatType(), True),
  ])
  
  df1 = spark.createDataFrame([
    (0, 'a', 0.0),
    (0, 'a', 0.0),
    (1, 'b', 0.1),
  ], schema)
  
  df2 = spark.createDataFrame([
    (0, 'a', 0.0),
    (1, 'b', 0.1),
  ], schema)

  assert _check_content_match(df1, df2, join_col=['index']) is False
  
@suite.add_test
def test_check_content_match_df1_dup_df2_extra():
  
  df1_schema = StructType([
    StructField('index', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('value', FloatType(), True),
  ])

  df1 = spark.createDataFrame([
    (0, 'a', 0.0),
    (0, 'a', 0.0),
  ], df1_schema)

  df2_schema = StructType([
    StructField('index', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('value', FloatType(), True),
    StructField('extra', StringType(), True),
  ])

  df2 = spark.createDataFrame([
    (0, 'a', 0.0, 'A'),
  ], df2_schema)

  assert _check_content_match(df1, df2, join_col=['index']) is False

@suite.add_test
def test_check_content_match_df2_extra_col():
  
  df1_schema = StructType([
    StructField('index', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('value', FloatType(), True),
  ])
  
  df1 = spark.createDataFrame([
    (0, 'a', 0.0),
    (1, 'b', 0.1),
  ], df1_schema)

  df2_schema = StructType([
    StructField('index', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('value', FloatType(), True),
    StructField('extra', StringType(), True),
  ])
  
  df2 = spark.createDataFrame([
    (0, 'a', 0.0, 'A'),
    (1, 'b', 0.1, 'B'),
  ], df2_schema)
  
  # This function should allow extra columns.
  assert _check_content_match(df1, df2, join_col=['index'])

  
@suite.add_test
def test_check_content_match_df1_extra_col():
  
  df1_schema = StructType([
    StructField('index', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('value', FloatType(), True),
    StructField('extra', StringType(), True),
  ])
  
  df1 = spark.createDataFrame([
    (0, 'a', 0.0, 'A'),
    (1, 'b', 0.1, 'B'),
  ], df1_schema)

  df2_schema = StructType([
    StructField('index', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('value', FloatType(), True),
  ])
  
  df2 = spark.createDataFrame([
    (0, 'a', 0.0),
    (1, 'b', 0.1),
  ], df2_schema)

  # This function should allow extra columns.
  assert _check_content_match(df1, df2, join_col=['index'])
  
@suite.add_test
def test_check_content_match_nulls():
  
  schema = StructType([
    StructField('index', IntegerType(), True),
    StructField('indexs', ShortType(), True),
    StructField('indexl', LongType(), True),
    StructField('name', StringType(), True),
    StructField('value', FloatType(), True),
  ])
  
  df1 = spark.createDataFrame([
    (0, 0, 0, 'a', 0.0),
    (1, 1, 1, 'b', None),
  ], schema)
  
  df2 = spark.createDataFrame([
    (0, 0, 0, 'a', 0.0),
    (1, 1, 1, 'b', None),
  ], schema)
  
  assert _check_content_match(df1, df2, join_col=['index'])
  
  
@suite.add_test
def test_check_content_match_nulls_mismatch():
  
  schema = StructType([
    StructField('index', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('value', FloatType(), True),
  ])
  
  df1 = spark.createDataFrame([
    (0, 'a', 0.0),
    (1, 'b', None),
  ], schema)
  
  df2 = spark.createDataFrame([
    (0, 'a', 0.0),
    (1, 'b', 0.1),
  ], schema)
  
  assert _check_content_match(df1, df2, join_col=['index']) is False
  
  
@suite.add_test
def test_check_content_match_null_join():
  
  schema = StructType([
    StructField('index', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('value', FloatType(), True),
  ])
  
  df1 = spark.createDataFrame([
    (0, 'a', 0.0),
    (None, 'b', 0.1),
  ], schema)
  
  df2 = spark.createDataFrame([
    (0, 'a', 0.0),
    (None, 'b', 0.1),
  ], schema)
  
  assert _check_content_match(df1, df2, join_col=['index'])
  

@suite.add_test
def test_check_content_match_null_join_mismatch():
  
  schema = StructType([
    StructField('index', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('value', FloatType(), True),
  ])
  
  df1 = spark.createDataFrame([
    (0, 'a', 0.0),
    (None, 'b', 0.1),
  ], schema)
  
  df2 = spark.createDataFrame([
    (0, 'a', 0.0),
    (1, 'b', 0.1),
  ], schema)
  
  assert _check_content_match(df1, df2, join_col=['index']) is False


# COMMAND ----------

@suite.add_test
def test_compare_results_nullable_mismatch():

  schema1 = StructType([
    StructField('id', LongType(), True),
    StructField('city', StructType([
      StructField('name', StringType(), True),
      StructField('post', StringType(), True)
      ]))
    ])

  df1 = spark.createDataFrame([(0, {'name': 'man', 'post': 'M'})], schema1)

  schema2 = StructType([
    StructField('id', LongType(), False),
    StructField('city', StructType([
      StructField('name', StringType(), False),
      StructField('post', StringType(), False)
      ]))
    ])

  df2 = spark.createDataFrame([(0, {'name': 'man', 'post': 'M'})], schema2)
  
  assert compare_results(df1, df2, join_columns=['id'], allow_nullable_schema_mismatch=True)
  assert compare_results(df1, df2, join_columns=['id'], allow_nullable_schema_mismatch=False) is False
  
  
@suite.add_test
def test_compare_results_col_order_mismatch():

  schema1 = StructType([
    StructField('city', StructType([
      StructField('name', StringType(), True),
      StructField('post', StringType(), True)
      ])),
    StructField('id', LongType(), True),
    StructField('state', StringType(), True)
    ])

  df1 = spark.createDataFrame([({'name': 'man', 'post': 'M'}, 0, 'aa')], schema1)

  schema2 = StructType([
    StructField('state', StringType(), False),
    StructField('id', LongType(), False),
    StructField('city', StructType([
      StructField('name', StringType(), False),
      StructField('post', StringType(), False)
      ]))
    ])

  df2 = spark.createDataFrame([('aa', 0, {'name': 'man', 'post': 'M'})], schema2)
  
  assert compare_results(df1, df2, join_columns=['id'], allow_nullable_schema_mismatch=True) is False
  
  
@suite.add_test
def test_compare_results_content_mismatch():

  schema1 = StructType([
    StructField('id', LongType(), True),
    StructField('city', StructType([
      StructField('name', StringType(), True),
      StructField('post', StringType(), True)
      ]))
    ])

  df1 = spark.createDataFrame([(0, {'name': 'man', 'post': 'M'})], schema1)

  schema2 = StructType([
    StructField('id', LongType(), False),
    StructField('city', StructType([
      StructField('name', StringType(), False),
      StructField('post', StringType(), True)
      ]))
    ])

  df2 = spark.createDataFrame([
    (0, {'name': 'man', 'post': None})
  ], schema2)

  assert compare_results(df1, df2, join_columns=['id'], allow_nullable_schema_mismatch=True) is False

# COMMAND ----------

@suite.add_test
def test_append_to_table_on_two_cols_id_match_with_nulls():
  df_existing = spark.createDataFrame([('0', None, 'v1'), ('1', 'a', 'v2'),], ['index1', 'index2', 'name'])
  df_input = spark.createDataFrame([('0', None, 'v3'), ('0', 'b', 'v4'),], ['index1', 'index2', 'name'])
    
  with TemporaryTable(df_existing, db=DB) as tmp_table:
    try:
      append_to_table(df_input, ['index1', 'index2'], f'{tmp_table.db}.{tmp_table.name}')
      raise Exception
    except AssertionError:
      assert True

# COMMAND ----------

suite.run()