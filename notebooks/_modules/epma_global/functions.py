# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, IntegerType
from typing import List
import re

from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F

# COMMAND ----------

def get_new_run_id(asset_name: str, run_id_field: str) -> int:
  ''' Generate a new run id by adding 1 to the maximum of existing run ids.
  '''
  previous_run_id = spark.table(asset_name).select(F.max(F.col(run_id_field).cast(IntegerType()))).collect()[0][0]
  new_run_id = previous_run_id + 1 if previous_run_id is not None else 1
  return new_run_id

# COMMAND ----------

def find_git_hash_regex(string_to_search: str) -> List[str]:
  ''' Use regex to get the git hash from the notebook path. This is used as the version id.
  '''
  result = re.search(r'[a-zA-Z0-9_.+]*git[a-zA-Z0-9_.+]*', string_to_search)
  if result:
    return result[0]
  if not result:
    return None

# COMMAND ----------

def get_data(db_or_asset_name, table=None):
    ''' 
    Retrieves a table and returns a dataframe
    db_or_asset: Either the db name or the full db and table name separated by a dot.
    table: This parameter must be given if the db name is given i nthe first parameter, otherwise it defaults to None.
    
    return: DataFrame
    '''
    if table is None:
      asset = db_or_asset_name
    else:
      asset = f'{db_or_asset_name}.{table}' 
      
    spark.sql(f'REFRESH TABLE {asset}')
    return spark.table(asset)

# COMMAND ----------

def flatten_struct_columns(nested_df):
  '''
  Check_content_match doesn't work on struct columns.
  So we need to split them up into columns before we can compare the dataframes.
  '''
  stack = [((), nested_df)]
  columns = []
  while len(stack) > 0:
      parents, df = stack.pop()
      for column_name, column_type in df.dtypes:
          if column_type[:6] == "struct":
              projected_df = df.select(column_name + ".*")
              stack.append((parents + (column_name,), projected_df))
          else:
              columns.append(col(".".join(parents + (column_name,))).alias("_".join(parents + (column_name,))))
  return nested_df.select(columns)

# COMMAND ----------

def check_schemas_match(dfa: DataFrame, dfb: DataFrame, allow_nullable_schema_mismatch=False):
  '''
  Returns True if the dataframe schemas match, or False otherwise.
  
  If allow_nullable_schema_mismatch is False then the nullability of the columns must also match. If True, nullablity isn't 
  included in the check.
  '''

  if dfa.schema == dfb.schema:
    return True
  elif not allow_nullable_schema_mismatch:
    return False
  
  if len(dfa.schema) != len(dfb.schema):
    return False
  
  for a_field, b_field in zip(dfa.schema, dfb.schema):
    if a_field.name != b_field.name:
      return False
    if a_field.dataType != b_field.dataType:
      return False
    
  return True

# COMMAND ----------

def _check_content_match(df1: DataFrame,
                         df2: DataFrame,
                         join_col: List[str]
                        ) -> bool:
  '''
  Compares the values in the common columns only.
  An outer join on the given join_cols is used to decide which records to compare.
  '''
  join_condition = [df1[c].eqNullSafe(df2[c]) for c in join_col] 
  df3 = df1.alias("d1").join(df2.alias("d2"), join_condition, "outer")
  if df1.count() == df2.count():
    for name in set(df1.columns).intersection(set(df2.columns)):
      df3 = df3.withColumn(name + "_diff", F.when((col("d1." + name).isNull() & col("d2." + name).isNotNull()) | 
                                                  (col("d1." + name).isNotNull() & col("d2." + name).isNull()), 
                                                  1) \
                                            .when(col("d1." + name) != col("d2." + name), 
                                                  1) \
                                            .otherwise(0))
    col_diff = [_col for _col in df3.columns if '_diff' in _col]
    diff_sum = df3.select(col_diff).groupBy().sum().first()
    if sum(diff_sum) == 0:
      res = True
    else:
      res = False
      print('Content not match.', diff_sum)
  else:
    res = False
    print('Content not match.')
  return res


# COMMAND ----------

def compare_results(df1: DataFrame,
                    df2: DataFrame,
                    join_columns: List,
                    allow_nullable_schema_mismatch=True):
  '''
  Compare two dataframes. Used in testing to check outputs match expected outputs.
  '''  
  df1_flat = flatten_struct_columns(df1)
  df2_flat = flatten_struct_columns(df2)
  
  if check_schemas_match(df1_flat, df2_flat, allow_nullable_schema_mismatch) is True:
    print('Schema match.')
    if _check_content_match(df1_flat, df2_flat, join_columns) is True:
      print('Content match.')
      return True
    else:
      return False
  else:
    print('Schema not match.')
    return False
  

# COMMAND ----------

def append_to_table(df: DataFrame,
                    id_cols: List[str],
                    output_loc: str, 
                    allow_nullable_schema_mismatch=False
                   ) -> None:
  '''
  Append data to existing table. The df and the target table need to have the same schema.
  
  Arguments
    df: dataframe to append
    id_cols: id columns for the delta and the main table to check duplicates aren't appended
    output_loc: Combined database and table name of the asset to append to
  '''
  spark.sql(f'REFRESH TABLE {output_loc}')
  df_existing = spark.table(output_loc)
  if check_schemas_match(df_existing, df, allow_nullable_schema_mismatch=allow_nullable_schema_mismatch) is False:
    raise AssertionError(f'Given dataframe schema does not match the target schema.\nTarget: {df_existing.schema}\nSource: {df.schema}')
  
  if id_cols is not None:
    df_join = df.join(df_existing, on=list(map(lambda x: df[x].eqNullSafe(df_existing[x]), id_cols)), how='inner')
    if df_join.count() != 0:
      raise AssertionError(f'Given dataframe has values in columns {id_cols} that match values in target ({output_loc}).')
  
  df.write.mode('append').insertInto(output_loc, overwrite=False)
  spark.sql(f'REFRESH TABLE {output_loc}')

# COMMAND ----------

class DFCheckpoint():
  '''
  Class that caches a dataframe then handles unpersisting it.
  
  This probably doesn't work because of various idiosyncracies of pyspark. Namely, the underlying object ID changes numerous 
  times in the pyspark API when caching. So there's no guarantee that the object cached is the same as the object unpersisted.
  
  This can also be used as a context manager.
  '''
  
  def __init__(self, df: DataFrame, blocking_unpersist=False):
    self._df_before = df
    self._blocking_unpersist = blocking_unpersist
    self._df = df.cache()
      
  def __del__(self):
    self._delete()
      
  def __enter__(self):
    return self
  
  def __exit__(self, exc_type, exc_value, tb):
    
    if exc_type is not None:
      traceback.print_exception(exc_type, exc_value, tb)
 
    self._delete()
          
  @property
  def df(self):
    return self._df
 
  def delete(self):
    self._delete()

  def _delete(self):
    self._df_before.unpersist(self._blocking_unpersist)
    self._df.unpersist(self._blocking_unpersist)

# COMMAND ----------

def clear_cache():
  spark.sql('CLEAR CACHE')

# COMMAND ----------

def create_table(df,
                 db_or_asset,
                 table=None,
                 overwrite=False
                ):
  ''' 
  Write a table from the input df.
  The database and table name can be provided as a single parameter input (db_or_asset), or separately (db_or_asset and table)
  If db is the test database, alter permissions so we can amend the table later.
  '''
  if table is None:
    asset_name = db_or_asset
    db = db_or_asset.split('.')[0]
    table = db_or_asset.split('.')[1]
  else:
    asset_name = f'{db_or_asset}.{table}'
    db = db_or_asset
 
  if overwrite is True:
    df.write.saveAsTable(asset_name, mode='overwrite')
  else:
    df.write.saveAsTable(asset_name)
      
  if db == 'test_epma_autocoding':
    # On ref when testing. Oo prod, or on ref normally, the job user has access to the epma_autocoding db, so this isn't necessary.
    spark.sql(f'ALTER TABLE {asset_name} OWNER TO `data-managers`')

  
def drop_table(db, table):
  spark.sql(f'drop table {db}.{table}')
 
  
def table_exists(db, table):
  tables = [r.tableName for r in spark.sql(f'show tables in {db}').select('tableName').collect()]
  return table in tables



def create_table_from_schema(schema: StructType,
                             db_or_asset: str,
                             table: str=None,
                             overwrite: bool=False,
                             allow_nullable_schema_mismatch=False
                            ) -> None:
  '''
  Creates a new empty table based on the given schema.
  
  If overwrite is True the table is dropped if it already exists.
  
  If overwrite is False and the table schemas don't match, an AssertionError is raised.
  
  Nullability of fields in the schema are not preserved. Nullability is always True in the new table.
  '''
  if table is None:
    db = db_or_asset.split('.')[0]
    table = db_or_asset.split('.')[1]
    asset_name = db_or_asset
  else:
    db = db_or_asset
    asset_name = f'{db}.{table}'
    
  df_new = spark.createDataFrame([], schema)
   
  if table_exists(db, table):
    if overwrite:
      drop_table(db, table)
      create_table(df_new, db, table)
    else:
      df_existing = spark.table(asset_name)
      if check_schemas_match(df_existing, df_new, allow_nullable_schema_mismatch=allow_nullable_schema_mismatch) is False:
        raise AssertionError(f'The given new schema does not match the existing schema, and overwrite is set to False.\nExisting: {df_existing.schema.json()}\nNew     : {df_new.schema.json()}')
  else:
      create_table(df_new, db, table)