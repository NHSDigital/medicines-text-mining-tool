# Databricks notebook source
from typing import Union, Callable
from uuid import uuid4
import traceback
 
from pyspark.sql import DataFrame
from pyspark.sql.types import StructField

# COMMAND ----------

# MAGIC %run ./functions

# COMMAND ----------

class TemporaryTable():
  
  def __init__(self, df_or_schema: Union[DataFrame, StructField], db: str, create: bool = True, 
               overwrite: bool = False):
    self._df_or_schema = df_or_schema
    self._db = db
    self._create = create
    self._overwrite = overwrite
    self._name = f'_tmp_{uuid4().hex}'
    self._asset = f'{self._db}.{self._name}'
      
  def __enter__(self):
    if not self._create:
      return self
    
    if isinstance(self._df_or_schema, DataFrame):
      df = self._df_or_schema
    elif isinstance(self._df_or_schema, StructType):
      df = spark.createDataFrame([], self._df_or_schema)
    else:
      raise TypeError('Given df_or_schema must be a dataframe or a schema.')
      
    create_table(df, self._db, self._name, overwrite=self._overwrite)      
    assert table_exists(self._db, self._name)
    return self
  
  def __exit__(self, exc_type, exc_value, tb):
    
    if exc_type is not None:
      traceback.print_exception(exc_type, exc_value, tb)
    
    if not table_exists(self._db, self._name):
      raise AssertionError(f'Table {self._asset} was already dropped')
    drop_table(self._db, self._name)
    if table_exists(self._db, self._name):
      raise AssertionError(f'Failed to drop table {self._asset}')
     
  @property
  def name(self):
    return self._name
  
  @property
  def db(self):
    return self._db

# COMMAND ----------

class MockNotebook(object):    
  '''
  Class that mocks the notebook object and the run function of dbutils.
  This is useful to unittest the  pipeline excution function .
  '''
  def run(self, notebook_location='', timeout_seconds=0, arguments=None):
     print("Notebook ran successfully")
      
      
class MockDBUtils(object):  
  '''
  Class that mocks the dbutils function.  
  '''
  notebook = MockNotebook() 
  

class MockDBUtilsContext:  
    '''  
  This is a context manager used to stub the dbutils with mock dbutil object and replace with original object after 
  exit
  '''
    def __init__(self, dbu):        
        self.temp_dbutils = dbu        

    def __enter__(self):
        global dbutils
        self.mock_dbutils = MockDBUtils()  
        dbutils = self.mock_dbutils        

    def __exit__(self, exc_type, exc_value, tb):      
        global dbutils
        
        if exc_type is not None:
          traceback.print_exception(exc_type, exc_value, tb)
          
        dbutils = self.temp_dbutils


# COMMAND ----------

class FunctionPatch():
  '''
  This class is a context manager that allows patching of functions "imported" from another notebook using %run.
  
  The patch function must be at global scope (i.e. top level)
  '''
  def __init__(self, real_func_name: str, patch_func: Callable):
    self._real_func_name = real_func_name
    self._patch_func = patch_func
    self._backup_real_func = None
    
  def __enter__(self):
    self._backup_real_func = globals()[self._real_func_name]
    globals()[self._real_func_name] = self._patch_func
    
  def __exit__(self, exc_type, exc_value, tb):
    if exc_type is not None:
      traceback.print_exception(exc_type, exc_value, tb)
      
    globals()[self._real_func_name] = self._backup_real_func
    