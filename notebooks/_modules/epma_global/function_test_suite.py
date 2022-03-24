# Databricks notebook source
import unittest
import pandas as pd
from typing import *
from functools import wraps

# COMMAND ----------

# MAGIC %run ./functions

# COMMAND ----------

class FunctionTestSuite(object):
  """
  Defines class for registering/running tests.

  Example usage:

  >> suite = FunctionTestSuite()
  >>
  >> def foo():
  >>   ...
  >>
  >> @suite.add_test
  >> def test_foo():
  >>   ...
  >>  
  >> suite.run()
  
 
  """
  def __init__(self):
    self._suite = unittest.TestSuite()
    self._runner = unittest.TextTestRunner()
    
  def add_test(self, test_func: Callable[[None], bool]) -> None:
    """ Add a test function to the suite. 
    
    Example: 
    >> def foo():
    >>   ...
    >>
    >> @suite.add_test
    >> def test_foo():
    >>   ...
    >>  
    """
    
    @wraps(test_func)
    def clean_up_func():
      result = test_func()
      clear_cache()
      return result
    
    test_case = unittest.FunctionTestCase(clean_up_func)
    self._suite.addTest(test_case)
    
  def run(self) -> unittest.TestResult:
    """ Run the tests & print the output to the console. 
    
    This method can be called once: further tests will need 
    to be assigned to a new object instance.
    
    Returns:
      unittest.TestResult: 
    """
    if not self._runner.run(self._suite).wasSuccessful():
      raise AssertionError()