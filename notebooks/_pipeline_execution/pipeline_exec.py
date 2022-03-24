# Databricks notebook source
from datetime import datetime

# COMMAND ----------

def print_header():  
  print(r'''
              ____|   _ \    \  |     \            \            |                            | _)                   
              __|    |   |  |\/ |    _ \          _ \    |   |  __|   _ \    __|   _ \    _` |  |  __ \    _` |     
              |      ___/   |   |   ___ \        ___ \   |   |  |    (   |  (     (   |  (   |  |  |   |  (   |     
             _____| _|     _|  _| _/    _\     _/    _\ \__,_| \__| \___/  \___| \___/  \__,_| _| _|  _| \__, |     
                                                                                                          |___/''')

# COMMAND ----------

def run_pipeline(pipeline_config):

  print_header()

  raw_data_stage = pipeline_config.pop(0)
  n_stages = len(pipeline_config)

  stages = []

  pipeline_start = datetime.now()
  print(f'BEGINNING EPMA PIPELINE EXECUTION @ {pipeline_start}')

  for counter, stage in enumerate(pipeline_config):
    
      check_key_conflict = [True if key in stage else False for key in raw_data_stage]     
      if True in check_key_conflict:
        raise KeyError('Key Conflict between raw_data_required and stage paramater')

      notebook_location = stage['notebook_location']
      stage_id = stage['stage_id']
      stages.append(stage)

      start_time = datetime.now()
      print('-'*80)
      print(f'Executing pipeline stage {stage_id} (pipeline stage {counter} of {n_stages})  @ {start_time}\n')

      if 'raw_data_required' not in stage:
        stage['raw_data_required'] = False
        
        
      if stage['raw_data_required']:
        stage.update(raw_data_stage)

      if stage['execute']:
        exit_message = dbutils.notebook.run(notebook_location, 0, arguments=stage)
      else:
        exit_message = 'Pipeline skipped as execute is set to False'       

      if counter == n_stages-1:
        pipeline_end = datetime.now()
        print(f'COMPLETED EPMA PIPELINE EXECUTION @ {pipeline_end}')
        run_time = pipeline_end - pipeline_start
        print(f'TIME TO COMPLETE PIPELINE = {run_time}')

      end_time = datetime.now()
      run_time = end_time - start_time
      print(exit_message)
      print(f'Time to complete stage = {run_time}')
      print('-'*80)

  return counter, stages