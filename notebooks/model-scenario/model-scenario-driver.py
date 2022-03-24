# Databricks notebook source
# MAGIC %run ../../notebooks/_modules/epma_global/functions

# COMMAND ----------

import os
import time
from pyspark.sql.functions import col,when,lit
import pyspark.sql.functions as F
import re
import pyspark.sql.types as pst

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text('input_table','epma_autocoding.match_lookup_final','Input table')
dbutils.widgets.text('vtm_table','dss_corporate.vtm','VTM table')
dbutils.widgets.text('vmp_table','dss_corporate.vmp','VMP table')
dbutils.widgets.text('amp_table','dss_corporate.amp','AMP table')
dbutils.widgets.text('invalid_scenario_table', 'epma_autocoding.match_lookup_final_invalid_scenario', 'invalid_scenario_table')
dbutils.widgets.text('update_scenario_table', 'epma_autocoding.match_lookup_final_update_scenario', 'update_scenario_table')

stage = {
  'input_table': dbutils.widgets.get('input_table'),
  'vtm_table': dbutils.widgets.get('vtm_table'),
  'vmp_table': dbutils.widgets.get('vmp_table'),
  'amp_table': dbutils.widgets.get('amp_table'),
  'invalid_scenario_table': dbutils.widgets.get('invalid_scenario_table'),
  'update_scenario_table': dbutils.widgets.get('update_scenario_table')
}

exit_message = []

# COMMAND ----------

data, message = get_all_data(stage, stage_specific_tables_spark=True)
df_lookup, _ = get_data(stage['input_table'], pandas=False)
exit_message = exit_message + message 

# COMMAND ----------

df_dss = data['amp'].select(col('APID').alias('match_id'), 'INVALID') \
.union(data['vmp'].select(col('VPID').alias('match_id'), 'INVALID')) \
.union(data['vtm'].select(col('VTMID').alias('match_id'), 'INVALID'))

df_lookup_invalid = df_lookup.join(df_dss, 'match_id', 'inner') \
.filter(col('INVALID')==1)

# COMMAND ----------

df_lookup_invalid_report = df_lookup_invalid.select('epma_id', 'epma_description', 'match_id', 'id_level', 'match_level', 'match_datetime', 'INVALID')
create_table(df_lookup_invalid_report, stage['invalid_scenario_table'], overwrite=True)

# COMMAND ----------

df_dss_prev = data['vmp'].select(col('VPIDPREV').alias('match_id'), col('VPID').alias('match_id_update')) \
.union(data['vtm'].select(col('VTMIDPREV').alias('match_id'), col('VTMID').alias('match_id_update')))

df_lookup_dss_prev_update = df_lookup.join(df_dss_prev, 'match_id', 'inner') \
.withColumn('match_id', F.col('match_id_update')) \
.drop('match_id_update')

# COMMAND ----------

df_lookup_update_report = df_lookup_dss_prev_update.select('epma_id','epma_description','match_id','id_level','match_level','match_datetime')
create_table(df_lookup_update_report, stage['update_scenario_table'], overwrite=True)