# Databricks notebook source
# MAGIC %run ../../_modules/epma_global/functions

# COMMAND ----------

# MAGIC %run ../../_modules/epma_global/constants

# COMMAND ----------

# MAGIC %run ../../1_exact_match/functions/exact_match_functions

# COMMAND ----------

# MAGIC %run ../functions/accuracy_calculating_functions

# COMMAND ----------

# MAGIC %run ../../_modules/epma_global/ref_data_lib

# COMMAND ----------

import os
from datetime import datetime
from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, LongType

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text('notebook_location', './notebooks/4_accuracy_calculating/accuracy_calculating_driver', 'notebook_location')
dbutils.widgets.text('ground_truth_table', '', 'ground_truth_table')
dbutils.widgets.text('input_table', '', 'input_table')
dbutils.widgets.text('raw_data_required', 'True', 'raw_data_required')
dbutils.widgets.text('output_table', '', 'output_table')

stage = {
  'notebook_path' : os.path.basename(dbutils.widgets.get('notebook_location')),
  'ground_truth_table' : dbutils.widgets.get('ground_truth_table'),
  'input_table' : dbutils.widgets.get('input_table'),
  'notebook_path' : dbutils.widgets.get('notebook_location'),
  'notebook_name' : os.path.basename(dbutils.widgets.get('notebook_location')),
  'raw_data_required' : bool(dbutils.widgets.get('raw_data_required')),
  'output_table' : dbutils.widgets.get('output_table')
}

# COMMAND ----------

exit_message = []
exit_message.append(f"\n notebook {stage['notebook_path']} execution started  @ {datetime.now().isoformat()}\n")

# COMMAND ----------

df_lookup = get_data(stage['input_table']) \
  .select(ORIGINAL_TEXT_COL, FORM_IN_TEXT_COL, col(MATCH_ID_COL).alias('our_id'), col(ID_LEVEL_COL).alias('our_id_level'), 
        col(MATCH_LEVEL_COL).alias('our_match_level'))

# Ground truth is taken from the DmdId column of the epmawspc table. Need some filters to get suitable records.
df_ground_truth = get_data(stage['ground_truth_table']).where(col('EFFECTIVE_TO').isNull()) \
                                                       .where(col('DmdId').isNotNull()) \
                                                       .withColumn('Drug', F.lower(col('Drug'))) \
                                                       .drop_duplicates(['Drug']) \
                                                       .select(col('Drug').alias(ORIGINAL_TEXT_COL), 
                                                               col('DmdId').alias('gt_id'), col('DmdName').alias('gt_name'))

# COMMAND ----------

df_amp_vmp_vtm = combine_ref_data(RefDataStore)

# Add the match term as 'our_name' to the pipeline output
df_lookup_add_dss_name = add_dss_to_lookup(df_amp_vmp_vtm,
                                           df_lookup, 
                                           ref_data_id_col=RefDataStore.ID_COL,
                                           ref_data_text_col=RefDataStore.TEXT_COL,
                                           new_text_col='our_name',
                                           lookup_id_col='our_id')

# Add the ground truth id level as 'gt_id_level'
# Drop nulls, any without an id_level cannot be used for ground truth comparisons.
df_ground_truth_add_id_level = ground_truth_add_id_level(df_amp_vmp_vtm, 
                                                         df_ground_truth, 
                                                         ref_data_id_col=RefDataStore.ID_COL,
                                                         ref_data_id_level_col=RefDataStore.ID_LEVEL_COL,
                                                         new_id_level_col='gt_id_level',
                                                         ground_truth_id_col='gt_id') \
                                                 .dropna(subset=['gt_id_level'])

# Join pipeline results to ground truth
# Fill na with whitespace. If there is no matching ground truth. Without this, comparing any string to null would always return True.
df_lookup_add_ground_truth = add_ground_truth_to_lookup(df_lookup_add_dss_name,
                                                        df_ground_truth_add_id_level,
                                                        join_col=ORIGINAL_TEXT_COL) \
                                                .fillna(' ')

# COMMAND ----------

# Tokenise the input description, pipeline match term, and ground truth match term.
# Comparisons are done on tokens so that matches are correct even if words are in the wrong order.
df_epma_token = select_distinct_tokens(df_lookup_add_ground_truth, ORIGINAL_TEXT_COL, 'epma_token')
df_our_token = select_distinct_tokens(df_epma_token, 'our_name', 'our_token')
df_gt_token = select_distinct_tokens(df_our_token, 'gt_name', 'gt_token')

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Case 1: We report vtm, ground truth report amp/vmp.
# MAGIC 
# MAGIC Rules: If ground truth name = epma name, and ground truth name != our name, we're wrong.

# COMMAND ----------

df_us_vtm = df_gt_token.filter(col('our_id_level') == 'VTMID') \
                       .filter((col('gt_id_level') == 'AMP') | (col('gt_id_level') == 'VMP'))

# We're wrong if ground truth = input and ground truth != pipeline
df_our_vtm_wrong = df_us_vtm.filter(col('epma_token') == col('gt_token')) \
                            .filter(col('our_token') != col('gt_token')) \
                            .withColumn('pipeline_mismatch_reason', F.lit('Source match same as epma description and at more granular level.'))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Case 2: We report amp. ground truth report amp/vmp.
# MAGIC 
# MAGIC Rules:
# MAGIC 1. If our name != epma name, and our name != ground truth name, we're wrong.
# MAGIC 2. If all the AMPs with the same name map to the same VMP, or a unique AMP maps to a VMP with the same name, we're wrong as we should have reported the VMP.

# COMMAND ----------

df_our_amp = df_gt_token.filter(col('our_id_level') == 'APID') \
                        .filter((col('gt_id_level') == 'AMP') | (col('gt_id_level') == 'VMP'))

# We're wrong if pipeline != input and pipeline != ground truth
df_our_amp_wrong = df_our_amp.filter(col('epma_token') != col('our_token')) \
                             .filter(col('gt_token') != col('our_token')) \
                             .withColumn('pipeline_mismatch_reason', F.lit('Pipeline match different from epma description.'))

df_amp = RefDataStore.amp.select(RefDataStore.ID_COL, col(RefDataStore.TEXT_COL).alias('amp_description'), 'APID', 'VPID')
df_vmp = RefDataStore.vmp.select(RefDataStore.ID_COL, col(RefDataStore.TEXT_COL).alias('vmp_description'))
df_amp_vmp = df_amp.join(df_vmp, on=(df_amp['VPID'] == df_vmp[RefDataStore.ID_COL]), how='left') \
                   .select('APID', 'VPID', 'amp_description', 'vmp_description')

# Get the count of the number of APIDS that have the same description.
window_desc_count = Window.partitionBy('amp_description')
df_amp_vmp_wdesc_count = df_amp_vmp.withColumn('apid_count', F.size(F.collect_set('APID').over(window_desc_count)))

# We're wrong if the amps with the same name map to a vmp with the same name, or a unique amp maps to a vmp with the same name
df_our_amp_should_map_to_vmp = df_our_amp.join(df_our_amp_wrong, [ORIGINAL_TEXT_COL, FORM_IN_TEXT_COL], how='left_anti') \
                                         .join(df_amp_vmp_wdesc_count, df_our_amp['our_id'] == df_amp_vmp_wdesc_count['APID'], how='left') \
                                         .where((col('apid_count') > 1) | ((col('apid_count') == 1) & (col('amp_description') == col('vmp_description')))) \
                                         .drop('APID', 'VPID', 'amp_description', 'vmp_description', 'apid_count') \
                                         .withColumn('pipeline_mismatch_reason', F.lit('APID chosen but should have mapped to VPID.'))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Case 3a: We report vmp. ground truth report VMP
# MAGIC 
# MAGIC Rules: 
# MAGIC If ground truth vmp = epma but our vmp != epma and our vmp != ground truth vmp. We're wrong.

# COMMAND ----------

df_us_vmp = df_gt_token.filter(col('our_id_level')=='VPID') \
                       .filter((col('gt_id_level') == 'AMP') | (col('gt_id_level') == 'VMP'))

# We're wrong if ground truth = input and pipeline != input and pipeline != ground truth
df_our_vmp_wrong = df_us_vmp.filter(col('gt_id_level') == 'VMP') \
                            .filter(col('epma_token') == col('gt_token')) \
                            .filter(col('epma_token') != col('our_token')) \
                            .filter(col('our_token') != col('gt_token')) \
                            .withColumn('pipeline_mismatch_reason', F.lit('Source match same as epma description, pipeline match different from epma description.'))

# COMMAND ----------

# MAGIC %md
# MAGIC Case 3b: We report vmp. ground truth report AMP
# MAGIC 
# MAGIC Rules: If ground truth report amp, ground truth amp = epma, and there is unique amp, and ground truth vmp != ground truth amp. ground truth correct.

# COMMAND ----------

# We're wrong if ground truth = input and ground truth is unique amp which doesn't map to a vmp with the same name
df_our_vmp_should_be_amp = df_us_vmp.filter(col('gt_id_level') == 'AMP') \
                           .filter(col('epma_token') == col('gt_token')) \
                           .join(RefDataStore.amp.select(col(RefDataStore.TEXT_COL).alias('gt_name'), col('VPID').alias('gt_vpid')).groupBy('gt_name', 'gt_vpid').count(), 'gt_name', 'left') \
                           .join(RefDataStore.vmp.select(col(RefDataStore.ID_COL).alias('gt_vpid'), col(RefDataStore.TEXT_COL).alias('gt_vmp')), 'gt_vpid', 'left') \
                           .filter((col('count') == 1) & (col('gt_name') != col('gt_vmp'))) \
                           .drop('gt_vpid', 'count', 'gt_vmp') \
                           .withColumn('pipeline_mismatch_reason', F.lit('Source match same as epma description and at more granular level.'))

# COMMAND ----------

# Collate all the wrong results
df_our_wrong_match = df_our_vtm_wrong \
  .unionByName(df_our_amp_wrong) \
  .unionByName(df_our_amp_should_map_to_vmp) \
  .unionByName(df_our_vmp_should_be_amp) \
  .unionByName(df_our_vmp_wrong)

# COMMAND ----------

# Use df_our_wrong_match to add a 'pipeline mismatch' flag to df_all_results
df_all_results = df_lookup_add_ground_truth.join(df_our_wrong_match.select([ORIGINAL_TEXT_COL]).withColumn('pipeline_mismatch', lit('pipeline_mismatch')),
                                                 ORIGINAL_TEXT_COL,
                                                 'left')

# COMMAND ----------

# Distinguish which records in the results were generated by the most recent pipeline run
df_all_results_with_run_id = df_all_results.join(spark.table(stage['input_table']).select(ORIGINAL_TEXT_COL, FORM_IN_TEXT_COL, RUN_ID_COL), [ORIGINAL_TEXT_COL, FORM_IN_TEXT_COL], 'left')
most_recent_run_id = df_all_results_with_run_id.select(F.max(col(RUN_ID_COL))).collect()[0][0]
df_most_recent_run = df_all_results_with_run_id.where(col(RUN_ID_COL) == most_recent_run_id)

# COMMAND ----------

# Create a summary table showing the count of pipeline mismatches vs correct results by id_level and match_level

# A cumulative version for all the results
df_total_by_id_level = df_all_results.groupBy('our_id_level', 'gt_id_level', 'our_match_level', 'pipeline_mismatch').count() \
.select(col('our_id_level').alias('pipeline_match_id_level'), col('our_match_level').alias('pipeline_match_level'), col('gt_id_level').alias('source_match_id_level'), 'pipeline_mismatch', col('count').alias('total_match_count')).withColumn(MATCH_DATETIME_COL, F.current_timestamp()).withColumn('run_id', lit('Cumulative'))

# A version for only the results from the most recent run
df_total_by_id_level_most_recent_run = df_most_recent_run.groupBy('our_id_level', 'gt_id_level', 'our_match_level', 'pipeline_mismatch').count() \
.select(col('our_id_level').alias('pipeline_match_id_level'), col('our_match_level').alias('pipeline_match_level'), col('gt_id_level').alias('source_match_id_level'), 'pipeline_mismatch', col('count').alias('total_match_count')).withColumn(MATCH_DATETIME_COL, F.current_timestamp()).withColumn('run_id', lit(most_recent_run_id).cast(StringType()))

# COMMAND ----------

append_to_table(df_total_by_id_level, None, output_loc = stage["output_table"], allow_nullable_schema_mismatch=True)
append_to_table(df_total_by_id_level_most_recent_run, None, output_loc = stage["output_table"], allow_nullable_schema_mismatch=True)

# COMMAND ----------

# Print out the accuracy score as a fraction
count_mismatch_total = df_all_results.filter(~col('pipeline_mismatch').isNull()).count()
count_match_total = df_all_results.filter(col('gt_id_level') != ' ').count()
print(count_mismatch_total)
print(count_match_total)

if count_mismatch_total is not None and count_match_total is not None and count_match_total > 0:
  accuracy = 1 - count_mismatch_total/count_match_total
  print(accuracy)

# COMMAND ----------

exit_message.append(f"notebook {stage['notebook_path']} execution completed  @ {datetime.now().isoformat()}")
exit_message = [i for i in exit_message if i] 
exit_message = '\n'.join(exit_message)
dbutils.notebook.exit(exit_message)