# Databricks notebook source
# MAGIC %run ../../_modules/epma_global/functions

# COMMAND ----------

# MAGIC %run ../../_modules/epma_global/constants

# COMMAND ----------

# MAGIC %run ../../_modules/epma_global/ref_data_lib

# COMMAND ----------

from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text('stage_id', 'source_b_accuracy', 'stage_id')
dbutils.widgets.text('notebook_location', './notebooks/5_source_b_accuracy/drivers/source_b_accuracy_driver', 'notebook_location')
dbutils.widgets.text('input_table', '', 'input_table')
dbutils.widgets.text('unmappable_table', '', 'unmappable_table')
dbutils.widgets.text('output_table', '', 'output_table')
dbutils.widgets.text('baseline_table', '', 'baseline_table')
dbutils.widgets.text('ground_truth_table', '', 'ground_truth_table')

stage = {
  'stage_id' : dbutils.widgets.get('stage_id'),
  'notebook_path' : dbutils.widgets.get('notebook_location'),
  'input_table' : dbutils.widgets.get('input_table'),
  'unmappable_table' : dbutils.widgets.get('unmappable_table'),
  'output_table' : dbutils.widgets.get('output_table'),
  'baseline_table' : dbutils.widgets.get('baseline_table'),
  'ground_truth_table' : dbutils.widgets.get('ground_truth_table'),
}

exit_message = []

# COMMAND ----------

df_baseline = get_data(stage['baseline_table']).withColumn(ORIGINAL_TEXT_COL, F.lower(F.col(ORIGINAL_TEXT_COL)))

df_gt = get_data(stage['ground_truth_table']).drop('id').withColumn(ORIGINAL_TEXT_COL, F.lower(F.col('medication_name_value'))).drop('medication_name_value')

df_match_lookup_final = get_data(stage['input_table']).select(ORIGINAL_TEXT_COL, FORM_IN_TEXT_COL, MATCH_ID_COL, MATCH_TERM_COL, ID_LEVEL_COL, MATCH_LEVEL_COL)

# Add columns to the unmappable table so it can be unioned with match_lookup_final
df_unmappable_table = get_data(stage['unmappable_table']) \
    .withColumn(MATCH_ID_COL, F.lit('non-mappable')) \
    .withColumn(MATCH_TERM_COL, F.lit('non-mappable')) \
    .withColumn(ID_LEVEL_COL, F.lit('non-mappable')) \
    .select(ORIGINAL_TEXT_COL, FORM_IN_TEXT_COL, MATCH_ID_COL, MATCH_TERM_COL, ID_LEVEL_COL, F.col(REASON_COL).alias(MATCH_LEVEL_COL))

df_outcome = df_match_lookup_final.union(df_unmappable_table)

# Save the outcome table. If the accuracy is better than the baseline, then we can use this table as a new baseline. 
create_table(df_outcome, stage['output_table'], overwrite=True)

# Make lower case, to enable joins to the ground truth.
df_outcome = df_outcome.withColumn(ORIGINAL_TEXT_COL, F.lower(F.col(ORIGINAL_TEXT_COL)))

# COMMAND ----------

# update ground truth with any VMPs that have a new VPID or VTMs that have a new VTMID

df_vtm_ids = RefDataStore.vtm_prev.select(col('ref_id').alias('old_id'), col('VTMID').alias('new_id')).dropna()
df_vmp_ids = RefDataStore.vmp_prev.select(col('VPIDPREV').alias('old_id'), col('ref_id').alias('new_id')).dropna()
df_ids_update = df_vtm_ids.union(df_vmp_ids)

df_gt = df_gt.join(df_ids_update, [df_gt.match_id_gt == df_ids_update.old_id], how='left')
df_gt = df_gt.withColumn('match_id_gt', F.when(col('new_id').isNotNull(), col('new_id')).otherwise(col('match_id_gt'))).drop('old_id', 'new_id')

df_gt = df_gt.join(df_ids_update, [df_gt.match_id_gt_r == df_ids_update.old_id], how='left')
df_gt = df_gt.withColumn('match_id_gt_r', F.when(col('new_id').isNotNull(), col('new_id')).otherwise(col('match_id_gt_r'))).drop('old_id', 'new_id')

# COMMAND ----------

df_baseline_results = df_baseline.join(df_gt, 
                                       [df_baseline[ORIGINAL_TEXT_COL] == df_gt[ORIGINAL_TEXT_COL],
                                        df_baseline[FORM_IN_TEXT_COL].eqNullSafe(df_gt[FORM_IN_TEXT_COL])],
                                        how = 'inner').drop(df_gt[ORIGINAL_TEXT_COL]).drop(df_gt[FORM_IN_TEXT_COL]) \
                                       .withColumn('result', F.when(col(MATCH_ID_COL) == col('match_id_gt'), 'correct').otherwise('incorrect'))
baseline_correct = df_baseline_results.where(col('result') == 'correct').count()
baseline_total = df_baseline_results.count()

if baseline_correct is not None and baseline_total is not None and baseline_total > 0:
  baseline_accuracy = baseline_correct / baseline_total
  print(baseline_accuracy)

# COMMAND ----------

df_outcome_results = df_outcome.join(df_gt, 
                                       [df_outcome[ORIGINAL_TEXT_COL] == df_gt[ORIGINAL_TEXT_COL],
                                        df_outcome[FORM_IN_TEXT_COL].eqNullSafe(df_gt[FORM_IN_TEXT_COL])],
                                        how = 'inner').drop(df_gt[ORIGINAL_TEXT_COL]).drop(df_gt[FORM_IN_TEXT_COL]) \
                                       .withColumn('result', F.when(col(MATCH_ID_COL) == col('match_id_gt'), 'correct').otherwise('incorrect'))
outcome_correct = df_outcome_results.where(col('result') == 'correct').count()
outcome_total = df_outcome_results.count()

if outcome_correct is not None and outcome_total is not None and outcome_total > 0:
  outcome_accuracy = outcome_correct / outcome_total
  print(outcome_accuracy)

# COMMAND ----------

if outcome_accuracy != baseline_accuracy:
  exit_message.append(f'WARN: Source B accuracy has changed from {baseline_accuracy} to {outcome_accuracy}.')
  
display(df_outcome_results.groupBy(MATCH_LEVEL_COL, 'result').count())

# COMMAND ----------

# Compare results between baseline and outcome, to see which records have changed, if any.
df_baseline = df_baseline.select(ORIGINAL_TEXT_COL, FORM_IN_TEXT_COL, \
                                 *(F.col(x).alias(x + '_baseline') for x in [MATCH_ID_COL, MATCH_TERM_COL, ID_LEVEL_COL, MATCH_LEVEL_COL]))
df_outcome = df_outcome.select(ORIGINAL_TEXT_COL, FORM_IN_TEXT_COL, \
                                 *(F.col(x).alias(x + '_outcome') for x in [MATCH_ID_COL, MATCH_TERM_COL, ID_LEVEL_COL, MATCH_LEVEL_COL]))

df_compare = df_baseline.join(df_outcome,
                              [df_baseline[ORIGINAL_TEXT_COL] == df_outcome[ORIGINAL_TEXT_COL],
                               df_baseline[FORM_IN_TEXT_COL].eqNullSafe(df_outcome[FORM_IN_TEXT_COL])],
                              how='inner') \
                        .join(df_gt,
                              [df_baseline[ORIGINAL_TEXT_COL] == df_gt[ORIGINAL_TEXT_COL],
                               df_baseline[FORM_IN_TEXT_COL].eqNullSafe(df_gt[FORM_IN_TEXT_COL])],
                              how='inner')

# COMMAND ----------

df_used_to_be_wrong_now_right = df_compare.where(F.col('match_id_baseline') != F.col('match_id_gt')) \
                                .where(F.col('match_id_outcome') == F.col('match_id_gt'))
display(df_used_to_be_wrong_now_right)

# COMMAND ----------

df_used_to_be_right_now_wrong = df_compare.where(F.col('match_id_baseline') == F.col('match_id_gt')) \
                                .where(F.col('match_id_outcome') != F.col('match_id_gt'))
display(df_used_to_be_right_now_wrong)

# COMMAND ----------

now_wrong_count = df_used_to_be_right_now_wrong.count()
if now_wrong_count>0:
  exit_message.append(f'WARN: Some results are different to the baseline. {now_wrong_count} records which used to be correct are now incorrect. Look at the ephemeral notebook for details.')

now_right_count = df_used_to_be_wrong_now_right.count()
if now_right_count>0:
  exit_message.append(f'WARN: Some results are different to the baseline. {now_right_count} records which used to be incorrect are now correct. Look at the ephemeral notebook for details.')

# COMMAND ----------

df_amp = RefDataStore.amp.select('APID', 'VPID')
df_vmp = RefDataStore.vmp.select(col('ref_id').alias('VPID'), 'VTMID', col('ref_description').alias('VMP_NM'))
df_vtm = RefDataStore.vtm.select(col('ref_id').alias('VTMID'), col('ref_description').alias('VTM_NM'))

df_amp_to_vmp = df_amp.join(df_vmp, on='VPID', how='left').select('APID', 'VPID', 'VMP_NM')
df_vmp_to_vtm = df_vmp.join(df_vtm, on='VTMID', how='left').select('VPID', 'VTMID', 'VTM_NM')
df_amp_to_vtm = df_amp_to_vmp.join(df_vmp_to_vtm, on='VPID', how='left').select('APID', 'VTMID', 'VTM_NM')

# COMMAND ----------

# Filter for where outcome results are wrong.
df_outcome_wrong = df_outcome_results.where(F.col('result') == 'incorrect') \
                                     .select(ORIGINAL_TEXT_COL, FORM_IN_TEXT_COL, MATCH_LEVEL_COL, MATCH_ID_COL, MATCH_TERM_COL, ID_LEVEL_COL, 'match_id_gt', 'matchterm_gt', 'id_level_gt')

# If the outcome is wrong in comparison to the ground truth, there may still be an acceptable match at a higher level.
# For any outcome matches at AMP level, find the corresponding VMP and VTM. For any outcome matches at VMP level, find the corresponding VTM.
# For any ground truth matches at AMP level, find the corresponding VMP and VTM. For any ground truth matches at VMP level, find the corresponding VTM.
# Now compare outcome match_id to ground truth match_id at VMP an VTM level. If this is a match then we are correct at higher level.
df_outcome_wrong = df_outcome_wrong.join(df_amp_to_vmp, df_outcome_wrong[MATCH_ID_COL] == df_amp_to_vmp['APID'], how='left') \
                                   .withColumn('match_VPID', F.when(F.col('VPID').isNotNull(), F.col('VPID')) \
                                                              .when(F.col(ID_LEVEL_COL) == 'VPID', F.col(MATCH_ID_COL)) \
                                                              .otherwise(None)) \
                                   .withColumn('match_VMP_NM', F.when(F.col('VMP_NM').isNotNull(), F.col('VMP_NM')) \
                                                                .when(F.col(ID_LEVEL_COL) == 'VPID', F.col(MATCH_TERM_COL)) \
                                                                .otherwise(None)) \
                                   .drop('APID', 'VPID', 'VMP_NM') \
                                   .join(df_vmp_to_vtm, df_outcome_wrong[MATCH_ID_COL] == df_vmp_to_vtm['VPID'], how='left') \
                                   .withColumn('match_VTMID_1', F.col('VTMID')) \
                                   .withColumn('match_VTM_NM_1', F.col('VTM_NM')) \
                                   .drop('VPID', 'VTMID', 'VTM_NM') \
                                   .join(df_amp_to_vtm, df_outcome_wrong[MATCH_ID_COL] == df_amp_to_vtm['APID'], how='left') \
                                   .withColumn('match_VTMID_2', F.col('VTMID')) \
                                   .withColumn('match_VTM_NM_2', F.col('VTM_NM')) \
                                   .drop('APID', 'VTMID', 'VTM_NM') \
                                   .withColumn('match_VTMID', F.when(F.col('match_VTMID_1').isNotNull(), F.col('match_VTMID_1')) \
                                                               .when(F.col('match_VTMID_2').isNotNull(), F.col('match_VTMID_2')) \
                                                               .when(F.col(ID_LEVEL_COL) == 'VTMID', F.col(MATCH_ID_COL)) \
                                                               .otherwise(None)) \
                                   .withColumn('match_VTM_NM', F.when(F.col('match_VTM_NM_1').isNotNull(), F.col('match_VTM_NM_1')) \
                                                                .when(F.col('match_VTM_NM_2').isNotNull(), F.col('match_VTM_NM_2')) \
                                                                .when(F.col(ID_LEVEL_COL) == 'VTMID', F.col(MATCH_TERM_COL)) \
                                                                .otherwise(None)) \
                                   .drop('match_VTMID_1', 'match_VTMID_2', 'match_VTM_NM_1', 'match_VTM_NM_2') \
                                   .join(df_amp_to_vmp, df_outcome_wrong['match_id_gt'] == df_amp_to_vmp['APID'], how='left') \
                                   .withColumn('match_VPID_gt', F.when(F.col('VPID').isNotNull(), F.col('VPID')) \
                                                                 .when(F.col('id_level_gt') =='VPID', F.col('match_id_gt')) \
                                                                 .otherwise(None)) \
                                   .withColumn('match_VMP_NM_gt', F.when(F.col('VMP_NM').isNotNull(), F.col('VMP_NM')) \
                                                                   .when(F.col('id_level_gt') == 'VPID', F.col('matchterm_gt')) \
                                                                   .otherwise(None)) \
                                   .drop('APID', 'VPID', 'VMP_NM') \
                                   .join(df_vmp_to_vtm, df_outcome_wrong['match_id_gt'] == df_vmp_to_vtm['VPID'], how='left') \
                                   .withColumn('match_VTMID_1_gt', F.col('VTMID')) \
                                   .withColumn('match_VTM_NM_1_gt', F.col('VTM_NM')) \
                                   .drop('VPID', 'VTMID', 'VTM_NM') \
                                   .join(df_amp_to_vtm, df_outcome_wrong['match_id_gt'] == df_amp_to_vtm['APID'], how='left') \
                                   .withColumn('match_VTMID_2_gt', F.col('VTMID')) \
                                   .withColumn('match_VTM_NM_2_gt', F.col('VTM_NM')) \
                                   .drop('APID', 'VTMID', 'VTM_NM') \
                                   .withColumn('match_VTMID_gt', F.when(F.col('match_VTMID_1_gt').isNotNull(), F.col('match_VTMID_1_gt')) \
                                                                  .when(F.col('match_VTMID_2_gt').isNotNull(), F.col('match_VTMID_2_gt')) \
                                                                  .when(F.col('id_level_gt') == 'VTMID', F.col('match_id_gt')) \
                                                                  .otherwise(None)) \
                                   .withColumn('match_VTM_NM_gt', F.when(F.col('match_VTM_NM_1_gt').isNotNull(), F.col('match_VTM_NM_1_gt')) \
                                                                   .when(F.col('match_VTM_NM_2_gt').isNotNull(), F.col('match_VTM_NM_2_gt')) \
                                                                   .when(F.col('id_level_gt') == 'VTMID', F.col('matchterm_gt')) \
                                                                   .otherwise(None)) \
                                   .drop('match_VTMID_1_gt', 'match_VTMID_2_gt', 'match_VTM_NM_1_gt', 'match_VTM_NM_2_gt')

df_outcome_correct_at_higher_level = df_outcome_wrong.where((col('match_VPID') == col('match_VPID_gt')) | (col('match_VTMID') == col('match_VTMID_gt')))
display(df_outcome_correct_at_higher_level)

# COMMAND ----------

outcome_correct_at_higher_level = outcome_correct + df_outcome_correct_at_higher_level.count()

if outcome_correct_at_higher_level is not None and outcome_total is not None and outcome_total > 0:
  outcome_accuracy_at_higher_level = outcome_correct_at_higher_level / outcome_total
  print(outcome_accuracy_at_higher_level)

# COMMAND ----------

exit_message_str = '\n'.join(exit_message)
dbutils.notebook.exit(exit_message_str)