# Databricks notebook source
import os

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %run ../../_modules/epma_global/functions

# COMMAND ----------

# MAGIC %run ../../_modules/epma_global/constants

# COMMAND ----------

# MAGIC %run ../functions/exceptions_and_preprocessing_functions

# COMMAND ----------

import os
from datetime import datetime

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text('notebook_location', './0_exceptions_and_preprocessing/drivers/exceptions_and_preprocessing_driver', 'notebook_location')
dbutils.widgets.text('epma_table', '', 'epma_table')
dbutils.widgets.text('source_dataset', '', 'source_dataset')
dbutils.widgets.text('output_table', '', 'output_table')
dbutils.widgets.text('unmappable_table', '', 'unmappable_table')
dbutils.widgets.text('match_lookup_final_table', '', 'match_lookup_final_table')
dbutils.widgets.text('batch_size', '', 'batch_size')
dbutils.widgets.text('run_id', '', 'run_id')

stage = {
  'notebook_path' : os.path.basename(dbutils.widgets.get('notebook_location')),
  'epma_table': dbutils.widgets.get('epma_table'),
  'source_dataset': dbutils.widgets.get('source_dataset'),
  'output_table': dbutils.widgets.get('output_table'),
  'unmappable_table': dbutils.widgets.get('unmappable_table'),
  'match_lookup_final_table': dbutils.widgets.get('match_lookup_final_table'),
  'batch_size': int(dbutils.widgets.get('batch_size')),
  'run_id': dbutils.widgets.get('run_id'),
}

# COMMAND ----------

exit_message = []
exit_message.append(f"\n notebook {stage['notebook_path']} execution started  @ {datetime.now().isoformat()}\n")

# COMMAND ----------

source_table = spark.table(stage['epma_table'])

if stage['source_dataset'] == 'source_b':
  src_medication_col = 'medication_name_value'
if stage['source_dataset'] == 'source_a':
  src_medication_col = 'Drug'
  source_table = source_table.withColumn(FORM_IN_TEXT_COL, lit(' '))
  # In the ref environment, only records with this EVENT_ID are unhashed in ref
  if os.environ.get('env') == 'ref':
    source_table = source_table.where(col('META.EVENT_ID').contains('582331:'))

# COMMAND ----------

source_table_without_null = drop_null_in_medication_col(source_table, src_medication_col)
df_match_lookup_final = spark.table(stage['match_lookup_final_table'])
df_unmappable_table = spark.table(stage['unmappable_table'])

df_epma = select_distinct_descriptions(source_table_without_null, src_medication_col=src_medication_col, original_text_col=ORIGINAL_TEXT_COL, form_in_text_col=FORM_IN_TEXT_COL, id_col=ID_COL)
df_selected = select_record_batch_to_process(df_epma, df_match_lookup_final, df_unmappable_table, stage['batch_size'], [ORIGINAL_TEXT_COL, FORM_IN_TEXT_COL])

# COMMAND ----------

# Preprocessing.
# Includes creating two versions of the epma_description, one with form_in_text appended and one without.
# In exact matching we try to match to both versions, to increase the chance of getting a match.
# For later stages of the pipeline we only match to the epma_description which has form_in_text appended, to save on processing time.

if stage['source_dataset'] == 'source_b':
  df_selected_cleaned = df_selected.df \
    .withColumn(TEXT_WITHOUT_FORM_COL, F.lower(col(ORIGINAL_TEXT_COL))) \
    .withColumn(FORM_IN_TEXT_COL, F.lower(col(FORM_IN_TEXT_COL))) \
    .withColumn(TEXT_WITHOUT_FORM_COL, standardise_doseform(col(TEXT_WITHOUT_FORM_COL))) \
    .withColumn(TEXT_WITHOUT_FORM_COL, standardise_drug_name(col(TEXT_WITHOUT_FORM_COL))) \
    .withColumn(TEXT_WITHOUT_FORM_COL, replace_hyphens_between_dosages_with_slashes(col(TEXT_WITHOUT_FORM_COL))) \
    .withColumn(TEXT_WITHOUT_FORM_COL, correct_common_unit_errors(col(TEXT_WITHOUT_FORM_COL))) \
    .withColumn(TEXT_COL, F.when(col(TEXT_WITHOUT_FORM_COL).contains(col(FORM_IN_TEXT_COL)), col(TEXT_WITHOUT_FORM_COL))
                          .when(col(FORM_IN_TEXT_COL).isNull(), col(TEXT_WITHOUT_FORM_COL))
                          .when(col(FORM_IN_TEXT_COL).contains('other (add comment)'), col(TEXT_WITHOUT_FORM_COL))
                          .otherwise(F.concat(col(TEXT_WITHOUT_FORM_COL), lit(' '), col(FORM_IN_TEXT_COL))))
  df_remaining, df_unmappable = filter_user_curated_unmappables(df_selected_cleaned, text_col=TEXT_COL, unmappable_regexes=['freetext medication', r'\+ neat', r'water .*\+'])     
if stage['source_dataset'] == 'source_a':
  df_selected_cleaned = df_selected.df \
    .withColumn(TEXT_WITHOUT_FORM_COL, lit(None).cast(StringType())) \
    .withColumn(TEXT_COL, F.lower(col(ORIGINAL_TEXT_COL))) \
    .withColumn(TEXT_COL, standardise_doseform(col(TEXT_COL))) \
    .withColumn(TEXT_COL, standardise_drug_name(col(TEXT_COL))) \
    .withColumn(TEXT_COL, replace_hyphens_between_dosages_with_slashes(col(TEXT_COL))) \
    .withColumn(TEXT_COL, correct_common_unit_errors(col(TEXT_COL)))
  df_remaining, df_unmappable = filter_user_curated_unmappables(df_selected_cleaned, text_col=TEXT_COL, unmappable_regexes=['freetext medication', r'\+ neat', r'water .*\+'])                                   

# COMMAND ----------

create_table(df_remaining, stage['output_table'], overwrite=True)

df_unmappable = df_unmappable.select(ORIGINAL_TEXT_COL, FORM_IN_TEXT_COL) \
                             .withColumn(REASON_COL, lit('user_curated_list')) \
                             .withColumn(MATCH_DATETIME_COL, F.current_timestamp()) \
                             .withColumn(RUN_ID_COL, lit(stage['run_id']))

append_to_table(df_unmappable, [ORIGINAL_TEXT_COL, FORM_IN_TEXT_COL], stage['unmappable_table'], allow_nullable_schema_mismatch=True)

df_selected.delete()

# COMMAND ----------

exit_message.append(f"notebook {stage['notebook_path']} execution completed  @ {datetime.now().isoformat()}")
exit_message = [i for i in exit_message if i] 
exit_message = '\n'.join(exit_message)
dbutils.notebook.exit(exit_message)