# Databricks notebook source
# MAGIC %run ../../_modules/epma_global/functions

# COMMAND ----------

# MAGIC %run ../../_modules/epma_global/constants

# COMMAND ----------

# MAGIC %run ../../_modules/epma_global/ref_data_lib

# COMMAND ----------

# MAGIC %run ../functions/entity_extraction_functions

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

import pyspark.sql.functions as F
import os
from datetime import datetime

# COMMAND ----------

dbutils.widgets.text('notebook_location', './2_entity_extraction/drivers/entity_extraction_driver', 'notebook_location')
dbutils.widgets.text('input_table', '', 'input_table')
dbutils.widgets.text('match_table', '', 'match_table')
dbutils.widgets.text('output_table', '', 'output_table')
dbutils.widgets.text('unmappable_table', '', 'unmappable_table')
dbutils.widgets.text('run_id', '')

stage = {
  'notebook_path' : os.path.basename(dbutils.widgets.get('notebook_location')),
  'input_table' : dbutils.widgets.get('input_table'),
  'match_table' : dbutils.widgets.get('match_table'),
  'output_table' : dbutils.widgets.get('output_table'),
  'unmappable_table': dbutils.widgets.get('unmappable_table'),
  'run_id': dbutils.widgets.get('run_id')
}

# COMMAND ----------

exit_message = []
exit_message.append(f"\n notebook {stage['notebook_path']} execution started  @ {datetime.now().isoformat()}\n")

# COMMAND ----------

df_exact_non_match = get_data(stage['input_table'])
df_vmp_parsed = RefDataStore.vmp_parsed
df_amp_parsed = RefDataStore.amp_parsed

# Get the list of doseforms, which are used in entity matching
DOSE_FORM_LIST_BC = sc.broadcast([row.DESC for row in get_data('default.form').select('DESC').distinct().collect()])

# Entity match to amp reference data
df_amp_match_output = entity_match(df_exact_non_match,
                                   df_amp_parsed,
                                   'APID',
                                   DOSE_FORM_LIST_BC,
                                   id_col=ID_COL,
                                   original_text_col=ORIGINAL_TEXT_COL,
                                   form_in_text_col=FORM_IN_TEXT_COL,
                                   text_col=TEXT_COL,
                                   match_id_col=MATCH_ID_COL,
                                   id_level_col=ID_LEVEL_COL,
                                   match_level_col=MATCH_LEVEL_COL,
                                   match_datetime_col=MATCH_DATETIME_COL,
                                   ref_id_col=RefDataStore.ID_COL)

# Remaining records after entity matching to amp
df_exact_non_match_less_amp_matches = df_exact_non_match.join(df_amp_match_output.select(ID_COL), ID_COL, 'leftanti')

# Entity match to vmp reference data
df_vmp_match_output = entity_match(df_exact_non_match_less_amp_matches,
                                   df_vmp_parsed,
                                   'VPID',
                                   DOSE_FORM_LIST_BC,
                                   id_col=ID_COL,
                                   original_text_col=ORIGINAL_TEXT_COL,
                                   form_in_text_col=FORM_IN_TEXT_COL,
                                   text_col=TEXT_COL,
                                   match_id_col=MATCH_ID_COL,
                                   id_level_col=ID_LEVEL_COL,
                                   match_level_col=MATCH_LEVEL_COL,
                                   match_datetime_col=MATCH_DATETIME_COL,
                                   ref_id_col=RefDataStore.ID_COL)

# Remaining records after entity matching to amp and vmp
df_exact_non_match_less_amp_and_vmp_matches = df_exact_non_match_less_amp_matches.join(df_vmp_match_output.select(ID_COL), ID_COL, 'leftanti')

# For non-matched records, match on moieties only. This is a prerequisite for linked fuzzy matching at the next stage. 
df_partial_entity_matches, df_partial_entity_unmappable = partial_entity_match(df_exact_non_match_less_amp_and_vmp_matches,
                                                                               df_amp_parsed=df_amp_parsed,
                                                                               df_vmp_parsed=df_vmp_parsed,
                                                                               id_col=ID_COL,
                                                                               original_text_col=ORIGINAL_TEXT_COL,
                                                                               form_in_text_col=FORM_IN_TEXT_COL,
                                                                               text_col=TEXT_COL,
                                                                               match_id_col=MATCH_ID_COL,
                                                                               match_level_col=MATCH_LEVEL_COL,
                                                                               match_term_col=MATCH_TERM_COL,
                                                                               ref_id_col=RefDataStore.ID_COL,
                                                                               ref_text_col=RefDataStore.TEXT_COL)

# Combine matches
df_match_output = df_vmp_match_output.union(df_amp_match_output)

# COMMAND ----------

append_to_table(df_match_output, [ID_COL], stage['match_table'], allow_nullable_schema_mismatch=True)

df_unmappable_select = df_partial_entity_unmappable.select(ORIGINAL_TEXT_COL, FORM_IN_TEXT_COL) \
                                                   .withColumn(REASON_COL, lit('entity_partial_no_unique_moiety')) \
                                                   .withColumn(MATCH_DATETIME_COL, F.current_timestamp()) \
                                                   .withColumn(RUN_ID_COL, lit(stage['run_id']))

append_to_table(df_unmappable_select, [ORIGINAL_TEXT_COL, FORM_IN_TEXT_COL], stage['unmappable_table'], allow_nullable_schema_mismatch=True)

create_table(df_partial_entity_matches, stage['output_table'], overwrite=True)

# COMMAND ----------

exit_message.append(f"notebook {stage['notebook_path']} execution completed  @ {datetime.now().isoformat()}")
exit_message = [i for i in exit_message if i] 
exit_message = '\n'.join(exit_message)
dbutils.notebook.exit(exit_message)