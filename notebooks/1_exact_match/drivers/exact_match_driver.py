# Databricks notebook source
# MAGIC %run ../../_modules/epma_global/functions

# COMMAND ----------

# MAGIC %run ../../_modules/epma_global/constants

# COMMAND ----------

# MAGIC %run ../functions/exact_match_functions

# COMMAND ----------

# MAGIC %run ../../_modules/epma_global/ref_data_lib

# COMMAND ----------

import os
from datetime import datetime

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text('notebook_location', './1_exact_match/drivers/exact_match_driver', 'notebook_location')
dbutils.widgets.text('input_table', '', 'input_table')
dbutils.widgets.text('output_table', '', 'output_table')
dbutils.widgets.text('match_table', '', 'match_table')

stage = {
  'notebook_path' : os.path.basename(dbutils.widgets.get('notebook_location')),
  'input_table' : dbutils.widgets.get('input_table'),
  'output_table' : dbutils.widgets.get('output_table'),
  'match_table' : dbutils.widgets.get('match_table'),
}

# COMMAND ----------

exit_message = []
exit_message.append(f"\n notebook {stage['notebook_path']} execution started  @ {datetime.now().isoformat()}\n")

# COMMAND ----------

df_epma = get_data(stage['input_table'])

# COMMAND ----------

df_vtm = RefDataStore.vtm.select(RefDataStore.ID_COL, RefDataStore.TEXT_COL, RefDataStore.ID_LEVEL_COL)
df_vmp = RefDataStore.vmp.select(RefDataStore.ID_COL, RefDataStore.TEXT_COL, RefDataStore.ID_LEVEL_COL)
df_amp = RefDataStore.amp.select(RefDataStore.ID_COL, RefDataStore.TEXT_COL, RefDataStore.ID_LEVEL_COL)
df_vmp_prev = RefDataStore.vmp_prev.where(col('NMPREV').isNotNull()) \
                                   .withColumn(RefDataStore.TEXT_COL, col('NMPREV')) \
                                   .select(RefDataStore.ID_COL, RefDataStore.TEXT_COL, RefDataStore.ID_LEVEL_COL)
df_amp_prev = RefDataStore.amp_prev.select(RefDataStore.ID_COL, RefDataStore.TEXT_COL, RefDataStore.ID_LEVEL_COL)

# COMMAND ----------

TOKEN_COL = 'token'

df_vtm_process = select_distinct_tokens(df_vtm, RefDataStore.TEXT_COL, TOKEN_COL)
df_vmp_process = select_distinct_tokens(df_vmp, RefDataStore.TEXT_COL, TOKEN_COL)
df_amp_process = select_distinct_tokens(df_amp, RefDataStore.TEXT_COL, TOKEN_COL)
df_vmp_process_prevName = select_distinct_tokens(df_vmp_prev, RefDataStore.TEXT_COL, TOKEN_COL)
df_amp_process_prevName = select_distinct_tokens(df_amp_prev, RefDataStore.TEXT_COL, TOKEN_COL)

# COMMAND ----------

df_input = df_epma.select(ID_COL, ORIGINAL_TEXT_COL, TEXT_COL, FORM_IN_TEXT_COL, TEXT_WITHOUT_FORM_COL)

# Perform exact matching twice. Once for the epma description with form_in_text, and once for the epma description without form_in_text.
# Later stages of the pipeline only attempt to match to the epma description with form_in_text, for speed. So the column TEXT_WITHOUT_FORM_COL is discarded after this.

df_output_matched = None

for col_to_be_tokenized in [TEXT_COL, TEXT_WITHOUT_FORM_COL]:

  df_input_process = select_distinct_tokens(df_input, col_to_be_tokenized, TOKEN_COL)
  
  df_matched, df_non_matched = get_exact_matches(df_input_process, 
                                                 df_vtm_process, 
                                                 df_vmp_process, 
                                                 df_amp_process,
                                                 df_vmp_process_prevName,
                                                 df_amp_process_prevName, 
                                                 id_col=ID_COL,
                                                 original_text_col=ORIGINAL_TEXT_COL,
                                                 text_col=TEXT_COL,
                                                 form_in_text_col=FORM_IN_TEXT_COL,
                                                 text_without_form_col=TEXT_WITHOUT_FORM_COL,
                                                 match_id_col=MATCH_ID_COL,
                                                 match_level_col=MATCH_LEVEL_COL,
                                                 match_datetime_col=MATCH_DATETIME_COL,
                                                 token_col=TOKEN_COL,
                                                 id_level_col=ID_LEVEL_COL,
                                                 ref_id_col=RefDataStore.ID_COL)
  df_output_matched = df_matched if df_output_matched is None else df_output_matched.union(df_matched)
  
  # if the source data does not contain a form_in_text column, then TEXT_WITHOUT_FORM_COL will be empty, in which case don't go round the loop a second time.
  if df_input.filter(col(TEXT_WITHOUT_FORM_COL).isNotNull()).count()==0:
    break
  df_input = df_non_matched
  
df_output_non_matched = df_non_matched

# COMMAND ----------

df_output_matched = df_output_matched.select(ID_COL, ORIGINAL_TEXT_COL, FORM_IN_TEXT_COL, TEXT_COL, MATCH_ID_COL, ID_LEVEL_COL, MATCH_LEVEL_COL, MATCH_DATETIME_COL)

df_output_non_matched = df_output_non_matched.select(ID_COL, ORIGINAL_TEXT_COL, FORM_IN_TEXT_COL, TEXT_COL)

# COMMAND ----------

create_table(df_output_non_matched, stage['output_table'], overwrite=True)
create_table(df_output_matched, stage['match_table'], overwrite=True)

# COMMAND ----------

exit_message.append(f"notebook {stage['notebook_path']} execution completed  @ {datetime.now().isoformat()}")
exit_message = [i for i in exit_message if i] 
exit_message = '\n'.join(exit_message)
dbutils.notebook.exit(exit_message)