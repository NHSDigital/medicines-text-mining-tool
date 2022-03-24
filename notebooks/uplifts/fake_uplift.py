# Databricks notebook source
# MAGIC %run ../_modules/epma_global/functions

# COMMAND ----------

dbutils.widgets.text('db', '', 'db')
DB = dbutils.widgets.get('db')
assert DB

dbutils.widgets.text('uplift_table_name', '', 'uplift_table_name')
UPLIFT_TABLE_NAME = dbutils.widgets.get('uplift_table_name')
assert UPLIFT_TABLE_NAME

# COMMAND ----------

print(UPLIFT_TABLE_NAME)

# COMMAND ----------

df_uplift_table = spark.createDataFrame([
  (1, 'description_1'),
  (2, 'description_1')
], ['epma_id', 'epma_description'])

# COMMAND ----------

create_table(df_uplift_table, UPLIFT_TABLE_NAME, overwrite=True)