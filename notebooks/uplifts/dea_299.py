# Databricks notebook source
dbutils.widgets.text('db', '', 'db')
DB = dbutils.widgets.get('db')
assert DB

# COMMAND ----------

spark.sql(f"drop table if exists {DB}.processed_epma_descriptions")
spark.sql(f"drop table if exists {DB}.processed_epma_descriptions_temp")