# Databricks notebook source
spark.sql('ALTER TABLE epma_autocoding.match_lookup_final RENAME TO epma_autocoding.match_lookup_final_2022_02_10')
spark.sql('ALTER TABLE epma_autocoding.unmappable RENAME TO epma_autocoding.unmappable_2022_02_10')
spark.sql('ALTER TABLE epma_autocoding.accuracy RENAME TO epma_autocoding.accuracy_2022_02_10')