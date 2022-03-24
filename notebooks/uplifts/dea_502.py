# Databricks notebook source
spark.sql('ALTER TABLE epma_autocoding.match_lookup_final RENAME TO epma_autocoding.match_lookup_final_2021_12_23')
spark.sql('ALTER TABLE epma_autocoding.unmappable RENAME TO epma_autocoding.unmappable_2021_12_23')
spark.sql('ALTER TABLE epma_autocoding.accuracy RENAME TO epma_autocoding.accuracy_2021_12_23')