# Databricks notebook source
spark.sql('ALTER TABLE epma_autocoding.match_lookup_final RENAME TO epma_autocoding.match_lookup_final_old')
spark.sql('ALTER TABLE epma_autocoding.unmappable RENAME TO epma_autocoding.unmappable_old')
spark.sql('ALTER TABLE epma_autocoding.accuracy RENAME TO epma_autocoding.accuracy_old')