# Databricks notebook source
spark.sql('DROP TABLE IF EXISTS epma_autocoding.batched_cleaned_pipeline_inputs')
spark.sql('DROP TABLE IF EXISTS epma_autocoding.entity_non_match')
spark.sql('DROP TABLE IF EXISTS epma_autocoding.exact_non_match')
spark.sql('DROP TABLE IF EXISTS epma_autocoding.fuzzy_non_linked')
spark.sql('DROP TABLE IF EXISTS epma_autocoding.fuzzy_nonlinked_non_match_output')
spark.sql('DROP TABLE IF EXISTS epma_autocoding.match_lookup')
spark.sql('DROP TABLE IF EXISTS epma_autocoding.fuzzy_non_match')