# Databricks notebook source
from enum import Enum

import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit
from pyspark.sql import DataFrame, Window

# COMMAND ----------

class RefDataset(Enum):
  AMP = 'amp'
  VMP = 'vmp'
  VTM = 'vtm'
  AMP_PARSED = 'amp_parsed'
  VMP_PARSED = 'vmp_parsed'

  
class ReferenceDataFormatter():
  '''
  Class that loads and holds reference datasets. Different preprocessing are applied to some of the ref datasets and multiple copies 
  are provided for different purposes.  
  '''

  ID_COL = 'ref_id'
  TEXT_COL = 'ref_description'
  ID_LEVEL_COL = 'id_level'
  
  def __init__(self, ref_database: str='dss_corporate'):
    self._ref_database = ref_database
    
    self._amp = None
    self._amp_prev = None
    self._amp_dedup = None
    
    self._vmp = None
    self._vmp_prev = None
    
    self._vtm = None
    self._vtm_prev = None
    
    self._amp_parsed = None
    self._vmp_parsed = None
    
  @property
  def amp(self):
    '''
    Returns the cleand AMP ref data.
    
    Field mappings:
    APID -> id
    NM ->TEXT_COL
    '''
    if self._amp is None:
      df = self._load_dataset(RefDataset.AMP)
      self._amp = df.withColumn(self.ID_COL, col('APID')) \
                    .withColumn('NM', F.regexp_replace(col('NM'), r'\bmr\b', 'modified-release')) \
                    .withColumn('NM', F.regexp_replace(col('NM'), r'\bsr\b', 'sustained release')) \
                    .select(self.ID_COL, col('NM').alias(self.TEXT_COL), 'APID', 'VPID') \
                    .withColumn(self.ID_LEVEL_COL, lit('APID'))
    return self._amp

  @property
  def amp_prev(self):
    '''
    Returns the previous AMP ref data where the text has changed.
    
    Field mappings:
    APID -> id
    NM_PREV -> TEXT_COL
    '''
    if self._amp_prev is None:
      df = self._load_dataset(RefDataset.AMP)
      self._amp_prev = df.where(col('NM_PREV').isNotNull()) \
                         .withColumnRenamed('NM_PREV', 'NMPREV') \
                         .withColumn(self.ID_COL, col('APID')) \
                         .withColumn('NMPREV', F.regexp_replace(col('NMPREV'), r'\bmr\b', 'modified-release')) \
                         .withColumn('NMPREV', F.regexp_replace(col('NMPREV'), r'\bsr\b', 'sustained release')) \
                         .select(self.ID_COL, col('NMPREV').alias(self.TEXT_COL), 'APID', 'VPID') \
                         .withColumn(self.ID_LEVEL_COL, lit('APID'))
    return self._amp_prev

  @property
  def amp_dedup(self):
    '''
    From the AMP ref data, deduplicate on the NM field, where the records with the lowest APID and lowest VPID are selected.
    
    Field mappings:
    VPID -> id
    NM -> TEXT_COL 
    '''
    if self._amp_dedup is None:   
      # Dedup by the text column and take the record with the lowest APID and then lowest VPID if there are duplicates
      window_by_text_lowest_apid_lowest_vpid = Window.partitionBy('NM').orderBy(col('_apid_long').asc(), col('_vpid_long').asc())
      df = self._load_dataset(RefDataset.AMP)
      self._amp_dedup = df.withColumn('_apid_long', col('APID').cast('long')) \
                          .withColumn('_vpid_long', col('VPID').cast('long')) \
                          .withColumn('_rn', F.row_number().over(window_by_text_lowest_apid_lowest_vpid)) \
                          .where(col('_rn') == 1) \
                          .drop('_rn', '_apid_long', '_vpid_long') \
                          .withColumn(self.ID_COL, col('APID')) \
                          .withColumn('NM', F.regexp_replace(col('NM'), r'\bmr\b', 'modified-release')) \
                          .withColumn('NM', F.regexp_replace(col('NM'), r'\bsr\b', 'sustained release')) \
                          .select(self.ID_COL, col('NM').alias(self.TEXT_COL), 'APID', 'VPID') \
                          .withColumn(self.ID_LEVEL_COL, lit('APID'))
    return self._amp_dedup

  @property
  def vmp(self):  
    '''
    Returns the cleand VMP ref data. Raises an assertion error if there are (VTMID, NM) duplicates.
    
    Field mappings:
    VPID -> id
    NM ->TEXT_COL
    '''
    if self._vmp is None:
      df = self._load_dataset(RefDataset.VMP)
      self._vmp = df.withColumn(self.ID_COL, col('VPID')) \
                    .withColumn('NM', F.regexp_replace(col('NM'), r'\bmr\b', 'modified-release')) \
                    .withColumn('NM', F.regexp_replace(col('NM'), r'\bsr\b', 'sustained release')) \
                    .select(self.ID_COL, col('NM').alias(self.TEXT_COL), 'VTMID') \
                    .withColumn(self.ID_LEVEL_COL, lit('VPID'))
      if self._vmp.count() != self._vmp.drop_duplicates(['VTMID', self.TEXT_COL]).count():
        raise AssertionError('There are duplicate entries of pair (VTMID, NM) in the VMP ref data.')
    return self._vmp
  
  @property
  def vmp_prev(self):
    '''
    Returns the previous names and IDs of VMP data. The names and IDs can change independently, so both samples 
    are included in the returned dataframe.
    
    Field mappings:
    VPID -> id
    NM -> TEXT_COL
    '''
    if self._vmp_prev is None:
      df = self._load_dataset(RefDataset.VMP)
      self._vmp_prev = df.where((col('NMPREV').isNotNull()) | (col('VPIDPREV').isNotNull())) \
                         .withColumn(self.ID_COL, col('VPID')) \
                         .withColumn('NM', F.regexp_replace(col('NM'), r'\bmr\b', 'modified-release')) \
                         .withColumn('NM', F.regexp_replace(col('NM'), r'\bsr\b', 'sustained release')) \
                         .select(self.ID_COL, col('NM').alias(self.TEXT_COL), 'VTMID', 'NMPREV', 'VPIDPREV') \
                         .withColumn(self.ID_LEVEL_COL, lit('VPID'))
    return self._vmp_prev

  @property
  def vtm(self):  
    '''
    Returns the cleand VTM ref data. Raises an assertion error if there are duplicates
    
    Field mappings:
    VTMID -> id
    NM ->TEXT_COL
    '''
    if self._vtm is None:
      df = self._load_dataset(RefDataset.VTM)
      self._vtm = df.withColumn(self.ID_COL, col('VTMID')) \
                    .select(self.ID_COL, col('NM').alias(self.TEXT_COL)) \
                    .withColumn(self.ID_LEVEL_COL, lit('VTMID'))
      if self._vtm.count() != self._vtm.drop_duplicates().count():
        raise AssertionError('There are duplicate entries in the VTM ref data.')
    return self._vtm
  
  @property
  def vtm_prev(self):
    '''
    Returns the previous VTM ref data where the ID has changed.
    
    Field mappings:
    VTMIDPREV -> id
    NM -> TEXT_COL
    '''
    if self._vtm_prev is None:
      df = self._load_dataset(RefDataset.VTM)
      self._vtm_prev = df.where(col('VTMIDPREV').isNotNull()) \
                         .withColumn(self.ID_COL, col('VTMIDPREV')) \
                         .select(self.ID_COL, col('NM').alias(self.TEXT_COL), 'VTMID') \
                         .withColumn(self.ID_LEVEL_COL, lit('VTMID'))
    return self._vtm_prev

  @property
  def amp_parsed(self):
    '''
    Returns the parsed AMP ref data.
    
    Field mappings:
    APID -> id
    TERM -> TEXT_COL
    
    Orange, blackcurrant, etc, are more likely to be flavours than active ingredients, so we don't want to use them as moieties.
    '''
    if self._amp_parsed is None:
      df = self._load_uncleaned_dataset(RefDataset.AMP_PARSED)
      self._amp_parsed = df.withColumn(self.ID_COL, col('APID')) \
                           .withColumn(self.TEXT_COL, col('TERM')) \
                           .drop('APID', 'TERM') \
                           .where(~F.lower(col('MOIETY')).isin(['blackcurrant', 'orange', 'raspberry', 'peppermint', 'syrup']))
    return self._amp_parsed
 
  @property
  def vmp_parsed(self):
    '''
    Returns the parsed VMP ref data.
    
    Field mappings:
    VPID -> id
    TERM -> TEXT_COL
    
    Orange, blackcurrant, etc, are more likely to be flavours than active ingredients, so we don't want to use them as moieties.
    '''
    if self._vmp_parsed is None:
      df = self._load_uncleaned_dataset(RefDataset.VMP_PARSED)
      self._vmp_parsed = df.withColumn(self.ID_COL, col('VPID')) \
                           .withColumn(self.TEXT_COL, col('TERM')) \
                           .drop('VPID', 'TERM') \
                           .where(~F.lower(col('MOIETY')).isin(['blackcurrant', 'orange', 'raspberry', 'peppermint', 'syrup']))
    return self._vmp_parsed

  def _clean_ref_data(self, df: DataFrame) -> DataFrame:
    df = df.where(col('INVALID').isNull())

    for text_field in ['NM', 'NMPREV', 'NM_PREV']:
      if text_field in df.schema.names:
        df = df.withColumn(text_field, F.lower(col(text_field)))

    return df

  def _load_dataset(self, dataset_name: RefDataset) -> DataFrame:
     return self._clean_ref_data(
       spark.table(f'{self._ref_database}.{dataset_name.value}'))

  def _load_uncleaned_dataset(self, dataset_name: RefDataset) -> DataFrame:
    return spark.table(f'{self._ref_database}.{dataset_name.value}')
      

# COMMAND ----------

RefDataStore = ReferenceDataFormatter()