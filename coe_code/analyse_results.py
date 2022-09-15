# %%
from typing import Dict
import logging
import csv
import glob
import xmltodict
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO)


# %%
orig_data = pd.read_csv('hah_example_out.csv', dtype={'dm+d': str})
orig_data['drug_name'] = orig_data['name']
orig_data = orig_data.iloc[:-1, :]
orig_data.drop('name', axis=1, inplace=True)
orig_data['drug_name'] = orig_data.apply(
    lambda row: row['drug_name'].lower(), axis=1
)
orig_data['orig_APID'] = orig_data['dm+d']
orig_data.drop('dm+d', axis=1, inplace=True)

orig_data['orig_VPID'] = orig_data.apply(
    lambda row: get_vmp_from_amp(row['orig_APID'], amp_dict),
    axis=1
)
orig_data['orig_VTMID'] = orig_data.apply(
    lambda row: get_vtm_from_vmp(row['orig_VPID'], vmp_dict),
    axis=1
)
# %%
results_data = pd.read_csv('hah_results.csv', dtype={'match_id': object})
# %%
joined_data = pd.merge(results_data, orig_data, how='left', left_on="original_epma_description", right_on='drug_name')
joined_data['precise_match'] = joined_data.apply(
    lambda row: 1 if row['match_id'] == row['orig_APID'] else 0,
    axis=1
)
joined_data['precise_match'].sum()
# %%
