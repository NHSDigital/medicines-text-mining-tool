'''
Script to expand a matched csv file from the medicines-text-mining-tool so each AMP also has a
VMP and each VMP has a VTM added to the file
'''

# System imports
import logging
import os
import glob

# Third Party imports
import argparse
import pandas as pd
import numpy as np
import xmltodict

logging.basicConfig(level=logging.INFO)
# Setup file-specific logging
local_logger = logging.getLogger(__name__)

# argparse arguements
parser = argparse.ArgumentParser()
parser.add_argument(
    "-i", "--input-file",
    type=str,
    required=True,
    action="store",
    dest="input_file",
    help="Name of the <Input> .csv file that you want to add additional info to."
)
parser.add_argument(
    "-o", "--output-file",
    type=str,
    required=True,
    action="store",
    dest="output_file",
    help="Name given to <Output> .csv file storing the results with the additional info added."
)
parser.add_argument(
    "-d", "--path-to-dmd-xml-files",
    type=str,
    required=True,
    action="store",
    dest="xml_files_path",
    help="Path to xml files that hold the dm+d code mappings."
)
args = parser.parse_args()
print(args)


def get_amp_dict() -> dict:
    ''' Function loads the AMP .xml file, and converts to a single dictionary where each key
    is the APID and the value is the dictionary of other values. '''

    local_logger.info("Loading AMP data.")
    amp_filename = glob.glob(os.path.join(args.xml_files_path, 'f_amp2_*.xml'))[0]
    with open(
        amp_filename,
        'r',
        encoding='utf-8'
        ) as file:
        amp_data = xmltodict.parse(file.read())

    # Use list comprehension to convert list of dictionaries into a single dictionary
    amp_dict = {d['APID']: d for d in amp_data['ACTUAL_MEDICINAL_PRODUCTS']['AMPS']['AMP']}

    return amp_dict


def get_vmp_dict() -> dict:
    ''' Function loads the VMP .xml file, and converts to a single dictionary where each key
    is the VPID and the value is the dictionary of other values. '''

    local_logger.info("Loading VMP data.")
    with open(
        os.path.join(args.xml_files_path,'f_vmp2_3250822.xml'),
        'r',
        encoding='utf-8'
        ) as file:
        vmp_data = xmltodict.parse(file.read())

    # Use list comprehension to convert list of dictionaries into a single dictionary
    vmp_dict = {d['VPID']: d for d in vmp_data['VIRTUAL_MED_PRODUCTS']['VMPS']['VMP']}

    return vmp_dict


def get_vtm_dict() -> dict:
    ''' Function loads the VTM .xml file, and converts to a single dictionary where each key
    is the VTMID and the value is the dictionary of other values. '''

    local_logger.info("Loading VTM data.")
    with open(
        os.path.join(args.xml_files_path, 'f_vtm2_3250822.xml'),
        'r',
        encoding='utf-8'
        ) as file:
        vtm_data = xmltodict.parse(file.read())

    # Use list comprehension to convert list of dictionaries into a single dictionary
    vtm_dict = {d['VTMID']: d for d in vtm_data['VIRTUAL_THERAPEUTIC_MOIETIES']['VTM']}

    return vtm_dict


def load_and_pivot_input() -> pd.DataFrame:
    ''' Function loads a .csv matched input file from the medicines-text-mining-tool and pivots
    the dataframe '''

    # Use pandas to load the .csv matched input file from the medicines-text-mining-tool
    local_logger.info("Loading input dataframe - %s.", args.input_file)
    with open(args.input_file, 'r', encoding='utf-8') as file:
        results = pd.read_csv(file, dtype=str)

    # Pivot results to make a new column for each unique entry in the id_level column
    # (APID, VPID, VMTID) and add the match_id value in the correct one
    pivoted_results = results.pivot(
        index=['original_epma_description', 'match_term', 'match_level'],
        columns='id_level',
        values='match_id'
        )

    return pivoted_results


# TODO: DELETE - just to help my understanding of how amp_dict is structured
# my_amp_dict = get_amp_dict()
# # e.g. using APID 526311000001106 to get thw dictionary of values for that APID
# print(my_amp_dict['526311000001106'])
# # e.g. using APID 526311000001106 to get the VPID
# print(my_amp_dict['526311000001106']['VPID'])


# TODO: DELETE - just to help my understanding of how vmp_dict is structured
# my_vmp_dict = get_vmp_dict()
# # e.g. using VPID 40962811000001105 to get the dictionary of values for that VPID
# print(my_vmp_dict['40962811000001105'])
# # e.g. using VPID 40962811000001105 to get the VTMID
# print(my_vmp_dict['40962811000001105']['VTMID'])


# TODO: DELETE - just to help my understanding of how vtm_dict is structured
# my_vtm_dict = get_vtm_dict()
# # e.g. using VTMID 68088000 to get the dictionary of values for that VTMID
# print(my_vtm_dict['68088000'])
# # e.g. using VTMID 68088000 to get the NM
# print(my_vtm_dict['68088000']['NM'])


def get_vmp_from_amp(apid, amp_dict):
    ''' Function to look up the VPID corresponding to an APID '''

    if apid in amp_dict:
        return amp_dict[apid].get('VPID', np.nan)


# TODO: DELETE - just checking get_vmp_from_amp() works with a valid APID
# test = get_vmp_from_amp('526311000001106', my_amp_dict)
# print(test)


def get_vtm_from_vmp(vpid, vmp_dict):
    ''' Function to look up the VPID and return a VTMID '''

    if vpid in vmp_dict:
        return vmp_dict[vpid].get('VTMID', np.nan)


# TODO: DELETE - just checking get_vtm_from_vmp() works with a valid VPID
# test = get_vtm_from_vmp('40962811000001105', my_vmp_dict)
# print(test)


def add_code_description(code, code_dict, d_name):
    ''' Function takes a code, a code dictionary and a description name as input and
    looks for a code in the code dictionary returning a description of the code if found
    or returning NaN if not found '''

    if code in code_dict:
        return code_dict[code][d_name]
    else:
        return np.nan


def add_addiitional_info():
    ''' Function loads and pivots the input .csv file, loads code dictionaries, loops through the
    codes and adds additional codes where a match is found.  Additional columns are also added to
    the dataframe with code descriptions '''

    # Load the .csv matched input file from the medicines-text-mining-tool
    pivoted_input = load_and_pivot_input()

    # Load the AMP dictionary
    amp_dict = get_amp_dict()

    local_logger.info("Searching APIDs and adding VPIDs.")
    for i in pivoted_input.index:
        apid = pivoted_input.loc[i, 'APID']
        if apid is not np.nan:
            vpid = get_vmp_from_amp(apid, amp_dict)
            pivoted_input.loc[i,'VPID'] = vpid

    # Load the VMP dictionary
    vmp_dict = get_vmp_dict()

    local_logger.info("Searching VPIDs and adding VTMIDs.")
    for i in pivoted_input.index:
        vpid = pivoted_input.loc[i, 'VPID']
        if vpid is not np.nan:
            vtm = get_vtm_from_vmp(vpid, vmp_dict)
            pivoted_input.loc[i, 'VTMID'] = vtm

    # Load the VTM dictionary
    vtm_dict = get_vtm_dict()

    local_logger.info("Adding code descriptions.")
    # Add code descriptions as new columns using the mapping dictionaries
    pivoted_input['AMP_desc'] = pivoted_input.apply(
        lambda row: add_code_description(row['APID'], amp_dict, 'DESC'),
        axis=1
        )
    pivoted_input['VMP_desc'] = pivoted_input.apply(
        lambda row: add_code_description(row['VPID'], vmp_dict, 'NM'),
        axis=1
        )
    pivoted_input['VTM_desc'] = pivoted_input.apply(
        lambda row: add_code_description(row['VTMID'], vtm_dict, 'NM'),
        axis=1
        )

    return pivoted_input


def main():

    # Run function to add additional info to input dataframe
    output = add_addiitional_info()

    # Write the final dataframe to a new file
    output.to_csv(args.output_file)
    local_logger.info("Output saved as %s", args.output_file)


if __name__ == '__main__':
    main()
