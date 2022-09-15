"""Parse the dmd files to extract sub-files in json format"""
import os
import json
import logging
import glob
import xmltodict

# Use these flags to just convert certain files
DO_LOOKUP = True
DO_AMP = True
DO_VMP = True
DO_VTM = True
DO_AMP_PARSED = True
DO_VMP_PARSED = True

logging.basicConfig(level=logging.INFO)

# Change to suit your need
DATA_FOLDER = "nhsbsa_dmd_8.4.0_20220829000001"

if DO_LOOKUP:
    lookup_file = glob.glob(os.path.join(
        DATA_FOLDER,
        "f_lookup2_*.xml"
    ))[0]

    logging.info("Loading lookup file (%s)", lookup_file)

    with open(lookup_file, 'r') as f:
        data_dict = xmltodict.parse(f.read())

    data_dict = data_dict['LOOKUP']

    desired_lookups = {
        'FORM': 'form.json',
        'ROUTE': 'route.json',
        'UNIT_OF_MEASURE': 'unit_of_measure.json'
    }

    for lookup, filename in desired_lookups.items():
        logging.info("Creating %s file (%s)", lookup, filename)
        lookup_data = data_dict[lookup]['INFO']
        json_data = json.dumps(lookup_data)
        with open(filename, 'w', encoding='utf-8') as json_file:
            json_file.write(json_data)


if DO_AMP:
    amp_file_name = glob.glob(
        os.path.join(
            DATA_FOLDER,
            "f_amp2_*.xml"
        )
    )[0]

    logging.info("Loading AMP file (%s)", amp_file_name)


    with open(amp_file_name, 'r') as amp_file:
        amp_data_dict = xmltodict.parse(amp_file.read())

    amp_list = amp_data_dict['ACTUAL_MEDICINAL_PRODUCTS']['AMPS']['AMP']
    amp_json_data = json.dumps(amp_list)
    with open('amp.json', 'w', encoding='utf-8') as amp_json_file:
        amp_json_file.write(amp_json_data)



# VMP

if DO_VMP:
    vmp_file_name = glob.glob(
        os.path.join(
            DATA_FOLDER,
            "f_vmp2_*.xml"
        )
    )[0]

    logging.info("Loading VMP file (%s)", vmp_file_name)

    with open(vmp_file_name, 'r') as vmp_file:
        vmp_data_dict = xmltodict.parse(vmp_file.read())

    vmp_list = vmp_data_dict['VIRTUAL_MED_PRODUCTS']['VMPS']['VMP']
    vmp_json_data = json.dumps(vmp_list)
    with open('vmp.json', 'w', encoding='utf-8') as vmp_json_file:
        vmp_json_file.write(vmp_json_data)

if DO_VTM:
    vtm_file_name = glob.glob(
        os.path.join(
            DATA_FOLDER,
            "f_vtm2_*.xml"
        )
    )[0]

    logging.info("Loading VTM file (%s)", vtm_file_name)

    with open(vtm_file_name, 'r') as vtm_file:
        vtm_data_dict = xmltodict.parse(vtm_file.read())

    vtm_list = vtm_data_dict['VIRTUAL_THERAPEUTIC_MOIETIES']['VTM']
    vtm_json_data = json.dumps(vtm_list)
    with open('vtm.json', 'w', encoding='utf-8') as vtm_json_file:
        vtm_json_file.write(vtm_json_data)

if DO_AMP_PARSED:
    # Different source
    DATA_FOLDER = "UKDrugBonusFiles_34.2.0_20220831000001"
    amp_parsed_file_name = glob.glob(
        os.path.join(
            DATA_FOLDER,
            "f_amp2*_parsed.xml"
        )
    )[0]
    logging.info("Loading AMP parsed (%s)", amp_parsed_file_name)
    with open(amp_parsed_file_name, 'r') as f:
        amp_p_data_dict = xmltodict.parse(f.read())
    amp_p_list = amp_p_data_dict['ACTUAL_MEDICAL_PRODUCTS_PARSED']['AMP']
    amp_p_json_data = json.dumps(amp_p_list)
    with open('amp_parsed.json', 'w', encoding='utf-8') as amp_p_json_file:
        amp_p_json_file.write(amp_p_json_data)

if DO_VMP_PARSED:
    DATA_FOLDER = "UKDrugBonusFiles_34.2.0_20220831000001"
    vmp_parsed_file_name = glob.glob(
        os.path.join(
            DATA_FOLDER,
            "f_vmp2*_parsed.xml"
        )
    )[0]
    logging.info("Loading VMP parsed (%s)", vmp_parsed_file_name)
    with open(vmp_parsed_file_name, 'r') as f:
        vmp_p_data_dict = xmltodict.parse(f.read())

    vmp_p_list = vmp_p_data_dict['VIRTUAL_MEDICAL_PRODUCTS_PARSED']['VMP']
    vmp_p_json_data = json.dumps(vmp_p_list)
    with open('vmp_parsed.json', 'w', encoding='utf-8') as vmp_p_json_file:
        vmp_p_json_file.write(vmp_p_json_data)