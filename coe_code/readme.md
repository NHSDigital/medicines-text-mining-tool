# Medicines Text Mining Tool
## AI CoE Instructions
### v0.1 December 2022

## Initial Setup

### Azure databricks

You should be able to access the `databricks-test` Azure Databricks Service (search for `databricks-test` on the Azure homepage).

Click on Compute and then on the "all" tab. If you cannot see any compute clusters, create a new one (the minimum specification is fine). If you want others to be able to access the cluster, ensure you choose the No Isolation Shared option in the Access Mode options.

### Code repository

You should be able to access the code via the repository under Simon's username. If not, it can be cloned from [https://github.com/simonrnss/medicines-text-mining-tool](https://github.com/simonrnss/medicines-text-mining-tool). It is crucial that you switch to the `azure_working` branch as it comtains fixes that are needed for the code to run.

### Reference data

**This is only required on initial setup and shouldn't be needed for running the tool unless a `dm+d` update is required.**

The code requires the following tables within the databricks workspace:

- `AMP` (1)
- `VMP` (1)
- `VTM` (1)
- `route` (2)
- `form` (2)
- `unit_of_measure` (2)
- `AMP_parsed` (3)
- `VMP_parsed` (3)

1. Accessed through [TRUD](https://isd.digital.nhs.uk/trud/users/guest/filters/0/home). Register and download the most recent release. Use the `parse_dmd.py` file to parse the xml and turn it into json that can be injested by databricks.
2. Accessed in the same way as (1), but must be extracted from the `lookup` file. Use `parse_dmd.py` to do this.
3. Access from [https://hscic.kahootz.com/connect.ti/t_c_home/view?objectId=14540272](https://hscic.kahootz.com/connect.ti/t_c_home/view?objectId=14540272) and convert with the `parse_dmd.py` file.

`parse_dmd.py` should work out of the box with just changes to the folder names for the two sets of files.

Once the script has produced .json files, they can be added to the database on databricks via `data -> add table`. Sometimes this method doesn't seem to recognise the header column, but this can be forced.

Name the tables as:
- `amp`
- `vmp`
- `vtm`
- `form`
- `unit_of_measure`
- `route`
- `amp_parsed`
- `vmp_parsed`

Take a note of the names as you will need them for a later step.

## Formatting Input data

**Assumimg that the input is provided as a .csv file.**

The python script `coe_code/parse_csv.py` can be used to convert the input file into a file that can be uploaded to databricks.

Usage:

```bash
python coe_code/parse_csv.py -i <input_csv_filename> -o <output_csv_filename> -t lloyds -c <column index of name column>
```

The column index should start at zero. I.e. if the name column in the original file is in the first column, the option `-c 0` should be used.

The resulting .csv file will have two columns. The first (heading = `medication_name_value`) will contain all of the unique names from the input file. The second (heading = `name_in_text`) will be empty (this is expected).

## Upload to Azure Databricks

The input data as prepared above needs to be added as a table into the `default` database on databricks.

1. Click the data icon from the left-hand menu and then click Create Table.
1. Drag the .csv file and then, once the file has uploaded, click on Create Table with UI button.
1. Select a working cluster to use to preview the table and then click Preview.
1. Choose the options in the resulting form that ensure the .csv has been correctly loaded. Ensure that the table has a sensible name. **Note**: you may need to click the First Row is Header checkbox.
1. Note down the table name you have chosen for use later.
1. When finished click Create Table.

## Run the notebooks
1. Click repos from the menu on the left
1. Select Simon's 'medicines-text-mining-tool' repository (if it is not visible, you will need to clone the repo [https://github.com/simonrnss/medicines-text-mining-tool](https://github.com/simonrnss/medicines-text-mining-tool))
1. Ensure you are on the branch `azure_working`
1. Click init_schemas
1. Connect the notebook to a running compute cluster
1. The notebook will open. This notebook creates the tables in which the scripts will store the outputs. You can set the names in the fields at the top. For example:
   - `accuracy_table_name` = `accuracy` (will probably be the default)
   - `db` = `default` (will probably be the default)
   - `match_lookup_final_table_name` = `match_lookup_final_lloyds_20221206` (for example, set as appropriate. This is where the found matches will be stored).
   - `unmappable_table_name` = `unmappable_lloyds_20221206` (for example, set as appropriate. This is where the unmappable names will be stored).
   - `uplift_notebook` = `/notebooks/uplifts/dea_417` (will be default, leave unchanged)
   - `uplift_table` = `match_lookup_final_lloyds_20221206` (normally set to match the `match_lookup_final_table`)
1. Run the init_schemas script (shift+enter in each cell).
1. Click repos from the menu on the left
1. Return to the repository and open run_notebooks
1. The notebook will open.  In the fields at the top change:
   -  `batch size` to an appropriate number depending on the number of records in the file. In previous experiments, setting this to a number higher than the total number of input records works fine.
   -  `db` = `default` (will probably be the default)
   -  `notebook_root` = `.` (will probably be the default)
   -  `raw_input_table` to the name of the table created to hold the input data (see above).
   -  `source_dataset` = `source_b` (will probably be the default, do not change)
1. Move to the fourth cell in which `MATCH_LOOKUP_FINAL_TABLE` and `UNMAPPABLE_TABLE` are defined. Modify these lines to point to the tables created in the `init_schemas` notebook. E.g. if `match_lookup_final_table_name` was set to `match_lookup_final_lloyds_20221206` then change the line to:

    `MATCH_LOOKUP_FINAL_TABLE = f'{DB}.match_lookup_final_lloyds_20221206'`

    and do the same for `UNMAPPABLE_TABLE`. 
1. Run the run_notebooks script (shift+enter in each cell). It will take around 20 minutes to complete for ~500 names.
1. Cells 8 & 9 of the `run_notebooks` script display the tables with matching and unmapped descriptions.  In both cells change the name of the source table to match the names of the matching and unmappable tables used above e.g.:
   
   ```python
   match_table = spark.table("match_lookup_final_lloyds_20221206")
   display(match_table.select("*"))
   ```
   ```python
   unmap_table = spark.table("unmappable_lloyds_20221206")
   display(unmap_table.select("*"))
   ```
   Once a table is displayed it can be downloaded locally as a .csv file.
   
   Click the 'Download all rows' icon at the bottom of the cell and the file will be saved to your local 'downloads' folder.


The matched output table has the following columns:

1. Mapped names table:
   - `original_epma_description` The original name that was matched
   - `form_in_text` From the input file (blank in our case)
   - `match_id` The dm+d ID for the match
   - `match_term` The dm+d name for the match
   - `id_level` Which level the match has been made at (e.g. VTMID, VMPID, AMPID)
   - `match_level` Which tool was used for the match.
   - `match_datetime` When the match was made
   - `version_id` Unknown -- value seems to be null.
   - `run_id` An autogenerated run ID.
1. Unmapped names table:
   - `original_epma_description` The original name that was matched
   - `form_in_text` From the input file (blank in our case)
   - `reason` Why a match wasn't possible
   - `match_datetime` When the match was made
   - `run_id` An autogenerated run ID.


