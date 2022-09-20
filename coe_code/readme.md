# Experimental dm+d mapping

## Source files

Provided by AF. Two formats: Lloyds and Healthcare at home (HAH). HAH will be used throughout the following test.

## Software

Created by NHS Digital, available at [https://github.com/NHSDigital/medicines-text-mining-tool](https://github.com/NHSDigital/medicines-text-mining-tool)

The tool runs a multi-step pipeline in which a different matching technique is attempted at each step with anything that **couldn't** be matched in a previous step. The steps are:
1. exact matching
1. exact matching by previous name (where a name has changed)
1. entity matching
1. linked fuzzy matching
1. fuzzy matching

(more details on these can be found in the slides shared by JL)

## Platform

Azure databricks -- within the sandbox resource group of the AI CoE.

## Reference data

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
1. Accessed in the same way as (1), but must be extracted from the `lookup` file. Use `parse_dmd.py` to do this.
1. Access from [https://hscic.kahootz.com/connect.ti/t_c_home/view?objectId=14540272](https://hscic.kahootz.com/connect.ti/t_c_home/view?objectId=14540272) and convert with the `parse_dmd.py` file.

`parse_dmd.py` should work out of the box with just changes to the folder names for the two sets of files.

Once the script has produced .json files, they can be added to the database on databricks via `data -> add table`. Sometimes this method doesn't seem to recognise the header column, but this can be forced.

I named the tables as:
- `amp`
- `vmp`
- `vtm`
- `form`
- `unit_of_measure`
- `route`
- `amp_parsed`
- `vmp_parsed`

## Preparing query data

Queries need to be in a table with two columns: `medication_name_value` and `form_in_text`. The text to be matched should be in the first, and the second can be empty (it is for a particular use case that the original developers had where the form of medication (tablet, etc) was in a separate column).

This table also needs to be uploaded to databricks.

## Running the code

### Schema initialisation

Firstly, run the `init_schemas` notebook. The first cell will create various prompts at the top of the notebook. You will likely need to change the name of the database. This will probably be `default` unless you have changed it (look in the data tab).

### Unit tests [optional]

[optional] Run the unit tests. I found that one of the tests in the exact match testing failed for no apparent reason. To run the tests, I found I had to change the code as follows:
- Remove the block that tests if `ENV == 'ref'` from the cell that actually calls all the tests. In my databricks instance, `ENV` didn't exist.

The test that failed for me was `tests.test_exact_match.test_select_distinct_tokens`, but this didn't seem to effect the operation of the script.

### Running the pipeline

Various code changes are required to get the pipeline to run. Here are the changes I had to make:
1. Remove the line `assert MATCH_LOOKUP_FINAL_VERSION` from `run_notebooks.py`. The previous line was returning `None`. This doesn't seem to be important.
1. `notebooks/2_entity_extraction/drivers/entity_extraction_driver.py`: the line starting `DOSE_FORM_LIST_BC = sc.broadcast...` was referencing the table `dss_corporate.form`. `dss_corporate` is the db name for the original implementation. I had to change this to my db name (`default.form`)
1. `notebooks/3_fuzzy_matching/functions/fuzzy_match_functions.py`: the default args to `get_nom_moiety_words` referenced the original db. I had to replace three instances of `dss_corporate.` with `default.`
1. `notebooks/_modules/epma_global/ref_data_lib.py` The default argument to `__init__` method for the `ReferenceDataFormatter` class is the `dss_corporate` database. I had to change this to `default`.

With these changes done, various customisations are required in `run_notebooks.py` before running. In particular, within the cell that defines `PIPELINE_CONFIG` replace the names of the various tables with the names you setup when adding the reference tables. I put these in _without_ the database prefix. I.e. for me, `'vtm_table': 'vtm'` sufficed. I also commented out the final stage of the pipeline which computes accuracy as it was too fiddly to get data into the format in which accuracy could be easily calculated.

Finally, some parameters that appear as widgets at the top need to be changed:
1. `db` change this to the name of your db (e.g. `default`)
1. `BATCH_SIZE` (optional) parameter can be changed. The pipeline will _only_ process the first `BATCH_SIZE` records. However, it remembers what has been previously processed. If `BATCH_SIZE` is smaller than the number of records, the pipeline will need to be re-run multiple times.
1. `raw_input_table` the name of the table with the queries in it
1. `source_dataset` this should be set to `source_b`

## Output

Two key tables are produced by the pipeline:
1. `match_lookup_final` -- the found matches
1. `unmappable` -- any records that could not be matched

To download these tables as .csv run a cell with the following code (replace `"match_lookup_final"` with `"unmappable"` etc)

```python
match_table = spark.table("match_lookup_final")
display(match_table.select("*"))
```

and then click the three dots at the top left of the rendered table, and click "download all rows"

## Performance Evaluation - HAH

### Number of found matches

Within the example HAH file, there are 158 _unique_ descriptions.
Of these, the tool found matches for 153 (97%), and 5 (3%) were considered 'unmappable'.

The following table shows the number of matches at each level (AMP, VMP, VTM). A match at AMP implies matches at VMP and most VMP match to a VTM.

| level | number |
| --- | --- |
| VTM | 152 |
| VMP | 131 |
| AMP | 92 |

Note that one record has a VMP and no VTM. This is VMP: 4255711000001101 ("Ethyl chloride spray"). No idea why this doesn't have a VTM.

### Accuracy of found matches

96 of the 158 unique descriptions in the example HAH file have a legitimate dm+d code (codes starting `00000` are discarded). All of these codes correspond to AMPs. We can therefore assess the matches at AMP, VMP and VTM levels.

| match level | number correct | percentage correct |
| --- | --- | --- |
| AMP | 54 | 56% |
| **VMP** | **92** | **96%** |
| VTM | 95 | 99% |


### How were the matches found

Looking at the matches correct at the VMP level (92), the following table shows the number of matches found with each matching tool:

| tool | count | percent |
| --- | --- | --- |
| exact_by_name | 68 | 74% |
| exact_by_prev_name | 13 | 14% |
| entity | 6 | 7% |
| fuzzy_linked | 5 | 5% |

88% were due to name matching, or matching to a previous name.

## Conclusions

1. The tool can be run on data of the form provided by HAH.
1. The tool appears to have good coverage of the data provided (finding a match for 153 of the 158 unique descriptions (97%))
1. Where there was a dm+d code originally (AMP), the correct AMP was retreived 56% of the time.
1. Where there was a dm+d code originally (AMP), the correct VMP was retreived 96% of the time.
1. The majority of the matches come from the exact name matching tool either with the current, or a previous name.

## Performance evaluation - Lloyds

The unique names from the Lloyds file were extracted (with the rows containing `REGIME` removed). This resulted in 311 unique names.

These were run throught he pipeline (note: final match and unmappable tables were defined with `_lloyds` postfix to keep the results separate from the HAH results).

| Match level | Number | Percent |
| --- | --- | --- |
| APID | 2 | 0.6% |
| VPID | 2 | 0.6% |
| VTMID | 244 | 78% |
| Unmappable | 63 | 20% |

In comparison with the HAH results:

1. A large proportion could not be mapped to anything.
1. Of those that could be mapped, the majority are just at the VTM level, with only four names matchable at a more detailed level.

The output of the pipeline provides reasons for not being able to map. Here are the reasons, and the counts:

| Reason | Count |
| --- | --- |
| fuzzy_moiety_no_unique_vtm | 21 |
| fuzzy_linked_tied_confidence_score | 5 |
| fuzzy_moiety_low_score | 37 |

Briefly, these translate as:
1. `fuzzy_moiety_no_unique_vtm`: found matches, but the matches spanned multiple VTMs (i.e. too ambiguous).
1. `fuzzy_linked_tied_confidence_score`: best matches had tied score, so too ambiguous.
1. `fuzzy_moiety_low_score`: best match didn't reach the score threshold.