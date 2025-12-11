# User Config

| Parameter | Description | Data Type | Acceptable Values |
|---|---|---|---|
| bucket | The path to the bucket. | string | Any filepath. |
| calibration_group_map_path | The filepath to the calibration group mapping file. | string | Any filepath. |
| classification_values_path | The filepath to the file containing SIC classification values. | string | Any filepath. |
| idbr_folder_path | The path to the folder containing input data. | string | Any filepath. |
| l_values_path | The filepath to the file containing l values. | string | Any filepath. |
| manual_outlier_path | The filepath to the file containing manual outliers data. | string | Any filepath. |
| snapshot_file_path | The filepath to the input data. | string | Any filepath. |
| manual_constructions_path | The filepath to the file containing manual constructions data. | string or null | Any filepath or null. |
| filter | The filepath to the file containing filter data. | string or null | Any filepath or null. |
| output_path | The filepath where outputs should be saved to. | string | Any filepath. |
| population_prefix | The prefix text for population frame data. | string | Any text. |
| sample_prefix | The prefix text for sample frame data. | string | Any text. |
| back_data_qv_path | The filepath for the file containing QV backdata. | string | Any filepath. |
| back_data_cp_path | The filepath for the file containing CP backdata. | string | Any filepath. |
| back_data_qv_cp_json_path | The filepath for the file containing the backdata snapshot. | string | Any filepath. |
| back_data_finalsel_path | The filepath for the file containing final selection backdata. | string | Any filepath. |
| current_period | The most recent period to include in the outputs. | int | Any int in the form `yyyymm`. |
| revision_window | The number of months to use as a revision window. | int | Any int in the form `mm` or `m` (does not need to be zero-padded). |
| generate_schemas | Whether to generate schema files. | bool | Either `true` or `false`. |
| schema_path | The path to where the schema files are stored. | string | Any valid filepath. |
| debug_mode | Whether to export all the intermediate methods outputs (imputation, estimation, winsorisation). | bool | Either `true` or `false`. |

## Guidance for use
As an end user, you will only need to change the user config (named `config_user.json`) - you just need to update the filepaths and period information in the user config. Note: for ONS users, you can find example filepaths in the Confluence documentation.

# Config outputs
| Parameter | Description | Data Type | Acceptable Values |
|---|---|---|---|
| bucket | The path to the bucket. | string | Any filepath. |
| idbr_folder_path | The path to the folder containing the IDBR data. | string | Any filepath. |
| snapshot_file_path | The full filepath to the snapshot data | string | Any filepath. |
| cons_output_path | The filepath to the file containing the methods output. | string | Any filepath. |
| output_path | The filepath where outputs should be saved to. | string | Any filepath. |
| ludets_prefix | The prefix for local unit files. | string | Any text. |
| current_period | The most recent period to include in the outputs (same as above). | int | Any int in the form `yyyymm`. |
| revision_window | The number of months to use as a revision window. | int | Any int in the form `mm` or `m` (does not need to be zero-padded). |
| region_mapping_path | The filepath to the region mapping file. | string | Any filepath. |
| r_and_m_quarter | The quarter to use to produce regional R&M extracts. Must be in 'YYYYQX' format, e.g., '2023Q1' | string or null | `"YYYYQX"` or null |
| sizeband_quarter | A list of optional quarters to filter the quarterly_by_sizeband_output on. | list | A list containing any quarter in the format `YYYYQX` (e.g. ["2023Q2"]) or an empty list. |
| imputation_contribution_periods | A list of optional periods to filter the imputation_contribution_output on. | list | Any period in the format `YYYYMM` (e.g. ["202201"]) or an empty list. |
| devolved_questions | List of devolved questions. | list | A list of question numbers. |
| question_no_plaintext | Mapping of question numbers to plain text. | dict | A dictionary mapping question numbers to plain text. |
| devolved_nations | List of devolved nations. | list | A list of nation names. |

## Guidance for additional outputs
As an end user, you will only need to change the outputs config (named `config_outputs.json`) - you just need to update the filepaths and period information in the output config. Note: for ONS users, you can find example filepaths in the Confluence documentation.



# Export Config

| Parameter | Description | Data Type | Acceptable Values |
|---|---|---|---|
| platform | Specifies whether you're running the pipeline locally or on DAP. | string | `"network"`, `"s3"` |
| bucket | The path to the bucket. | string | Any filepath. |
| ssl_file | The path to the ssl certificate. | string | Any filepath. |
| output_dir | The path to the folder containing the files to be exported from. | string | Any filepath. |
| export_dir | The path to the folder containing the files to be exported to. | string | Any filepath. |
| schemas_dir | The path to the folder containing the schema toml data, if empty the export headers in manifest will be set to empty string. | string | Any filepath. |
| copy_or_move_files | Whether to copy or move the listed files. | string | `"copy"`, `"move"` |
| files_to_export | The filnames for the files to be exported use `false` if you don't want to export the relevant files. | dictionary of strings | Any filename. |
| files_basename | The base name for a file. | dictionary of strings | Any base file name. |
e.g the example below has run_id `202511071451` , methods_output set to `true` and methods_output basename `cons_results`, thus will export only the file `cons_results_202511071451.csv` and create the relevant manifest file:
```
"run_id": "202511071451",
"files_to_export": {
    "methods_output": false,
    "population_counts": false,
    "produce_qa_output": false,
    "r_and_m_regional_extracts": false,
    "constructed_output": false,
    "cord_output":false,
    "imputation_contribution_output":false,
    "quarterly_by_sizeband_output":false,
    "standard_errors": false
},
"files_basename": {
    "methods_output": "cons_results",
    "population_counts": "population_counts",
    "produce_qa_output": "produce_qa_output",
    "r_and_m_regional_extracts": "r_and_m_regional_extracts",
    "constructed_output": "constructed228",
    "cord_output":"cord_output",
    "imputation_contribution_output":"imputation_contribution_output",
    "quarterly_by_sizeband_output":"quarterly_by_sizeband_output",
    "standard_errors":"standard_errors_publication_period"
}

```

You may extend the `files_to_export` and `files_basename` dictionaries with more outputs if required.

## Guidance for additional outputs
As an end user, you will only need to change the export config (named `config_export.json`). The process will copy (or move) the files listed in the `files` section from the defined `output_dir` to `export_dir`, and will create a manifest json file for the NiFi.


# Dev Config
| Parameter | Description | Default | Data Type | Acceptable Values |
|---|---|---|---|---|
| platform | Specifies whether you're running the pipeline locally or on DAP. | `"s3"` | string | `"network"`, `"s3"` |
| back_data_type | The name of the backdata type marker column. | `"type"` | string | Any valid column name. |
| back_data_format | The format of the backdata file. | `"json"` | string | Any valid format, e.g. `"json"`. |
| imputation_marker_col | The name of the column being used as an imputation marker. | `"imputation_flags_adjustedresponse"` | string | Any valid column name. |
| state | The state of the pipeline (frozen or live). | `"frozen"` | string | `"frozen"` or `"live"` |
| auxiliary | The name of the column containing the auxiliary variable. | `"frotover"` | string | Any valid column name. |
| auxiliary_converted | The name of the column containing the auxiliary variable converted into monthly actual pounds. | `"converted_frotover"` | string | Any valid column name. |
| calibration_factor | The name of the column containing the calibration factor variable. | `"calibration_factor"` | string | Any valid column name. |
| cell_number | The name of the column containing the cell number variable. | `"cell_no"` | string | Any valid column name. |
| design_weight | The name of the column containing the design weight variable. | `"design_weight"` | string | Any valid column name. |
| status | The name of the column containing the status variable. | `"statusencoded"` | string | Any valid column name. |
| form_id_idbr | The name of the column containing the form type (IDBR) variable. | `"formtype"` | string | Any valid column name. |
| calibration_group | The name of the column containing the calibration group variable. | `"cell_no"` | string | Any valid column name. |
| sic | The name of the column containing the SIC code. | `"frosic2007"` | string | Any valid column name. |
| period | The name of the column containing the period variable. | `"period"` | string | Any valid column name. |
| question_no | The name of the column containing the question number/code variable. | `"questioncode"` | string | Any valid column name. |
| reference | The name of the column containing the reference variable. | `"reference"` | string | Any valid column name. |
| region | The name of the column containing the region variable. | `"region"` | string | Any valid column name. |
| sampled | The name of the column containing the is_sampled variable. | `"is_sampled"` | string | Any valid column name. |
| census | The name of the column containing the is_census variable. | `"is_census"` | string | Any valid column name. |
| strata | The name of the column containing the strata variable. | `"cell_no"` | string | Any valid column name. |
| target | The name of the column containing the target variable. | `"adjustedresponse"` | string | Any valid column name. |
| form_id_spp | The name of the column containing the form type (SPP) variable. | `"form_type_spp"` | string | Any valid column name. |
| imputation_class | The name of the column containing the imputation class variable. | `"imputation_class"` | string | Any valid column name. |
| l_value_question_no | The name of the column containing the l value question number. | `"questioncode"` | string | Any valid column name. |
| froempment | The name of the column containing the frozen employment variable. | `"froempment"` | string | Any valid column name. |
| nil_status_col | The name of the column containing the nil status. | `"status"` | string | Any valid column name. |
| pound_thousand_col | The name of the column containing the target variable expressed in thousands. | `"adjustedresponse_pounds_thousands"` | string | Any valid column name. |
| master_column_type_dict | Defines the expected data types for various columns. | `{ "reference": "int", "period": "date", "response": "str", "questioncode": "int", "adjustedresponse": "float", "frozensic": "str", "frozenemployees": "int", "frozenturnover": "float", "cellnumber": "int", "formtype": "str", "status": "str", "statusencoded": "int", "frosic2007": "str", "froempment": "int", "frotover": "float", "cell_no": "int", "region": "str"}` | dict | Any dictionary in the format `{ "column_name": "data_type"}` where column name is a valid column and data_type is one of `"bool"`, `"int"`, `"str"` or `"float"`. Both key and value should be enclosed in quotation marks. |
| contributors_keep_cols | Columns to keep for contributors. | `["period", "reference", "status", "statusencoded"]` | list | A list of valid column names. |
| responses_keep_cols | Columns to keep for responses. | `["adjustedresponse", "period", "questioncode", "reference", "response"]` | list | A list of valid column names. |
| finalsel_keep_cols | Columns to keep for final selection. | `["formtype", "cell_no", "froempment", "frotover", "reference", "region", "runame1", "entname1"]` | list | A list of valid column names. |
| temporarily_remove_cols | Columns to temporarily remove. | `[]` | list | A list of valid column names. |
| non_sampled_strata | Non-sampled strata values. | `["5141", "5142", "5143", "5371", "5372", "5373", "5661", "5662", "5663"]` | list | A list of cell numbers/strata. |
| population_column_names | Column names in the population frame. | `["reference", "checkletter", "inqcode", "entref", "wowentref", "frosic2003", "rusic2003", "frosic2007", "rusic2007", "froempees", "employees", "froempment", "employment", "froFTEempt", "FTEempt", "frotover", "turnover", "entrepmkr", "legalstatus", "inqstop", "entzonemkr", "region", "live_lu", "live_vat", "live_paye", "immfoc", "ultfoc", "cell_no", "selmkr", "inclexcl" ]` | list | A list of valid column names in the population frame dataset. |
| population_keep_columns | Population columns to keep. | `["reference", "region", "frotover", "cell_no"]` | list | A list of valid column names in the population frame dataset. |
| sample_column_names | Column names in the sample data. | `["reference", "checkletter", "frosic2003", "rusic2003", "frosic2007", "rusic2007", "froempees", "employees", "froempment", "employment", "froFTEempt", "FTEempt", "frotover", "turnover", "entref", "wowentref", "vatref", "payeref", "crn", "live_lu", "live_vat", "live_paye", "legalstatus", "entrepmkr", "region", "birthdate", "entname1", "entname2", "entname3", "runame1", "runame2", "runame3", "ruaddr1", "ruaddr2", "ruaddr3", "ruaddr4", "ruaddr5", "rupostcode", "tradstyle1", "tradstyle2", "tradstyle3", "contact", "telephone", "fax", "seltype", "inclexcl", "cell_no", "formtype", "cso_tel", "currency"]` | list | A list of valid column names in the sample data. |
| sample_keep_columns | Sample columns to keep. | `["reference", "runame1", "entname1"]` | list | A list of valid column names in the sample data. |
| filter_out_questions | A list of questions to filter out when running the pipeline. | `[11, 12, 146, 902, 903, 904]` | list | A list of ints where each int refers to a question. |
| csw_to_spp_columns | Mapping of CSW to SPP columns. | `{ "returned_value":"response", "adjusted_value":"adjustedresponse", "question_no":"questioncode"}` | dict | A dictionary in the format `{ "CSW_col_name": "SPP_col_name"}`. |
| type_to_imputation_marker | A dictionary mapper mapping type to imputation marker. | `{ "0": "selected, no return", "1": "r", "2": "derived", "3": "fir", "4": "bir", "5": "mc", "6": "c", "10": "r", "11": "r", "12": "derived", "13": "fir" }` | dict | A dictionary in the format `{ "type":"imputation_marker"}` where imputation marker is a value found in the imputation_marker_col. |
| mandatory_outputs | A list of mandatory outputs to produce after the pipeline has run. | `["produce_qa_output"]` | list | Any of the outputs listed in `cons_results/outputs/produce_additional_outputs.py` within the `produce_additional_outputs` function. |
| components_questions | List of component question numbers. | `[201,202,211,212,221,222,231,232,241,242,243]` | list | A list of question numbers. |
| census_extra_calibration_group | Extra calibration groups for census. | `[]` | list | A list of calibration groups. |
| bands | Mapping of band numbers to question ranges. | `{ "0": [1,7], "1": [11,17], ... }` | dict | A dictionary mapping band numbers to question ranges. |
| imputation_contribution_sics | A list of Standard Industry Classification codes (SICs) that should be included in the imputation contribution output. | `["41200", "41201", "41202", "42000", "42110", "42120", "42130", "42210", "42220", "42900", "42910", "42990", "43100", "43110", "43120", "43130", "43210", "43220", "43290", "43310", "43320", "43330", "43340", "43341", "43342", "43390", "43910", "43990", "43991", "43999"]` | list   | A list containing SIC codes.   |
| imputation_contribution_classification | A list of higher-level classification codes that should be included in the imputation contribution output. | `["41200", "42000", "42900", "43100", "43210", "43220", "43290", "43310", "43320", "43330", "43340", "43390", "43910", "43990"]` | list   | A list containing classification codes. |
| local_unit_columns | List of columns for local unit data. | `["ruref", "entref", "lu ref", "check letter", "sic03", "sic07", "employees", "employment", "fte", "Name1", "Name2", "Name3", "Address1", "Address2", "Address3", "Address4", "Address5", "Postcode", "trading as 1", "trading as 2", "trading as 3", "region"]` | list | A list of valid column names for local unit data. |
| nil_values | List of nil value statuses. | `["Combined child (NIL2)", "Out of scope (NIL3)", "Ceased trading (NIL4)", "Dormant (NIL5)", "Part year return (NIL8)", "No UK activity (NIL9)"]` | list | A list of nil value statuses. |
| non_response_statuses | A list of status values that refer to non-responses. | `["Form sent out", "Excluded from Results"]` | list | A list of statuses found in the "status" column. |
| clear_statuses | List of clear statuses. | `["Clear", "Clear - overridden"]` | list | A list of clear statuses. |
| sizeband_numeric_to_character | Mapping of numeric sizebands to characters. | `{ "1": "A", "2": "B", "3": "C", "4": "D", "5": "E", "6": "F", "7": "G"}` | dict | A dictionary mapping numeric sizebands to characters. |
| pounds_thousands_questions | List of question numbers where values are in thousands. | `[201, 202, 211, 212, 221, 222, 231, 232, 241, 242, 243, 290]` | list | A list of question numbers. |


## Usage
**Adding new columns**: To add new columns throughout the pipeline, you will need to add it to one of the keep_cols, i.e. `finalsel_keep_cols`, `responses_keep_cols` or `contributors_keep_cols` **and** you will also need to add it to the `master_column_type_dict` parameter.

## Updating
Please update this when you can - for example, if anything is added/removed from the config, or some of the sensible default values change, update this as part of your pull request.
