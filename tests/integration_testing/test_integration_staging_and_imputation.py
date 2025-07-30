import json
from pathlib import Path

import pandas as pd
import pytest

from cons_results.imputation.impute import impute
from cons_results.staging.stage_dataframe import stage_dataframe


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/integration_testing")


@pytest.fixture(scope="class")
def test_config(filepath):
    return {
        "platform": "network",
        "bucket": "",
        "calibration_group_map_path": "",
        "classification_values_path": None,
        "idbr_folder_path": filepath,
        "l_values_path": None,
        "manual_outlier_path": "",
        "snapshot_file_path": str(filepath / "non_response_1.json"),
        "manual_constructions_path": None,
        "filter": None,
        "output_path": str(filepath),
        "population_prefix": None,
        "sample_prefix": "finalsel228",
        "back_data_qv_path": filepath / "test_qv_009_202412.csv",
        "back_data_cp_path": filepath / "test_cp_009_202412.csv",
        "back_data_finalsel_path": str(filepath / "finalsel228_dummy_202412"),
        "sic_domain_mapping_path": "",
        "current_period": 202502,
        "revision_window": 2,
        "state": "frozen",
        "optional_outputs": [""],
        "components_questions": [1, 2, 3, 4, 5, 6],
    }


def load_config(config_file_path):
    with open(config_file_path, "r") as f:
        return json.load(f)


def load_config_temp():
    config_dev_path = "cons_results/configs/config_dev.json"
    config = load_config(config_dev_path)
    return config


@pytest.mark.parametrize(
    "snapshot_file,expected_output_file",
    [
        ("non_response_1.json", "expected_non_response_1.csv"),
        ("form_in_error_1.json", "expected_form_in_error_1.csv"),
        ("form_in_error_2.json", "expected_form_in_error_2.csv"),
        ("paper_form_1.json", "expected_paper_form_1.csv"),
        ("total_only_1.json", "expected_total_only_1.csv"),
        ("total_only_2.json", "expected_total_only_2.csv"),
        ("total_only_3.json", "expected_total_only_3.csv"),
        ("total_only_4.json", "expected_total_only_4.csv"),
    ],
)
def test_run_integration_parametrised(
    test_config, filepath, snapshot_file, expected_output_file
):
    """Run Staging and Imputation based on test_config"""
    config = load_config_temp()
    config.update(test_config)
    config["snapshot_file_path"] = str(filepath / snapshot_file)

    df, manual_constructions, filter_df = stage_dataframe(config)

    df = impute(df, config, manual_constructions, filter_df)

    cols_output = [
        "reference",
        "period",
        "questioncode",
        "status",
        "imputation_flags_adjustedresponse",
        "skipped_question",
        "290_flag",
    ]

    # Load expected output DataFrame
    expected_output_path = filepath / expected_output_file
    expected_df = pd.read_csv(expected_output_path)

    # Sort both DataFrames for consistent comparison
    df_sorted = (
        df[cols_output]
        .sort_values(by=["period", "questioncode"])
        .reset_index(drop=True)
    )

    cols_expected = df_sorted.columns.to_list()
    expected_sorted = (
        expected_df[cols_expected]
        .sort_values(by=["period", "questioncode"])
        .reset_index(drop=True)
    )

    df_sorted["skipped_question"] = df_sorted["skipped_question"].astype(float)

    # enforce bool to match testing dtype
    df_sorted["290_flag"] = df_sorted["290_flag"].astype(bool)

    expected_sorted["skipped_question"] = expected_sorted["skipped_question"].astype(
        float
    )

    pd.testing.assert_frame_equal(df_sorted, expected_sorted)
