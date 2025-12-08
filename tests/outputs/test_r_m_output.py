from pathlib import Path

import pandas as pd
import pytest
from mbs_results.outputs.scottish_welsh_gov_outputs import read_and_combine_ludets_files
from pandas.testing import assert_frame_equal

from cons_results.outputs.r_m_output import (
    calculate_regional_percent,
    filter_region,
    produce_r_m_output,
    reformat_r_m_output,
)


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/outputs/r_m_output")


@pytest.fixture(scope="class")
def input_df(filepath):
    return pd.read_csv(filepath / "input_df.csv", index_col=False)


@pytest.fixture(scope="class")
def output_df(filepath):
    return pd.read_csv(filepath / "output_df.csv", index_col=False)


@pytest.fixture(scope="class")
def filter_region_output(filepath):
    return pd.read_csv(filepath / "filter_region_output.csv", index_col=False)


@pytest.fixture(scope="class")
def calculate_regional_percent_output(filepath):
    return pd.read_csv(
        filepath / "calculate_regional_percent_output.csv", index_col=False
    )


@pytest.fixture(scope="class")
def reformat_input(filepath):
    return pd.read_csv(filepath / "reformat_input.csv", index_col=False)


@pytest.fixture(scope="class")
def reformat_output(filepath):
    return pd.read_csv(filepath / "reformat_output.csv", index_col=False)


@pytest.fixture(scope="class")
def ludets_data(filepath):

    config = {
        "idbr_folder_path": filepath,
        "ludets_prefix": "dummy_ludets228",
        "current_period": 202304,
        "revision_window": 2,
        "platform": "network",
        "local_unit_columns": [
            "ruref",
            "entref",
            "lu ref",
            "check letter",
            "sic03",
            "sic07",
            "employees",
            "employment",
            "fte",
            "Name1",
            "Name2",
            "Name3",
            "Address1",
            "Address2",
            "Address3",
            "Address4",
            "Address5",
            "Postcode",
            "trading as 1",
            "trading as 2",
            "trading as 3",
            "region",
        ],
        "period": "period",
        "bucket": "",
    }

    return read_and_combine_ludets_files(config)


class TestRMOutput:
    def test_r_m_output(self, filepath, input_df, output_df):

        config = {
            "question_no_plaintext": {
                "202": "public_housing",
                "212": "private_housing",
            },
            "r_m_region_order": {
                "North East": 1,
                "Yorkshire and The Humber": 2,
                "East Midlands": 3,
                "East of England": 4,
                "London": 5,
                "South East": 6,
                "South West": 7,
                "Wales": 8,
                "West Midlands": 9,
                "North West": 10,
                "Scotland": 11,
            },
            "r_m_quarter": "2023Q2",
            "local_unit_columns": [
                "ruref",
                "entref",
                "lu ref",
                "check letter",
                "sic03",
                "sic07",
                "employees",
                "employment",
                "fte",
                "Name1",
                "Name2",
                "Name3",
                "Address1",
                "Address2",
                "Address3",
                "Address4",
                "Address5",
                "Postcode",
                "trading as 1",
                "trading as 2",
                "trading as 3",
                "region",
            ],
            "idbr_folder_path": filepath,
            "ludets_prefix": "dummy_ludets228",
            "current_period": 202304,
            "revision_window": 2,
            "r_m_questions": [202, 212],
            "platform": "network",
            "period": "period",
            "bucket": "",
        }

        expected_output = output_df

        actual_output, filepath = produce_r_m_output(input_df, **config)

        assert_frame_equal(expected_output, actual_output)

    def test_filter_region(self, ludets_data, filter_region_output):

        expected_output = filter_region_output

        actual_output = filter_region(ludets_data, "Scotland", ["XX"])

        assert_frame_equal(expected_output, actual_output)

    def test_calculate_regional_percent(
        self, input_df, filter_region_output, calculate_regional_percent_output
    ):

        df = input_df[input_df["questioncode"].isin([202, 212])]

        df["gross_turnover_uk"] = (
            df["adjustedresponse"]
            * df["design_weight"]
            * df["outlier_weight"]
            * df["calibration_factor"]
            / 1000
        )

        expected_output = calculate_regional_percent_output

        actual_output = calculate_regional_percent(
            df, filter_region_output, "Scotland", ["XX"]
        )

        assert_frame_equal(expected_output, actual_output)

    def test_reformat_r_m_output(self, reformat_input, reformat_output):

        config = {
            "question_no_plaintext": {
                "202": "public_housing",
                "212": "private_housing",
            },
            "r_m_region_order": {
                "North East": 1,
                "Yorkshire and The Humber": 2,
                "East Midlands": 3,
                "East of England": 4,
                "London": 5,
                "South East": 6,
                "South West": 7,
                "Wales": 8,
                "West Midlands": 9,
                "North West": 10,
                "Scotland": 11,
            },
            "r_m_quarter": "2023Q2",
        }

        expected_output = reformat_output

        actual_output, qtr = reformat_r_m_output(reformat_input, **config)

        assert_frame_equal(expected_output, actual_output)
