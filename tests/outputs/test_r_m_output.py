from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from cons_results.outputs.r_m_output import produce_r_m_output


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/outputs/r_m_output")

@pytest.fixture(scope="class")
def input_df(filepath):
    return pd.read_csv(filepath / "input_df.csv", index_col=False)

@pytest.fixture(scope="class")
def output_df(filepath):
    return pd.read_csv(filepath / "output_df.csv", index_col=False)

        
class TestRMOutput:
    def test_r_m_output(self, filepath, input_df, output_df):
        
        config = {
                "question_no_plaintext": {
                    "202": "public_housing",
                    "212": "private_housing"
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
                    "Scotland": 11
                },
                "r_m_quarter": "2023Q2",
                "local_unit_columns": ["ruref", "entref", "lu ref", "check letter", "sic03", "sic07",
                                       "employees", "employment", "fte", "Name1", "Name2", "Name3", "Address1", "Address2",
                                       "Address3", "Address4", "Address5", "Postcode", "trading as 1", "trading as 2",
                                       "trading as 3", "region"
                                       ],
                "idbr_folder_path": filepath,
                "ludets_prefix": "dummy_ludets228",
                "current_period": 202304,
                "revision_window": 2,
                "r_m_questions": [202, 212],
                "platform": "network",
                "period": "period",
                "bucket": ""
            }
        
        expected_output = output_df
        
        actual_output, filepath = produce_r_m_output(input_df, **config)
        
        assert_frame_equal(expected_output, actual_output)