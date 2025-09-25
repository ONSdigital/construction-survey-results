import glob

import pandas as pd
import pytest

from cons_results.outputs.produce_additional_outputs import produce_quarterly_extracts


@pytest.fixture
def filepath():
    return "tests/data/outputs/quarterly_extracts/"


@pytest.fixture
def sample_config(filepath):
    return {
        "platform": "network",
        "bucket": "",
        "period": "period",
        "region": "region",
        "question_no": "questioncode",
        "target": "adjustedresponse",
        "produce_quarterly_extracts": True,
        "region_mapping_path": filepath + "region_mapping.csv",
        "output_path": filepath,
    }


def test_quarterly_extracts(filepath, sample_config):

    input_df = pd.read_csv(filepath + "quarterly_extracts_input.csv")

    expected_output_df = pd.read_csv(filepath + "quarterly_extracts_output.csv")
    expected_output_df = expected_output_df.sort_values("region_name").reset_index(
        drop=True
    )

    produce_quarterly_extracts(sample_config, input_df)

    pattern = glob.glob(filepath + "r_and_m_regional_extracts*")

    for p in pattern:
        output_df = pd.read_csv(p)

    pd.testing.assert_frame_equal(output_df, expected_output_df)
