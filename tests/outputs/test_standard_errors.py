import pandas as pd
import pytest

from cons_results.outputs.standard_errors import create_standard_errors


@pytest.fixture
def filepath():
    return "tests/data/outputs/standard_errors/"


@pytest.fixture
def sample_config(filepath):
    return {
        "period": "period",
        "region": "region",
        "question_no": "questioncode",
        "target": "adjustedresponse",
        "produce_quarterly_extracts": True,
        "region_mapping_path": filepath + "region_mapping.csv",
        "output_path": filepath,
    }


def test_quarterly_extracts(filepath, sample_config):

    input_df = pd.read_csv(filepath + "input.csv")

    expected_output = pd.read_csv(filepath + "output.csv")

    actual_output = create_standard_errors(input_df)

    pd.testing.assert_frame_equal(actual_output, expected_output)