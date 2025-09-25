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
        "quarterly_extract": "2023Q1",
    }


def test_quarterly_extracts(filepath, sample_config):

    input_df = pd.read_csv(filepath + "quarterly_extracts_input.csv")

    expected_output_df = pd.read_csv(filepath + "quarterly_extracts_output.csv")
    expected_output_df["quarter"] = pd.PeriodIndex(
        expected_output_df["quarter"], freq="Q"
    )

    output_df, _ = produce_quarterly_extracts(input_df, **sample_config)

    # We need to do this to the actual output, because
    # it comes pivoted from the function call
    output_df = output_df.reset_index()
    output_df.columns = ["quarter", "region_name", "202", "212", "222", "232", "243"]

    pd.testing.assert_frame_equal(output_df, expected_output_df)
