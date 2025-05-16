import pandas as pd
import pytest

def test_rescale_290_case():
    expected_output_df = pd.read_csv(
        "tests/data/imputation/test_data_rescale_290_output.csv"
    )

    input_df = pd.read_csv(
        "tests/data/imputation/test_data_rescale_290_output.csv",
        dtype={"adjustedresponse": float},
    )

    output_df = rescale_290_case(
        input_df, "period", "reference", "question_no", "adjustedresponse"
    )

    pd.testing.assert_frame_equal(output_df, expected_output_df)
  
