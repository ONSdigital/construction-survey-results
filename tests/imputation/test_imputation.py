from pathlib import Path

import pandas as pd
import pytest

from cons_results.imputation.post_imputation import rescale_290_case


@pytest.fixture()
def filepath():
    return Path("tests/data/imputation")


def test_rescale_290_case(filepath):
    expected_output_df = pd.read_csv(filepath / "test_data_rescale_290_output.csv")

    input_df = pd.read_csv(
        "tests/data/imputation/test_data_rescale_290_output.csv",
        dtype={"adjustedresponse": float},
    )

    output_df = rescale_290_case(
        input_df, "period", "reference", "question_no", "adjustedresponse"
    )

    pd.testing.assert_frame_equal(output_df, expected_output_df)
