from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from cons_results.staging.stage_dataframe import flag_290_case


@pytest.fixture()
def filepath():
    return Path("tests/data/staging/stage_dataframe")


def test_flag_290_case(filepath):
    expected_output_df = pd.read_csv(filepath / "test_data_290_cases.csv")

    input_df = expected_output_df.drop(columns=["290_flag"])

    output_df = flag_290_case(
        input_df, "period", "reference", "question_no", "adjustedresponse"
    )

    assert_frame_equal(output_df, expected_output_df)
