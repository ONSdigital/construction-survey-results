from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from cons_results.staging.stage_dataframe import flag_290_case, set_290_components_null


@pytest.fixture()
def filepath():
    return Path("tests/data/staging/stage_dataframe")


def test_flag_290_case(filepath):
    expected_output_df = pd.read_csv(filepath / "290_flag_expected.csv")

    responses = pd.read_csv(filepath / "290_flag_responses.csv")
    contributors = pd.read_csv(filepath / "290_flag_contributors.csv")

    output_df = flag_290_case(
        responses,
        contributors,
        "period",
        "reference",
        "question_no",
        "adjustedresponse",
    )

    assert_frame_equal(output_df, expected_output_df)


def test_set_290_component_null(filepath):
    expected_output_df = pd.read_csv(filepath / "290_flag_null_components_expected.csv")

    input_df = pd.read_csv(filepath / "290_flag_null_components_input.csv")

    output_df = set_290_components_null(
        input_df,
        "question_no",
        "adjustedresponse",
    )

    assert_frame_equal(output_df, expected_output_df)
