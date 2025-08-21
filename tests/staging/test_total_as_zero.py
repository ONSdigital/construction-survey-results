import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from cons_results.staging.total_as_zero import flag_total_only_and_zero


@pytest.fixture()
def responses_df():
    responses = {
        "reference": [1, 2, 2, 2, 2, 3, 5, 5, 5, 6, 7, 7, 7],
        "period": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        "qcodes": [290, 290, 2, 3, 4, 4, 290, 2, 3, 290, 1, 2, 290],
        "values": [0, 150, 75, 75, 0, 0, 0, 99, 99, 99, 0, 0, 290],
    }
    df = pd.DataFrame(responses)
    return df


@pytest.fixture()
def contributors_df():
    contributors = {
        "reference": [1, 2, 3, 5, 6, 7],
        "period": [1, 1, 1, 1, 1, 1],
        "status": ["Clear", "Clear", "Clear", "Clear", "Clear", "Check needed"],
    }
    df = pd.DataFrame(contributors)
    return df


@pytest.fixture()
def expected_output_df():
    data = {
        "reference": [1, 2, 2, 2, 2, 3, 5, 5, 5, 6, 7, 7, 7],
        "period": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        "qcodes": [290, 290, 2, 3, 4, 4, 290, 2, 3, 290, 1, 2, 290],
        "values": [0, 150, 75, 75, 0, 0, 0, 99, 99, 99, 0, 0, 290],
        "is_total_only_and_zero": [
            True,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
        ],
    }
    df = pd.DataFrame(data)

    return df


def test_flag_total_only_and_zero(responses_df, contributors_df, expected_output_df):

    actual_output = flag_total_only_and_zero(
        responses_df, contributors_df, "reference", "period", "values", "qcodes"
    )

    print(actual_output)

    assert_frame_equal(actual_output, expected_output_df)
