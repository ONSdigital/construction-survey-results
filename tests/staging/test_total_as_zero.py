from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from cons_results.staging.total_as_zero import flag_total_only_and_zero


@pytest.fixture()
def input_df():
    data = {
        'reference': [1, 2, 2, 2, 2,3],
        'period': [202201, 202201, 202201, 202201, 202201,202201],
        'qcodes': [290,290,2,3,4,4],
        'values': [0, 150, 75, 75, 0,0]
    }
    df = pd.DataFrame(data)
    return df

@pytest.fixture()
def expected_output_df():
    data = {
        'reference': [1, 2, 2, 2, 2,3],
        'period': [202201, 202201, 202201, 202201, 202201,202201],
        'qcodes': [290,290,2,3,4,4],
        'values': [0, 150, 75, 75, 0,0],
        'is_total_only_and_zero':[True,False,False,False,False,False]
    }
    df = pd.DataFrame(data)

    return df

def test_flag_total_only_and_zero(input_df,expected_output_df):
    

    actual_output = flag_total_only_and_zero(
        input_df,"reference","period","values","qcodes")

    assert_frame_equal(actual_output, expected_output_df)
