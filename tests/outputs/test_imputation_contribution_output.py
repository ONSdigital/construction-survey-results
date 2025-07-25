from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from cons_results.outputs.imputation_contribution_output import (
    get_imputation_contribution_output,
)


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/outputs/imputation_contribution")


@pytest.fixture(scope="class")
def input_df(filepath):
    return pd.read_csv(filepath / "input.csv", index_col=False)


@pytest.fixture(scope="class")
def output_df(filepath):
    return pd.read_csv(filepath / "output.csv", index_col=False)


class TestImputationContributionOutput:
    def test_imputation_contribution_output(self, input_df, output_df):

        expected_output = output_df

        actual_output = get_imputation_contribution_output(
            input_df, [41200, 41201, 42000, 42001], [201, 202, 211]
        )

        assert_frame_equal(actual_output, expected_output)
