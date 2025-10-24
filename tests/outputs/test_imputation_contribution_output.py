import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from cons_results.outputs.imputation_contribution_output import (
    get_imputation_contribution_output,
)


@pytest.fixture(scope="class")
def input_df(outputs_data_dir):
    return pd.read_csv(
        outputs_data_dir / "imputation_contribution" / "input.csv", index_col=False
    )


@pytest.fixture(scope="class")
def output_df(outputs_data_dir):
    return pd.read_csv(
        outputs_data_dir / "imputation_contribution" / "output.csv", index_col=False
    )


class TestImputationContributionOutput:
    def test_imputation_contribution_output(self, input_df, output_df):
        expected_output = output_df

        config = {
            "period": "period",
            "imputation_contribution_periods": [202302],
            "imputation_contribution_sics": [41200, 41201, 42000, 42001, 42002],
            "imputation_contribution_classification": [41200, 42000, 42001],
            "components_questions": [201, 202, 211, 221],
            "question_no": "questioncode",
            "snapshot_file_path": "test_snapshot",
            "run_id": "1",
        }

        actual_output = get_imputation_contribution_output(input_df, **config)[0]

        expected_output = expected_output[
            ["questioncode", "total", "returned", "imputed"]
        ]
        actual_output = actual_output[["questioncode", "total", "returned", "imputed"]]

        assert_frame_equal(actual_output.reset_index(drop=True), expected_output)
