from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from cons_results.outputs.cord_output import get_cord_output


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/outputs/cord_output")


@pytest.fixture(scope="class")
def input_df(filepath):
    return pd.read_csv(filepath / "cord_input.csv", index_col=False)


@pytest.fixture(scope="class")
def output_df(filepath):
    return pd.read_csv(filepath / "cord_output.csv", index_col=False)


class TestCordOutput:
    def test_cord_output(self, input_df, output_df):

        config = {
            "question_no": "questioncode",
            "cell_number": "cell_no",
            "period": "period",
            "target": "adjustedresponse",
            "sizeband_numeric_to_character": {
                "1": "A",
                "2": "B",
                "3": "C",
                "4": "D",
                "5": "E",
                "6": "F",
                "7": "G",
            },
            "components_questions": [
                201,
                202,
            ],
            "imputation_contribution_classification": ["1", "2", "3"],
            "filter_out_questions": [11, 12, 146, 902, 903, 904],
        }

        expected_output = output_df

        actual_output = get_cord_output(input_df, **config)

        assert_frame_equal(actual_output, expected_output)
