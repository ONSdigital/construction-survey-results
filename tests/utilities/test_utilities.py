from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from cons_results.utilities.csw_to_228_snapshot import remove_skipped_questions


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/utilities/remove_skipped_questions/")


@pytest.mark.parametrize(
    "input_file,expected_output_file",
    [
        ("remove_skipped_questions_input.csv", "remove_skipped_questions_expected.csv"),
        (
            "remove_skipped_questions_with_yes_no_input.csv",
            "remove_skipped_questions_with_yes_no_expected.csv",
        ),
        (
            "remove_skipped_questions_with_empty_input.csv",
            "remove_skipped_questions_with_empty_expected.csv",
        ),
    ],
)
class TestRemoveSkippedQuestions:
    def test_remove_skipped_questions(
        self,
        filepath,
        input_file,
        expected_output_file,
    ):
        skipped_questions_input = pd.read_csv(str(filepath / input_file))
        skipped_questions_expected = pd.read_csv(str(filepath / expected_output_file))

        actual_output = remove_skipped_questions(
            responses_df=skipped_questions_input,
            reference_col="reference",
            period_col="period",
            questioncode_col="questioncode",
            target_col="adjusted_value",
            route_skipped_questions={
                902: [201, 202, 211, 212],
                903: [221, 222],
                904: [231, 232, 241, 242, 243],
            },
            no_values=[2, "no", np.nan],
        )

        actual_output.sort_values(["period", "reference", "questioncode"], inplace=True)
        skipped_questions_expected.sort_values(
            ["period", "reference", "questioncode"], inplace=True
        )

        actual_output.reset_index(drop=True, inplace=True)
        skipped_questions_expected.reset_index(drop=True, inplace=True)

        actual_output["questioncode"] = actual_output["questioncode"].astype(int)

        assert_frame_equal(actual_output, skipped_questions_expected)
