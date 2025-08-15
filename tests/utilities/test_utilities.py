from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from cons_results.utilities.csw_to_228_snapshot import remove_skipped_questions


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/utilities/remove_skipped_questions")


@pytest.fixture(scope="class")
def skipped_questions_input(filepath):
    return pd.read_csv(filepath / "remove_skipped_questions_input.csv", index_col=False)


@pytest.fixture(scope="class")
def skipped_questions_expected(filepath):
    return pd.read_csv(
        filepath / "remove_skipped_questions_expected.csv", index_col=False
    )


class TestRemoveSkippedQuestions:
    def test_remove_skipped_questions(
        self,
        skipped_questions_input,
        skipped_questions_expected,
    ):

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
            no_values=[2],
        )

        actual_output.sort_values(["period", "reference", "questioncode"], inplace=True)
        skipped_questions_expected.sort_values(
            ["period", "reference", "questioncode"], inplace=True
        )

        actual_output.reset_index(drop=True, inplace=True)
        skipped_questions_expected.reset_index(drop=True, inplace=True)

        actual_output["questioncode"] = actual_output["questioncode"].astype(int)

        assert_frame_equal(actual_output, skipped_questions_expected)
