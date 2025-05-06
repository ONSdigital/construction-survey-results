from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from cons_results.staging.create_missing_questions import create_missing_questions


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/staging/create_missing_questions")


@pytest.fixture(scope="class")
def contributors(filepath):
    return pd.read_csv(filepath / "contributors.csv", index_col=False)


@pytest.fixture(scope="class")
def responses_input(filepath):
    return pd.read_csv(filepath / "responses.csv", index_col=False)


@pytest.fixture(scope="class")
def responses_espected(filepath):
    return pd.read_csv(filepath / "responses_expected.csv", index_col=False)


class TestCreateMissingQuestions:
    def test_create_missing_questions(
        self,
        contributors,
        responses_input,
        responses_espected,
    ):
        actual_output = create_missing_questions(
            contributors=contributors,
            responses=responses_input,
            all_questions=[77, 88, 99],
            reference="reference",
            period="period",
            question_col="questioncode",
        )

        assert_frame_equal(actual_output, responses_espected)
