import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from cons_results.staging.create_missing_questions import (
    convert_values,
    create_missing_questions,
)


@pytest.fixture(scope="class")
def filepath(staging_data_dir):
    return staging_data_dir / "create_missing_questions"


@pytest.fixture(scope="class")
def contributors(filepath):
    return pd.read_csv(filepath / "contributors.csv", index_col=False)


@pytest.fixture(scope="class")
def responses_input(filepath):
    return pd.read_csv(filepath / "responses.csv", index_col=False)


@pytest.fixture(scope="class")
def responses_expected(filepath):
    return pd.read_csv(filepath / "responses_expected.csv", index_col=False)


class TestCreateMissingQuestions:
    def test_create_missing_questions(
        self,
        contributors,
        responses_input,
        responses_expected,
    ):
        actual_output = create_missing_questions(
            contributors=contributors,
            responses=responses_input,
            components_questions=[77, 88, 99],
            reference="reference",
            period="period",
            question_col="questioncode",
        )

        actual_output = actual_output.sort_values(
            by=["reference", "period", "questioncode"]
        ).reset_index(drop=True)
        responses_expected = responses_expected.sort_values(
            by=["reference", "period", "questioncode"]
        ).reset_index(drop=True)

        assert_frame_equal(actual_output, responses_expected)


class TestConvert_values:
    def test_convert_values(self):

        df_in = pd.DataFrame(
            {"a": [1.0, 2.0, 3.0, 4.0, 5.0], "b": [True, False, True, False, True]}
        )
        df_expected = pd.DataFrame(
            {
                "a": [np.nan, 2.0, np.nan, 4.0, np.nan],
                "b": [True, False, True, False, True],
            }
        )

        df_out = convert_values(df_in, "a", df_in["b"])

        assert_frame_equal(df_out, df_expected)

    def test_convert_values_raises_error_mask_not_bool(self):

        wrong_df = pd.DataFrame({"a": [1, 2, 3, 4, 5], "b": [1, 0, 1, 0, 1]})
        with pytest.raises(ValueError):
            convert_values(wrong_df, "a", wrong_df["b"])
