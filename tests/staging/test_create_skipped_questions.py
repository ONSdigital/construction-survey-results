from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from cons_results.staging.create_skipped_questions import create_skipped_questions


@pytest.fixture()
def filepath():
    return Path("tests/data/staging/create_skipped_questions")


def test_create_skipped_questions(filepath):
    df_input = pd.read_csv(filepath / "create_skipped_questions_input.csv")
    df_expected_output = pd.read_csv(
        filepath / "create_skipped_questions_output.csv",
        dtype={"adjustedresponse": float},
    )
    column_order = df_expected_output.columns.tolist()
    actual_output = create_skipped_questions(
        df_input,
        reference="reference",
        period="period",
        question_col="questioncode",
        all_questions=[1, 2, 3, 4, 5, 6, 7],
        target_col="adjustedresponse",
        contributors_keep_col=["reference", "period"],
        responses_keep_col=["reference", "period", "questioncode", "adjustedresponse"],
        finalsel_keep_col=["reference", "period", "status"],
    )
    assert actual_output.shape == df_expected_output.shape
    actual_output = actual_output.sort_values(
        by=["reference", "period", "questioncode"]
    ).reset_index(drop=True)
    actual_output = actual_output[column_order]
    assert_frame_equal(actual_output, df_expected_output)
