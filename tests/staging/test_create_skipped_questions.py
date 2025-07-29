from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from cons_results.staging.create_skipped_questions import create_skipped_questions


@pytest.fixture()
def filepath():
    return Path("tests/data/staging/create_skipped_questions")


@pytest.mark.parametrize(
    "input_file,output_file,filter_value,new_flag_column_name",
    [
        (
            "create_skipped_questions_input.csv",
            "create_skipped_questions_output.csv",
            ["Clear", "Clear - overridden"],
            "skipped_question",
        ),
        (
            "create_skipped_questions_input_derive_zeros.csv",
            "create_skipped_questions_output_derive_zeros.csv",
            ["Form sent out"],
            "derived_zeros",
        ),
    ],
)
def test_create_skipped_questions(
    filepath, input_file, output_file, filter_value, new_flag_column_name
):
    df_input = pd.read_csv(filepath / input_file)
    df_expected_output = pd.read_csv(
        filepath / output_file,
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
        status_col="status",
        status_filter=filter_value,
        flag_col_name=new_flag_column_name,
    )

    # assert actual_output.shape == df_expected_output.shape
    actual_output = actual_output.sort_values(
        by=["reference", "period", "questioncode"]
    ).reset_index(drop=True)
    df_expected_output = df_expected_output.sort_values(
        by=["reference", "period", "questioncode"]
    ).reset_index(drop=True)
    actual_output = actual_output[column_order]
    assert_frame_equal(actual_output, df_expected_output)
