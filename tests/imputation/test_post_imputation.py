from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from cons_results.imputation.post_imputation import (
    create_q290,
    derive_q290,
    rescale_290_case,
)


@pytest.fixture()
def filepath():
    return Path("tests/data/imputation/post_imputation")


def test_rescale_290_case(filepath):
    expected_output_df = pd.read_csv(filepath / "test_data_rescale_290_output.csv")

    input_df = pd.read_csv(
        filepath / "test_data_rescale_290_input.csv",
        dtype={"adjustedresponse": float},
    )

    output_df = rescale_290_case(
        input_df, "period", "reference", "question_no", "adjustedresponse"
    )

    assert_frame_equal(output_df, expected_output_df)


def test_derive_q290(filepath):
    df_input = pd.read_csv(filepath / "derive_q290_input.csv")
    df_expected_output = pd.read_csv(filepath / "derive_q290_output.csv")

    actual_output = derive_q290(
        df=df_input,
        question_no="question_no",
        imputation_flag="imputation_flag",
        period="period",
        reference="reference",
        adjustedresponse="adjustedresponse",
    )

    assert_frame_equal(actual_output, df_expected_output)


def test_create_q290(filepath):

    df_input = pd.read_csv(filepath / "create_q290_input.csv")

    df_expected_output = pd.read_csv(
        filepath / "create_q290_output.csv", dtype={"adjustedvalue": np.float64}
    )

    config = {
        "finalsel_keep_cols": ["froempment", "frotover", "reference"],
        "contributors_keep_cols": ["period", "reference", "status"],
    }

    actual_output = create_q290(
        df=df_input,
        config=config,
        reference="reference",
        period="period",
        question_no="question_no",
        adjustedresponse="adjustedresponse",
        imputation_flag="imputation_flag",
    )

    assert_frame_equal(actual_output, df_expected_output)
