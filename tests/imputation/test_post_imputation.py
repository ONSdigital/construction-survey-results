import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from cons_results.imputation.post_imputation import (
    create_q290,
    derive_q290,
    rescale_290_case,
    validate_q290,
)


@pytest.fixture()
def filepath():
    return Path("tests/data/imputation/post_imputation")


def test_rescale_290_case(filepath):
    expected_output_df = pd.read_csv(
        filepath / "test_data_rescale_290_output.csv",
        dtype={"adjustedresponse_pre_rescale": float},
    )

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


def test_validate_q290(filepath):
    df_input = pd.read_csv(filepath / "validate_q290_input.csv")
    with tempfile.TemporaryDirectory() as tmpdirname:
        validate_q290(
            df_input,
            period="period",
            reference="reference",
            adjustedresponse="adjustedresponse",
            question_no="question_no",
            output_path=tmpdirname,
            output_file_name="validate_q290_test_output.csv",
        )
        actual_output = pd.read_csv(
            os.path.join(tmpdirname, "validate_q290_test_output.csv")
        )
        expected_output = pd.read_csv(filepath / "validate_q290_output.csv")
        assert_frame_equal(actual_output, expected_output)


@patch("pandas.DataFrame.to_csv")  # mock pandas export csv function
def test_validate_q290_warnings_and_output(mock_to_csv, filepath):
    df_input = pd.read_csv(filepath / "validate_q290_input.csv")

    with pytest.warns(
        UserWarning, match="q290 values do not match the sum of components"
    ):
        validate_q290(
            df_input,
            period="period",
            reference="reference",
            adjustedresponse="adjustedresponse",
            question_no="question_no",
            output_path="",
            output_file_name="mismatched_q290_totals.csv",
        )

    # check to csv was called with the correct file name
    mock_to_csv.assert_called_once_with("mismatched_q290_totals.csv", index=False)


@patch("pandas.DataFrame.to_csv")  # mock pandas export csv function
def test_validate_q290_no_csv(mock_to_csv, filepath):
    df_input = pd.read_csv(filepath / "validate_q290_input.csv")
    validate_q290(
        df_input,
        period="period",
        reference="reference",
        adjustedresponse="adjustedresponse",
        question_no="question_no",
    )
    # check to csv was not called
    mock_to_csv.assert_not_called()
