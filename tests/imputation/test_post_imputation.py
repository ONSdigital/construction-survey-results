import logging
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
    validate_r_before_derived_zero,
)

LOGGER = logging.getLogger(__name__)


@pytest.fixture()
def filepath():
    return Path("tests/data/imputation/post_imputation")


@pytest.fixture()
def config():
    return {"run_id": "1"}


class TestPostImputation:
    def test_rescale_290_case(self, filepath):
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

    def test_derive_q290(self, filepath):
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

    def test_create_q290(self, filepath):
        df_input = pd.read_csv(filepath / "create_q290_input.csv")
        df_expected_output = pd.read_csv(
            filepath / "create_q290_output.csv", dtype={"adjustedvalue": np.float64}
        )
        config = {
            "finalsel_keep_cols": ["froempment", "frotover", "reference"],
            "contributors_keep_cols": ["period", "reference", "status"],
            "run_id": "1",
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

    def test_validate_q290(self, filepath, config):
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
                config=config,
            )
            actual_output = pd.read_csv(
                os.path.join(tmpdirname, "validate_q290_test_output.csv")
            )
            expected_output = pd.read_csv(filepath / "validate_q290_output.csv")
            assert_frame_equal(actual_output, expected_output)

    @patch("pandas.DataFrame.to_csv")
    def test_validate_q290_warnings_and_output(self, mock_to_csv, filepath, config):
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
                config=config,
            )
        mock_to_csv.assert_called_once_with("mismatched_q290_totals.csv", index=False)

    @patch("pandas.DataFrame.to_csv")
    def test_validate_q290_no_csv(self, mock_to_csv, filepath, config):
        df_input = pd.read_csv(filepath / "validate_q290_input.csv")
        validate_q290(
            df_input,
            period="period",
            reference="reference",
            adjustedresponse="adjustedresponse",
            question_no="question_no",
            config=config,
        )
        mock_to_csv.assert_not_called()

    def test_validate_r_before_derived_zero(self, filepath, caplog):
        """
        Testing the warning message for validate_r_before_derived_zero function
        references 7 and 8 should trigger the warning.
        """

        df_input = pd.read_csv(filepath / "validate_r_before_derived_zero_input.csv")

        # This is to get the list of indices that should trigger the warning
        false_indices_list = (
            df_input[df_input["reference"].isin([7, 8, 9, 10, 11, 12])]
            .set_index(["reference", "question_no"])
            .index.tolist()
        )
        # get unique and sorted indices
        false_indices_list = list(sorted(set(false_indices_list)))

        # copy the exact warning message from the function
        expected_warning = f"""These reference and questioncode combinations
                       have a 'd' flag for components without being preceded
                       by a response, which may be an error.
                       Please check these: {false_indices_list}"""

        with caplog.at_level(logging.WARNING):
            validate_r_before_derived_zero(
                df=df_input,
                question_no="question_no",
                imputation_flag="imputation_flag",
                period="period",
                reference="reference",
            )
        assert expected_warning in caplog.text
