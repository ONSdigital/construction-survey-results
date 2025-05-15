import pandas as pd
import pytest

from cons_results.imputation.post_imputation import (
    flag_290_case,
    forward_impute_290_case,
    rescale_imputed_values,
)

scenarios = [100, 101, 102, 103, 104, 105]


@pytest.mark.parametrize("reference", scenarios)
def test_rescale_imputed_values(reference):
    expected_output = pd.read_csv(
        "tests/data/imputation/test_data_rescale_imputed_single.csv"
    )
    expected_output = expected_output.loc[expected_output["reference"] == reference]
    input_data = expected_output.drop(columns=["adjusted_value", "rescale_factor"])

    derive_map = {
        "map_1": {
            "derive": 5,
            "from": [1, 2, 3, 4],
        },
    }
    actual_output = rescale_imputed_values(
        input_data, "question_no", "target", "marker", derive_map
    )

    order = [
        "period",
        "reference",
        "question_no",
        "target",
        "marker",
        "derived_target",
        "constrain_marker",
        "rescale_factor",
        "adjusted_value",
    ]
    actual_output = actual_output[order]
    expected_output = expected_output[order]
    pd.testing.assert_frame_equal(actual_output, expected_output)


@pytest.mark.parametrize("reference", scenarios)
def test_rescale_imputed_values_double_call(reference):
    expected_output = pd.read_csv(
        "tests/data/imputation/test_data_rescale_imputed_double.csv"
    )
    expected_output = expected_output.loc[expected_output["reference"] == reference]
    input_data = expected_output.drop(columns=["adjusted_value", "rescale_factor"])

    derive_map = {
        "map_1": {
            "derive": 5,
            "from": [1, 2, 3, 4],
        },
        "map_2": {
            "derive": 6,
            "from": [
                1,
                2,
            ],
        },
    }
    actual_output = rescale_imputed_values(
        input_data, "question_no", "target", "marker", derive_map
    )

    order = [
        "period",
        "reference",
        "question_no",
        "target",
        "marker",
        "derived_target",
        "constrain_marker",
        "rescale_factor",
        "adjusted_value",
    ]
    actual_output = actual_output[order]
    expected_output = expected_output[order]

    print(actual_output, expected_output)
    pd.testing.assert_frame_equal(actual_output, expected_output)


def test_flag_290_case():
    expected_output_df = pd.read_csv("tests/data/imputation/test_data_290_cases.csv")

    input_df = expected_output_df.drop(columns=["290_flag"])

    output_df = flag_290_case(
        input_df, "period", "reference", "question_no", "adjustedresponse"
    )

    pd.testing.assert_frame_equal(output_df, expected_output_df)


def test_forward_impute_290_case():
    expected_output_df = pd.read_csv(
        "tests/data/imputation/test_data_impute_290_output.csv"
    )

    input_df = pd.read_csv(
        "tests/data/imputation/test_data_impute_290_output.csv",
        dtype={"adjustedresponse": float},
    )

    output_df = forward_impute_290_case(
        input_df, "period", "reference", "question_no", "adjustedresponse"
    )

    pd.testing.assert_frame_equal(output_df, expected_output_df)
