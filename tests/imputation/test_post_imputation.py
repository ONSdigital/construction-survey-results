import pandas as pd
import pytest

from cons_results.imputation.post_imputation import rescale_imputed_values

scenarios = [100, 101, 102, 103, 104, 105]


@pytest.mark.parametrize("reference", scenarios)
def test_rescale_imputed_values(reference):
    expected_output = pd.read_csv(
        "tests/data/imputation/test_data_rescale_imputed_single.csv"
    )
    expected_output = expected_output.loc[expected_output["reference"] == reference]
    print(expected_output)
    input_data = expected_output.drop(columns=["adjusted_value", "rescale_factor"])

    derive_map = {
        "derive": 5,
        "from": [1, 2, 3, 4],
    }
    actual_output = rescale_imputed_values(
        input_data, "question_no", "target", "marker", derive_map
    )

    order = [
        "period",
        "reference",
        "question_no",
        "target",
        "derived_target",
        "adjusted_value",
        "rescale_factor",
    ]
    actual_output = actual_output[order]
    expected_output = expected_output[order]
    print(actual_output)

    actual_output.to_csv("test_output.csv")
    pd.testing.assert_frame_equal(actual_output, expected_output)
