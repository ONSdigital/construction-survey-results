from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from cons_results.staging.derive_imputation_class import derive_imputation_class


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/staging/derive_imputation_class")


@pytest.fixture(scope="class")
def expected_output(filepath):
    return pd.read_csv(filepath / "derive_imputation_class.csv", index_col=False)


@pytest.fixture(scope="class")
def input(expected_output):
    return expected_output.drop("expected_bin", axis=1)


bands_are_ok_cases = [
    (
        [
            [1, 7],
            [11, 17],
            [21, 27],
            [31, 37],
            [41, 47],
            [51, 57],
            [61, 67],
            [71, 77],
            [81, 87],
            [91, 97],
            [101, 107],
            [111, 117],
            [121, 127],
            [131, 137],
        ]
    )
]
bands_not_ok_cases = [
    (
        # bad case 1 wrong size, not matching len of 2 (lower, upper)
        [[1, 7, 3], [11]],
        ValueError,
    ),
    (
        # bad case 2 values exist but no size band
        [[1, 7], [11, 17]],
        ValueError,
    ),
    (
        # bad case 3 no values in that size band
        [[1, 7], [888, 999]],
        ValueError,
    ),
]


@pytest.mark.parametrize("ok_bands", bands_are_ok_cases)
def test_derive_imputation_class(input, ok_bands, expected_output):

    actual_output = derive_imputation_class(input, ok_bands, "values", "expected_bin")
    actual_output["expected_bin"] = actual_output["expected_bin"].astype(str)
    assert_frame_equal(actual_output, expected_output)


@pytest.mark.parametrize("not_ok_bands,expected_error", bands_not_ok_cases)
def test_derive_imputation_class_raises_error(not_ok_bands, input, expected_error):
    if expected_error:
        with pytest.raises(expected_error):
            derive_imputation_class(input, not_ok_bands, "values", "expected_bin")
