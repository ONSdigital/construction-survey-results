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
    {
        "0": [1, 7],
        "1": [11, 17],
        "2": [21, 27],
        "3": [31, 37],
        "4": [41, 47],
        "5": [51, 57],
        "6": [61, 67],
        "7": [71, 77],
        "8": [81, 87],
        "9": [91, 97],
        "10": [101, 107],
        "11": [111, 117],
        "12": [121, 127],
        "13": [131, 137],
    }
]
bands_not_ok_cases = [
    (
        # bad case 1 wrong size, not matching len of 2 (lower, upper)
        {1: [1, 7, 3], 2: [11]},
        ValueError,
    ),
    (
        # bad case 2 values exist but no size band
        {1: [1, 7], 2: [11, 17]},
        ValueError,
    ),
]


@pytest.mark.parametrize("ok_bands", bands_are_ok_cases)
def test_derive_imputation_class(input, ok_bands, expected_output):

    actual_output = derive_imputation_class(input, ok_bands, "values", "expected_bin")
    assert_frame_equal(actual_output, expected_output)


@pytest.mark.parametrize("not_ok_bands,expected_error", bands_not_ok_cases)
def test_derive_imputation_class_raises_error(not_ok_bands, input, expected_error):
    if expected_error:
        with pytest.raises(expected_error):
            derive_imputation_class(input, not_ok_bands, "values", "expected_bin")
