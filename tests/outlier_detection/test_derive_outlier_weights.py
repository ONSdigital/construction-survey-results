import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from cons_results.outlier_detection.derive_outlier_weights import (
    derive_q290_outlier_weights,
)


@pytest.fixture()
def filepath(outlier_data_dir):
    return outlier_data_dir / "derive_outlier_weights"


all_questions = [201, 202, 211, 212, 221, 222]


def test_derive_q290_outlier_weights(filepath):
    expected_output_df = pd.read_csv(
        filepath / "derive_q290_outlier_weights_output.csv"
    )

    input_df = pd.read_csv(filepath / "derive_q290_outlier_weights_output.csv")

    output_df = derive_q290_outlier_weights(
        input_df,
        all_questions,
        "adjustedresponse",
        "question_no",
        "reference",
        "period",
    )

    assert_frame_equal(output_df, expected_output_df)
