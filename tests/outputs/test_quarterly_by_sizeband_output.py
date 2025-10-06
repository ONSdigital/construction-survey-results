import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from cons_results.outputs.quarterly_by_sizeband_output import (
    get_quarterly_by_sizeband_output,
)


@pytest.fixture
def filepath(outputs_data_dir):
    return outputs_data_dir / "quarterly_by_sizeband"


@pytest.fixture
def input_df(filepath):
    return pd.read_csv(
        filepath / "quarterly_by_sizeband_input.csv",
        index_col=False,
        dtype={"period": str},
    )


@pytest.fixture
def output_df(filepath):
    return pd.read_csv(filepath / "quarterly_by_sizeband_output.csv", index_col=False)


@pytest.fixture
def config():
    return {
        "period": "period",
        "question_no": "questioncode",
        "target": "adjustedresponse",
        "cell_number": "cell_no",
        "components_questions": [40, 49],
        "sizeband_quarter": ["2023Q2"],
    }


def test_get_quarterly_by_sizeband_output(input_df, output_df, config):

    expected_output = output_df

    actual_output = get_quarterly_by_sizeband_output(input_df, **config)

    assert_frame_equal(actual_output, expected_output)
