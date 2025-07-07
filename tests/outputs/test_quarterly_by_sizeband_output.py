import pandas as pd

from cons_results.outputs.quarterly_by_sizeband_output import (
    get_quarterly_by_sizeband_output,
)


def test_get_quarterly_by_sizeband_output():

    data = {
        "period": ["202401", "202402", "202404", "202405"],
        "sizeband": [1, 2, 1, 2],
        "questioncode": [201, 201, 202, 202],
        "adjustedresponse": [100.0, 200.0, 150.0, 250.0],
    }
    additional_outputs_df = pd.DataFrame(data)

    config = {
        "period": "period",
        "question_no": "questioncode",
        "target": "adjustedresponse",
    }

    result = get_quarterly_by_sizeband_output(additional_outputs_df, **config)

    expected_df = pd.DataFrame(
        {
            "quarter": ["2024Q1", "2024Q1", "2024Q2", "2024Q2"],
            "sizeband": [1, 2, 1, 2],
            "201": [100.0, 200.0, 0.0, 0.0],
            "202": [0.0, 0.0, 150.0, 250.0],
        }
    )

    pd.testing.assert_frame_equal(result, expected_df)
