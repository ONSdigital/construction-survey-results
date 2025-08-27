import pandas as pd
from pandas.testing import assert_frame_equal

from cons_results.staging.live_or_frozen import run_live_or_frozen


def test_run_live_or_frozen():
    """Test function with frozen run (live does nothing)"""

    responses_input = pd.DataFrame(
        data={
            "reference": [1, 1, 1, 1],
            "period": [202201, 202201, 202202, 202202],
            "questioncode": [100, 101, 100, 101],
            "target": [99, 99, 99, 99],
        }
    )

    contributors_input = pd.DataFrame(
        data={
            "reference": [1, 1],
            "period": [202201, 202202],
            "error_column": [210, 201],
        }
    )

    responses_expected = pd.DataFrame(
        data={
            "reference": [1, 1],
            "period": [202201, 202201],
            "questioncode": [
                100,
                101,
            ],
            "target": [99, 99],
        }
    )

    frozen_responses_expected = pd.DataFrame(
        data={
            "reference": [1, 1],
            "period": [202202, 202202],
            "questioncode": [100, 101],
            "live_target": [99, 99],
        }
    )

    responses_actual, frozen_responses_actual = run_live_or_frozen(
        responses=responses_input,
        contributors=contributors_input,
        period="period",
        reference="reference",
        question_no="questioncode",
        target="target",
        status="error_column",
        current_period=202202,
        revision_window=2,
        state="frozen",
        error_values=[201],
    )

    assert_frame_equal(responses_actual, responses_expected)
    assert_frame_equal(frozen_responses_actual, frozen_responses_expected)
