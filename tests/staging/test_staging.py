import pandas as pd

from cons_results.staging.stage_dataframe import flag_290_case


def test_flag_290_case():
    expected_output_df = pd.read_csv("tests/data/staging/test_data_290_cases.csv")

    input_df = expected_output_df.drop(columns=["290_flag"])

    output_df = flag_290_case(
        input_df, "period", "reference", "question_no", "adjustedresponse"
    )

    pd.testing.assert_frame_equal(output_df, expected_output_df)
