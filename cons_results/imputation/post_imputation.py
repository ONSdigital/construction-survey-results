import warnings
from typing import List

import numpy as np
import pandas as pd


def rescale_290_case(
    df: pd.DataFrame,
    period: str,
    reference: str,
    question_no: str,
    adjusted_response: str,
) -> pd.DataFrame:

    """
    Forward impute and rescale components for flagged 290 special cases
    """

    flagged_pairs = df[df["290_flag"]].groupby([period, reference]).sum().index

    all_pairs = pd.MultiIndex.from_frame(df[[period, reference]])

    for per, ref in flagged_pairs:

        impute_source = df[all_pairs.isin([(per, ref)])]

        numer = impute_source[impute_source[question_no] == 290][
            adjusted_response
        ].sum()

        denom = impute_source[impute_source[question_no] != 290][
            adjusted_response
        ].sum()

        if denom != 0:
            rescale_factor = numer / denom

            imputed_data = impute_source[impute_source[question_no] != 290][
                [question_no, adjusted_response]
            ]

            imputed_data[adjusted_response] *= rescale_factor

            for entry in imputed_data.to_dict("records"):

                df.loc[
                    (all_pairs.isin([(per, ref)]))
                    & (df[question_no] == entry[question_no]),
                    [adjusted_response],
                ] = entry[adjusted_response]

    return df
