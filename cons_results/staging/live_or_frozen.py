from typing import List

import pandas as pd
from mbs_results.utilities.utils import convert_column_to_datetime


def run_live_or_frozen(
    responses: pd.DataFrame,
    contributors: pd.DataFrame,
    period,
    reference,
    question_no,
    target: str or list[str],
    status: str,
    current_period: int,
    revision_window: int,
    state: str = "live",
    error_values: List[str] = ["Check needed"],
) -> (pd.DataFrame, pd.DataFrame):
    """
    For frozen contributors in error are treated as non-response and dropped
    from responses, exporting the responses in error as a seperate dataframe.

    For live returning original responses dataframe and empty
    frozen_responses_in_error. Note that frozen_responses_in_error will be
    merged again to respones in staging so initiliasing it with required columns.

    Parameters
    ----------
    responses : pd.DataFrame
        Dataframe with responses.
    contributors : pd.DataFrame
        Dataframe with contributors.
    target : str or list[str]
        Column(s) to treat as non-response.
    period : str
        Column name with period variable
    reference : str
        Column name with reference variable
    question_no : str
        Column name with question_no variable
    status : str
        Column containing error status.
    current_period : int
        Current period to in YYYYMM format, this it used to calculate backdata period
    revision_window : int
        Revision window in months, this it used to calculate backdata period
    state : str, optional
        Function config parameter. The default is "live". "live" state won't do
        anyting, "frozen" will convert to null the error_values within status
    error_values : list[str], optional
        Values to ignore. The default is ['Check needed'].
        Mapping:
            'Check needed' : '201', ("E" or "W" for CSW)
            'Clear' : '210',
            'Clear - overridden' : '211'

    Returns
    -------
    responses : pd.DataFrame
        Original responses without contributors in error.
    frozen_responses_in_error : pd.DataFrame
        Original responses witj contributors in error.
    """

    responses = responses.copy()

    if state not in ["frozen", "live"]:
        raise ValueError(
            """{} is not an accepted state status, use either frozen or live """.format(
                state
            )
        )

    frozen_responses_in_error = pd.DataFrame(
        columns=[period, reference, question_no, f"live_{target}"]
    )

    # using current period and revision window to calculate backdata period
    # TODO: look at moving is_backdata flag earlier in pipeline to use here
    backdata_period = convert_column_to_datetime(current_period) - pd.DateOffset(
        months=revision_window
    )

    if state == "frozen":

        con_in_error = contributors.loc[
            (contributors[status].isin(error_values))
            & (contributors[period] != backdata_period)
        ]

        con_in_error = con_in_error[[period, reference]]

        responses = responses.merge(con_in_error, how="outer", indicator=True)

        frozen_responses_in_error = responses[(responses._merge == "both")].copy()

        frozen_responses_in_error[f"live_{target}"] = frozen_responses_in_error[
            target
        ].copy()

        frozen_responses_in_error = frozen_responses_in_error[
            [reference, period, question_no, f"live_{target}"]
        ]

        responses = responses[(responses._merge == "left_only")].drop("_merge", axis=1)

        responses.reset_index(drop=True, inplace=True)
        frozen_responses_in_error.reset_index(drop=True, inplace=True)

    return responses, frozen_responses_in_error
