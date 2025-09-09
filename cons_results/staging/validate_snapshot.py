import warnings

import pandas as pd


def validate_snapshot(
    responses: pd.DataFrame,
    contributors: pd.DataFrame,
    status: str,
    reference: str,
    period: str,
    non_response_statuses: list,
):
    """
    Validate that there are no reference and period groupings that are listed as
    non-response statuses in contributors but are present in responses.

    Parameters
    ----------
    responses : pd.DataFrame
        DataFrame containing survey responses.
    contributors : pd.DataFrame
        DataFrame containing contributor information.
    config : dict
        Configuration dictionary containing column names.

    Raises
    ------
    Warning
        If any reference and period groupings are found in both non-response statuses
        in contributors and in responses, a warning is raised with details.
    """
    non_responses = contributors[contributors[status].isin(non_response_statuses)]

    non_responses = non_responses.set_index([reference, period])

    responses = responses.set_index([reference, period])

    non_response_in_responses = responses.loc[non_responses.index]

    indices = non_response_in_responses.index

    if len(non_response_in_responses) > 0:
        warning_message = f"""There are {len(non_response_in_responses)} period and
        reference groupings that are listed as non-response statuses in contributors
        but are present in responses. The first 5 (or less) of these are:
    {indices[:min(5, len(non_response_in_responses))].to_list()}"""

        warnings.warn(warning_message)
