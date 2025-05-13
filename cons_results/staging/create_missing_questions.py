from typing import List

import numpy as np
import pandas as pd


def create_missing_questions(
    responses: pd.DataFrame,
    contributors: pd.DataFrame,
    all_questions: List[int],
    reference: str,
    period: str,
    question_col: str,
) -> pd.DataFrame:
    """
    Generates expected questions by checking which references and questions exist in
    responses and which references exist in contributors.

    The rules are:
        If a reference has not responded in a specific period then look to
        prior period to generate which questions should had been responded

        If a reference has never responded then generate all possible questions

    Period must be sortable in correct order, this is fine if dtype is datetime
    or integer,str in a format which can be sorted, e,g, yyyymm. This will not
    work if date is in mmyyyy.

    Parameters
    ----------
    responses : pd.Dataframe
        Dataframe containing responses.
        Reference,period,question_col unique identifiers in responses.
    contributors : pd.DataFrame
        Dataframe containing contributors.
        Reference,period unique identifiers in contributors.
    all_questions: List[int]
        List of all question codes which are expected in the responses.
    reference : str
        Column name containing reference variable, must exist in both
        responses and contributors.
    period : str
        Column name containing period variable, must exist in both
        responses and contributors.
    question_col : str
        Column name containing question_col variable, must exist in responses.

    Returns
    -------
    responses_full : pd.DataFrame
        A dataframe containing full responses, original plus missing rows for
        expected questions which were not in original responses.

    Examples
    --------
    >>> d_contributors = {'ref': [1,1,2], 'period': [202201,202202,202202]}
    >>> contributors = pd.DataFrame(data=d_contributors)
            ref  period
         0    1  202201
         1    1  202202
         2    2  202202

    >>> d_responses = {
        'ref': [1, 1],
        'period': [202201,202201],
        'questioncode':[90,91],
        'target':[999,999]
        }
    >>> responses = pd.DataFrame(data=d_responses)
        ref  period  questioncode  target
     0    1  202201            90     999
     1    1  202202            91     999
    >>> questions = [90,91]
    >>> result = create_missing_questions(
        responses,
        contributors,
        questions,
        "ref",
        "period",
        "questioncode")
       ref  period  questioncode  target
    0    1  202201            90   999.0
    1    1  202201            91   999.0
    2    1  202202            90     NaN
    3    1  202202            91     NaN
    4    2  202202            90     NaN
    5    2  202202            91     NaN
    """

    contributors = contributors.set_index([reference, period]).index

    responses_questions = (
        responses.groupby([reference, period])[question_col].apply(list).to_frame()
    )

    responses_questions["filling_helper"] = (
        responses_questions[question_col].map(set(all_questions).intersection).map(list)
    )

    # Sorting first by reference and then by period, for ffill
    expected_responses = (
        responses_questions.reindex(contributors)
        .reset_index()
        .sort_values([reference, period])
    )

    expected_responses["filling_helper"] = expected_responses.groupby([reference])[
        "filling_helper"
    ].ffill()

    expected_responses["filling_helper"] = expected_responses["filling_helper"].fillna(
        {row: all_questions for row in expected_responses.index}
    )

    # question col now has list of questions which were in the responses
    # if question col is na it means that it was missing and we should use the
    # list of questions in filling_helper column

    expected_responses[question_col] = expected_responses[question_col].fillna(
        expected_responses["filling_helper"]
    )

    expected_responses = expected_responses.explode(question_col, ignore_index=True)

    expected_rows_index = expected_responses.set_index(
        [reference, period, question_col]
    ).index

    responses = responses.set_index([reference, period, question_col])

    responses_full = responses.reindex(expected_rows_index).reset_index()

    return responses_full


def convert_values(
    df: pd.DataFrame, col: str, mask: pd.Series, replace_with=np.nan
) -> pd.DataFrame:
    """
    Replaces flaged (or masked) values in a dataframe col with replace_with value,
    default is numpy na.

    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe.
    col : str
        df column to replace values.
    mask : pd.Series
        Mask of the dataframe this must be a bool series or a condition.
    replace_with : float, optional
        Flaged value will be replaced with this, data type should be as dtype
        of col. The default is np.nan.

    Returns
    -------
    df : pd.DataFrame
        Original dataframe with updated flaged values.

    """

    if type(mask.dtypes) is not bool:
        raise ValueError(mask, "is not type", bool)

    # np.nan is float
    if df.dtypes[col] is not type(replace_with):
        raise ValueError(col, "dtype not the same as replace value")

    df.loc[mask, col] = replace_with

    return df
