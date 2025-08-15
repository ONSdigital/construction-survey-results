from typing import List

import numpy as np
import pandas as pd
from pandas.api.types import is_bool_dtype


def create_missing_questions(
    responses: pd.DataFrame,
    contributors: pd.DataFrame,
    manual_constructions: pd.DataFrame,
    components_questions: List[int],
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
    components_questions: List[int]
        List of components_question codes which are expected in the responses.
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
        'target':[999,999],
        '290_flag':[False,False]
        }
    >>> responses = pd.DataFrame(data=d_responses)
        ref  period  questioncode  target   290_flag
     0    1  202201            90     999   False
     1    1  202202            91     999   False
    >>> questions = [90,91]
    >>> result = create_missing_questions(
        responses,
        contributors,
        questions,
        "ref",
        "period",
        "questioncode")
       ref  period  questioncode  target    290_flag
    0    1  202201            90   999.0    False
    1    1  202201            91   999.0    False
    2    1  202202            90     NaN    False
    3    1  202202            91     NaN    False
    4    2  202202            90     NaN    False
    5    2  202202            91     NaN    False
    """

    all_questions = components_questions + [290]

    contributors = contributors.set_index([reference, period]).index

    responses_questions = (
        responses.groupby([reference, period])[question_col].apply(list).to_frame()
    )

    q290 = responses.copy()
    q290 = q290[q290[question_col] == 290].set_index([reference, period])[
        ["290_flag", "is_total_only_and_zero"]
    ]

    responses_questions = pd.concat([responses_questions, q290], axis=1)

    responses_questions.loc[
        (responses_questions["290_flag"])
        | (responses_questions["is_total_only_and_zero"]),
        question_col,
    ] = np.nan

    responses_questions[question_col] = responses_questions[question_col].fillna(
        {row: all_questions for row in responses_questions.index}
    )

    # Creating a new column to save list of questions to be created
    responses_questions["missing_questions_helper"] = (
        responses_questions[question_col]
        .map(set(components_questions).intersection)
        .map(list)
    )

    # Sorting first by reference and then by period, for ffill
    expected_responses = (
        responses_questions.reindex(contributors)
        .reset_index()
        .sort_values([reference, period])
    )

    expected_responses["missing_questions_helper"] = expected_responses.groupby(
        [reference]
    )["missing_questions_helper"].ffill()

    # Handling expected questions when they exist in manual constructions
    # it adds the manual constructions to expected questions and forwards fill
    if isinstance(manual_constructions, pd.DataFrame):

        man_helper = (
            manual_constructions.groupby([reference, period])[question_col]
            .apply(list)
            .to_frame()
        ).reset_index()
        expected_responses = expected_responses.merge(
            man_helper, on=[reference, period], how="left", suffixes=("", "_man")
        )

        expected_responses[f"{question_col}_man"] = expected_responses.groupby(
            [reference]
        )[f"{question_col}_man"].ffill()

        # filling na with empty list to allow concating of 2 lists on next step
        expected_responses[f"{question_col}_man"] = (
            expected_responses[f"{question_col}_man"].fillna("").apply(list)
        )

        expected_responses["missing_questions_helper"] = expected_responses.apply(
            lambda row: list(
                set(row["missing_questions_helper"] + row[f"{question_col}_man"])
            ),
            axis=1,
        )

    expected_responses["missing_questions_helper"] = expected_responses[
        "missing_questions_helper"
    ].fillna({row: components_questions for row in expected_responses.index})

    # question col now has list of questions which were in the responses
    # if question col is na it means that it was missing and we should use the
    # list of questions in filling_helper column

    expected_responses[question_col] = expected_responses[question_col].fillna(
        expected_responses["missing_questions_helper"]
    )

    # We only have NAs for non-responders which should have False for 290_flag
    expected_responses["290_flag"] = expected_responses["290_flag"].fillna(False)

    expected_responses["is_total_only_and_zero"] = expected_responses[
        "is_total_only_and_zero"
    ].fillna(False)

    expected_responses.loc[
        expected_responses["is_total_only_and_zero"], question_col
    ] = 290

    expected_responses = expected_responses.explode(question_col, ignore_index=True)

    expected_rows_index = expected_responses.set_index(
        [reference, period, question_col, "290_flag", "is_total_only_and_zero"]
    ).index

    responses = responses.set_index(
        [reference, period, question_col, "290_flag", "is_total_only_and_zero"]
    )

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

    Raises
    ------
    ValueError
        If mask is not bool
    """

    if not is_bool_dtype(mask):
        raise ValueError(mask, "is not type", bool)

    df.loc[mask, col] = replace_with

    return df
