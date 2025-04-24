from typing import List

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
        If a reference has not responsed in a specific period then look to
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
        expected questions which were not in oroginal responses.

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
    >>> result = generate_expected_questions(
        responses,
        contributors,
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

    responses_questions = responses.groupby([reference, period])[question_col].apply(
        list
    )

    # Sorting first by reference and then by period, for ffill
    expected_responses = (
        responses_questions.reindex(contributors)
        .reset_index()
        .sort_values([reference, period])
    )

    expected_responses[question_col] = expected_responses.groupby([reference])[
        question_col
    ].ffill()

    expected_responses[question_col] = expected_responses[question_col].fillna(
        {row: all_questions for row in expected_responses.index}
    )

    expected_responses = expected_responses.explode(question_col, ignore_index=True)

    expected_rows_index = expected_responses.set_index(
        [reference, period, question_col]
    ).index

    responses = responses.set_index([reference, period, question_col])

    responses_full = responses.reindex(expected_rows_index).reset_index()

    return responses_full
