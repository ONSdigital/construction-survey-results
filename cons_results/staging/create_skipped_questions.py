from typing import List

import pandas as pd


def create_skipped_questions(
    df: pd.DataFrame,
    all_questions: List[int],
    reference: str,
    period: str,
    question_col: str,
    target_col: str,
    contributors_keep_col: List[str],
    responses_keep_col: List[str],
    finalsel_keep_col: List[str],
    imputation_marker_col: str = "",
):

    # getting list of all references and period combinations
    df_index = (
        df[["reference", "period"]]
        .drop_duplicates()
        .set_index([reference, period])
        .index
    )

    responses_questions = (
        df.groupby([reference, period])[question_col].apply(list).to_frame()
    )

    # Creating a new column to save list of questions to be created
    responses_questions["skipped_question_col"] = responses_questions[question_col].map(
        lambda x: list(set(all_questions) - set(x))
    )

    # Sorting first by reference and then by period, for ffill
    expected_responses = (
        responses_questions.reindex(df_index)
        .reset_index()
        .sort_values([reference, period])
    )

    expected_responses["skipped_question_col"] = expected_responses[
        "skipped_question_col"
    ].fillna({row: all_questions for row in expected_responses.index})

    # question col now has list of questions which were in the responses
    # if question col is na it means that it was missing and we should use the
    # list of questions in filling_helper column

    # Combine the original and missing questions lists for each row
    expected_responses[question_col] = expected_responses.apply(
        lambda row: (row[question_col] if isinstance(row[question_col], list) else [])
        + (
            row["skipped_question_col"]
            if isinstance(row["skipped_question_col"], list)
            else []
        ),
        axis=1,
    )

    expected_responses = expected_responses.explode(question_col, ignore_index=True)
    # resetting type for created column ready for join
    expected_responses[question_col] = expected_responses[question_col].astype(int)

    expected_rows_index = expected_responses.set_index(
        [reference, period, question_col]
    ).index

    responses = df.set_index([reference, period, question_col])

    responses_full = responses.reindex(expected_rows_index).reset_index()

    # Best way to flag these newly created skipped questions at the moment.
    responses_full = responses_full.merge(
        expected_responses[[reference, period, question_col, "skipped_question_col"]],
        on=[reference, period, question_col],
        how="left",
    )
    # Assign column true if question no is found in skipped_question_col
    responses_full["skipped_question"] = responses_full.apply(
        lambda row: row[question_col] in row["skipped_question_col"]
        if isinstance(row["skipped_question_col"], list)
        else False,
        axis=1,
    )
    responses_full = responses_full.drop(columns=["skipped_question_col"])

    responses_full = fill_columns_in_created_questions(
        responses_full,
        reference,
        period,
        question_col,
        target_col,
        contributors_keep_col,
        responses_keep_col,
        finalsel_keep_col,
        imputation_marker_col,
    )

    return responses_full


def fill_columns_in_created_questions(
    df: pd.DataFrame,
    reference: str,
    period: str,
    question_col: str,
    target_col: str,
    contributors_keep_col: List[str],
    responses_keep_col: List[str],
    finalsel_keep_col: List[str],
    imputation_marker_col: str,
) -> pd.DataFrame:
    """
    filles columns which are information on a contributor level.
    Takes the union from all three dataframe keep column values from config, removes
    period, reference, question_number and target from columns to fill.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing the data.
    reference : str
        Column name for reference.
    period : str
        Column name for period.
    question_col : str
        Column name for question number.
    target_col : str
        Column name for target.
    contributors_keep_col : List[str]
        List of contributor columns to keep.
    responses_keep_col : List[str]
        List of response columns to keep.
    finalsel_keep_col : List[str]
        List of final selection columns to keep.
    imputation_marker_col : str
        Column name for imputation marker.

    Returns
    -------
    pd.DataFrame
        DataFrame with filled columns.
    """
    df.loc[df["skipped_question"], target_col] = 0

    # combining and filling columns
    columns_dont_fill = [reference, question_col, target_col, period]
    columns_to_fill = set(
        contributors_keep_col + responses_keep_col + finalsel_keep_col
    )
    columns_to_fill = list(columns_to_fill - set(columns_dont_fill))

    df[columns_to_fill] = df.groupby([reference, period])[columns_to_fill].transform(
        "ffill"
    )

    return df
