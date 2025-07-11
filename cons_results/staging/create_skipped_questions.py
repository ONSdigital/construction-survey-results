import warnings
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
):
    """function to create skipped questions in the DataFrame."""
    if count_na := df[target_col].isna().sum() > 0:
        warnings.warn(
            f"DataFrame contains {count_na} rows with NaN in 'adjustedresponse'. "
            "These will be flagged as newly created skipped questions."
        )

    subset_df = (
        df[["reference", "period"]]
        .drop_duplicates()
        .set_index([reference, period])
        .index
    )

    responses_questions = (
        df.groupby([reference, period])[question_col].apply(list).to_frame()
    )
    # Creating a new column to save list of questions to be created
    responses_questions["missing_questions_helper"] = responses_questions[
        question_col
    ].map(lambda x: list(set(all_questions) - set(x)))

    # Sorting first by reference and then by period, for ffill
    expected_responses = (
        responses_questions.reindex(subset_df)
        .reset_index()
        .sort_values([reference, period])
    )

    expected_responses["missing_questions_helper"] = expected_responses.groupby(
        [reference]
    )["missing_questions_helper"].ffill()

    expected_responses["missing_questions_helper"] = expected_responses[
        "missing_questions_helper"
    ].fillna({row: all_questions for row in expected_responses.index})

    # question col now has list of questions which were in the responses
    # if question col is na it means that it was missing and we should use the
    # list of questions in filling_helper column

    # Combine the original and missing questions lists for each row
    expected_responses[question_col] = expected_responses.apply(
        lambda row: (row[question_col] if isinstance(row[question_col], list) else [])
        + (
            row["missing_questions_helper"]
            if isinstance(row["missing_questions_helper"], list)
            else []
        ),
        axis=1,
    )

    expected_responses = expected_responses.explode(question_col, ignore_index=True)

    expected_rows_index = expected_responses.set_index(
        [reference, period, question_col]
    ).index

    responses = df.set_index([reference, period, question_col])

    responses_full = responses.reindex(expected_rows_index).reset_index()

    # Best way to flag these newly created skipped questions at the moment.
    responses_full["skipped_question"] = responses_full[target_col].isna()
    responses_full[target_col].fillna(0, inplace=True)

    # combining and filling columns
    columns_dont_fill = [reference, question_col, target_col, period]
    columns_to_fill = (
        set(contributors_keep_col) | set(responses_keep_col) | set(finalsel_keep_col)
    )
    columns_to_fill = list(columns_to_fill - set(columns_dont_fill))

    responses_full[columns_to_fill] = responses_full.groupby([reference, period])[
        columns_to_fill
    ].transform("ffill")
    return responses_full
