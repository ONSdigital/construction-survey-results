from typing import List

import pandas as pd


def create_skipped_questions(
    df: pd.DataFrame,
    all_questions: List[int],
    reference: str,
    period: str,
    question_col: str,
):
    """function to create skipped questions in the DataFrame."""

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

    responses_full = responses_full.merge(
        expected_responses,
        on=[reference, period, question_col],
        how="left",
        suffixes=("", "_expected"),
    )
    responses_full["skipped_question"] = responses_full[question_col].isin(
        responses_full["missing_questions_helper"]
    )
    return responses_full


if __name__ == "__main__":
    # Example usage
    df = pd.DataFrame(
        {
            "reference": [1, 1, 2, 2],
            "period": ["202301", "202301", "202301", "202302"],
            "question": ["Q1", "Q2", "Q1", "Q3"],
            "response": [5, 3, 4, 2],
        }
    )
    create_skipped_questions(
        df,
        reference="reference",
        period="period",
        question_col="question",
        all_questions=["Q1", "Q2", "Q3", "Q4"],
    )
