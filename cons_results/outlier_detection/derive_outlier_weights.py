import pandas as pd


def derive_q290_outlier_weights(
    df: pd.DataFrame,
    all_questions: list,
    target: str,
    question_no: str,
    reference: str,
    period: str,
) -> pd.DataFrame:
    """
    Derives the outlier weights for question 290 rows, from the component questions.
    The formula for this calculation is:
    [(Q201 * q201_oweight) + (Q202 * q202_oweight)….+(Q243 * q243_oweight)] / Q290

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe containing the necessary data for calculations.
        It should have at least the following columns:
        "reference", "period", "question_no", "o_weights", "adjustedresponse".
    all_questions : list
        A list containing all of the component questions.
    target : str
        The target variable column name.
    question_no : str
        The question_no variable column name.
    reference : str
        The reference variable column name.
    period : str,
        The period variable column name.

    Returns
    -------
    df : pd.DataFrame
        Updated dataframe with derived weights for question 290 rows.

    Example
    -------
    >>> all_questions: [201, 202, 211, 212, 221, 222]
    >>> df = {
    'reference': [1, 1, 1],
    'period': [202201, 202201, 202201],
    'questioncode':[201, 202, 290],
    'target':[4, 5, 9],
    'o_weights': [0.5, 0.2]
    }
    >>> result = calculate_q290_o_weights(
    df,
    all_questions,
    target,
    question_no,
    reference,
    period
    )
    reference  period  questioncode  target  o_weights
    1    1  202201            201       4       0.5
    2    1  202201            202       5       0.2
    3    1  202201            290       9       0.33333

    """

    # Using the formula to calculate q290 outlier weights
    components = df[df[question_no].isin(all_questions)]
    components.loc[:, "o_weight_times_value"] = (
        components["outlier_weight"] * components[target]
    )
    sum_o_weight_times_value = components.groupby([reference, period])[
        "o_weight_times_value"
    ].sum()

    df.set_index([reference, period], inplace=True)

    # Setting outlier weight to 1 for q290 when component outlier weights are 1
    # (non-winsorised)
    avg_component_o_weight = (
        df[df[question_no].isin(all_questions)]
        .groupby(["reference", "period"])["outlier_weight"]
        .mean()
    )
    non_winsorised = avg_component_o_weight[avg_component_o_weight == 1.00]
    df.loc[
        df.index.isin(non_winsorised.index) & (df[question_no] == 290), "outlier_weight"
    ] = 1.00

    # Changing 290 outlier weight only when it hasn't already been set to 1
    q290_mask = (df[question_no] == 290) & (df["outlier_weight"] != 1.00)

    df.loc[q290_mask, "outlier_weight"] = (
        sum_o_weight_times_value / df.loc[q290_mask, target]
    )

    df.reset_index(inplace=True)

    return df
