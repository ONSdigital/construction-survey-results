import warnings
from typing import List

import numpy as np
import pandas as pd

# Case 1 - all responses - calculate total (even if given or not this will be same)
# Case 2 - some non responses - no total, impute non responses and calculate total
# Case 3 - some non responses - given total - impute non responses and re weight imputed
# case 4 - all non responses - no total, impute non responses and calculate total

# Can the total be imputed? my guess would be no


def calculate_totals(df: pd.DataFrame, derive_from: List[int]) -> pd.DataFrame:

    """
    Returns the sums of a dataframe in which the first level index is in
    derive_from. The sum is based on common indices. Columns must contain float
    or int.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe to sum, first level index must contain values from derive_from

    derive_from : List[int]
        Values to take a subset of df.

    Returns
    -------
    sums : pd.DataFrame
        A dataframe with sums, constain marker, and columns from index which the
        sum was based on.
    """

    df_temp = df.fillna(0)

    sums = sum(
        [
            df_temp.loc[question_no]
            for question_no in derive_from
            if question_no in df_temp.index
        ]
    )
    sums.rename(columns={"target": "derived_target"}, inplace=True)
    return sums.assign(constrain_marker=f"sum{derive_from}").reset_index()


def create_derive_map():
    """
    Function to create derive mapping dictionary
    Will check the unique values for form types and remove this
    from the dictionary if not present. handles error

    Returns
    -------
    dict
        Derived question mapping in a dictionary.
        Removes form IDs which are not present in dataframe
    """
    warnings.warn(
        "This might need to be expanded to allow for multiple derived questions "
        "and updated to correct question numbers"
    )
    derive_map = {
        "derive": 5,
        "from": [1, 2, 3, 4],
    }
    return derive_map


def post_imputation_processing(
    df: pd.DataFrame,
    period,
    reference,
    question_no,
    target,
    imputation_marker,
    **config,
) -> pd.DataFrame:
    """
    first outline of post imputation processing for the construction survey

    Parameters
    ----------
    df : pd.DataFrame
        post imputation dataframe
    period : _type_
        period column name
    reference : _type_
        reference column name
    question_no : _type_
        question number column name
    target : _type_
        target column name
    imputation_marker : _type_
        imputation marker column name
    **config: Dict
        main pipeline configuration. Can be used to input the entire config dictionary
    Returns
    -------
    pd.DataFrame
        post imputation dataframe with derived questions and rescaled values where
        needed
    """
    question_no_mapping = create_derive_map()

    df_subset = df.set_index(
        [question_no, period, reference], verify_integrity=False, drop=True
    )
    df_subset = df_subset[[target]]

    derived_values = calculate_totals(df_subset, question_no_mapping["from"]).assign(
        **{question_no: question_no_mapping["derive"]}
    )

    final_constrained = pd.merge(
        df, derived_values, on=[question_no, period, reference], how="outer"
    )

    final_constrained = (
        final_constrained.groupby([period, reference])
        .apply(
            lambda group_df: rescale_imputed_values(
                group_df, question_no, target, imputation_marker, question_no_mapping
            )
        )
        .reset_index(drop=True)
    )

    return final_constrained


def rescale_imputed_values(
    df: pd.DataFrame,
    question_no: str,
    target: str,
    imputation_marker: str,
    question_no_mapping: dict,
    drop_intermediate_cols: bool = False,
) -> pd.DataFrame:
    """
    rescales imputed / constructed values if total is a return.

    Parameters
    ----------
    df : pd.DataFrame
        original dataframe, grouped by period and reference
    question_no : str
        question number column name
    target : str
        target column name
    imputation_marker : str
        imputation marker column name
    question_no_mapping : dict
        dictionary containing question number derived and question numbers summed

    Returns
    -------
    pd.DataFrame
        original dataframe with adjusted values and rescale factors
    """

    reference_value = df["reference"].unique()[0]
    # question_no_mapping is nested dict
    derived_question_no_list = []
    for i in question_no_mapping:
        derived_question_no = question_no_mapping[i]["derive"]
        derived_question_no_list.append(derived_question_no)
    print(derived_question_no_list)

    derived_question_mask = df[question_no].isin(derived_question_no_list)

    # Checking if target and derived target are equal
    if df.loc[derived_question_mask, target].equals(
        df.loc[derived_question_mask, "derived_target"]
    ):
        df["adjusted_value"] = df[target]
        df["rescale_factor"] = np.nan
        return df

    # Check if all markers are 'r'
    if (df[imputation_marker].nunique() == 1) and (
        "r" in df[imputation_marker].unique()
    ):
        warnings.warn(
            "Derived and returned value are not equal."
            + f"All other values are returns. reference: {reference_value} \n"
        )
        df["adjusted_value"] = df[target]
        df["rescale_factor"] = np.nan
        return df

    # Handles if target is NaN i.e. not returned total
    if df.loc[derived_question_mask, target].isna().any():
        df["adjusted_value"] = df[target]
        df.loc[derived_question_mask, "adjusted_value"] = df.loc[
            derived_question_mask, "derived_target"
        ]
        df["rescale_factor"] = np.nan
        return df

    # If target and derived_target values are not equal for derived question, rescale
    sum_returned_exclude_total = df.loc[
        (~df[question_no].isin(derived_question_no_list))
        & (df[imputation_marker] == "r"),
        target,
    ].sum()
    sum_imputed = df.loc[
        (~df[question_no].isin(derived_question_no_list))
        & (df[imputation_marker] != "r"),
        target,
    ].sum()

    # Calculate the rescale factor
    rescale_factor = (
        df.loc[derived_question_mask, target].values[0] - sum_returned_exclude_total
    ) / sum_imputed
    df["rescale_factor"] = np.where(df[imputation_marker] != "r", rescale_factor, 1)
    df.loc[derived_question_mask, "rescale_factor"] = np.nan

    # Apply the rescale factor to the target values
    df["adjusted_value"] = df[target] * df["rescale_factor"]

    # Set derived question value to target if a return, derived otherwise
    df.loc[derived_question_mask, "adjusted_value"] = np.where(
        df.loc[derived_question_mask, imputation_marker] == "r",
        df.loc[derived_question_mask, target],
        df.loc[derived_question_mask, "derived_target"],
    )

    if drop_intermediate_cols:
        df.drop(columns=["adjusted_value"], inplace=True)

    return df  # Return the modified DataFrame


if __name__ == "__main__":
    derive_map_nested = {
        "map_1": {
            "derive": 5,
            "from": [1, 2, 3, 4],
        },
        "map_2": {
            "derive": 6,
            "from": [
                1,
                2,
            ],
        },
    }

    derive_map = {"derive": 5, "from": [1, 2, 3, 4]}
    q_list = []
    for i in derive_map:
        if i == "derive":
            q_no = derive_map.get(i)
            q_list.append(q_no)
            print(q_list)
