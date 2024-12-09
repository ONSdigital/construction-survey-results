import warnings

import numpy as np
import pandas as pd

# Case 1 - all responses - calculate total (even if given or not this will be same)
# Case 2 - some non responses - no total, impute non responses and calculate total
# Case 3 - some non responses - given total - impute non responses and re weight imputed
# case 4 - all non responses - no total, impute non responses and calculate total

# Can the total be imputed? my guess would be no


def calculate_totals(df: pd.DataFrame, derive_from: list[int]) -> pd.DataFrame:

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
    # difference between using sum or agg RE NaNs
    # Temp fix, fillna with 0.
    # Add info the backlog ticket to replace this temp fix after combining columns
    df_temp = df.fillna(0)

    sums = sum(
        [
            df_temp.loc[question_no]
            for question_no in derive_from
            if question_no in df_temp.index
        ]
    )
    sums.rename(columns={"target": "derived_target"}, inplace=True)
    print(sums)

    return sums.assign(constrain_marker=f"sum{derive_from}").reset_index()


def constrain_imputed_values(df: pd.DataFrame, derive_from: list[int]) -> pd.DataFrame:
    df_temp = df.fillna(0)

    sums = sum(
        [
            df_temp.loc[question_no]
            for question_no in derive_from
            if question_no in df_temp.index
        ]
    )
    print("sums: \n", sums)


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

    derive_map = {
        "derive": 5,
        "from": [1, 2, 3, 4],
    }
    return derive_map


def post_imputation_processing(
    df: pd.DataFrame, period, reference, question_no, target, marker
):
    question_no_mapping = create_derive_map()

    # Check if there is a returned total, remove this reference when deriving
    # returned_total_reference = df.loc[
    #     (df[question_no] == question_no_mapping["derive"]) & (df["marker"] == "r"),
    #     reference,
    # ].unique()

    # df_subset = df.loc[~df[reference].isin(returned_total_reference)]
    df_subset = df.set_index(
        [question_no, period, reference], verify_integrity=False, drop=True
    )
    df_subset = df_subset[[target]]

    derived_values = pd.concat(
        [
            calculate_totals(df_subset, question_no_mapping["from"]).assign(
                **{question_no: question_no_mapping["derive"]}
            )
        ]
    )

    final_constrained = pd.merge(
        df, derived_values, on=[question_no, period, reference], how="outer"
    )
    print(final_constrained.columns)

    final_constrained = final_constrained.groupby([period, reference]).apply(
        lambda df: rescale_imputed_values(df, question_no, marker, question_no_mapping)
    )

    return final_constrained


def rescale_imputed_values(
    df: pd.DataFrame, question_no: str, marker: str, question_no_mapping: dict
) -> pd.DataFrame:
    # TODO: remove hard coded refrence to question_no 4
    # deal with derived nans when question does not exist? - not urgent
    #
    # Check if the target and derived_target values are equal for question_no 4
    reference_value = df["reference"].unique()[0]
    derived_question_mask = df[question_no] == question_no_mapping["derive"]

    if df.loc[df[question_no] == question_no_mapping["derive"], "target"].equals(
        df.loc[df[question_no] == question_no_mapping["derive"], "derived_target"]
    ):
        print("values are equal, reference: {} \n".format(reference_value))
        df["adjusted_value"] = df["target"]
        return df  # Return the DataFrame as is

    # Check if all markers are 'r'
    if (df[marker].nunique() == 1) and ("r" in df[marker].unique()):
        warnings.warn(
            """Derived and returned value are not equal.
            All other values are returns. reference: {} \n""".format(
                reference_value
            )
        )
        df["adjusted_value"] = df["target"]
        return df  # Return the DataFrame as is

    if df.loc[df[question_no] == question_no_mapping["derive"], "target"].isna().any():
        df["adjusted_value"] = df["target"]
        df.loc[derived_question_mask, "adjusted_value"] = df.loc[
            derived_question_mask, "derived_target"
        ]
        return df

    # If the target and derived_target values are not equal for derived q, rescale
    print(
        "values are not equal - need to rescale. reference: {} \n".format(
            reference_value
        )
    )
    sum_returned_exclude_total = df.loc[
        (df[question_no] != question_no_mapping["derive"]) & (df[marker] == "r"),
        "target",
    ].sum()
    sum_imputed = df.loc[
        (df[question_no] != question_no_mapping["derive"]) & (df[marker] != "r"),
        "target",
    ].sum()

    # Calculate the rescale factor
    df["rescale_factor"] = (
        df.loc[df[question_no] == question_no_mapping["derive"], "target"]
        - sum_returned_exclude_total
    ) / sum_imputed
    df.loc[df[marker] != "r", "rescale_factor"] = df.loc[
        df[question_no] == question_no_mapping["derive"], "rescale_factor"
    ].values[0]
    df.loc[df[marker] == "r", "rescale_factor"] = 1
    df.loc[derived_question_mask, "rescale_factor"] = None

    # Apply the rescale factor to the target values
    df.loc[df[question_no] != question_no_mapping["derive"], "adjusted_value"] = (
        df["target"] * df["rescale_factor"]
    )

    df.loc[derived_question_mask, "adjusted_value"] = np.where(
        df.loc[derived_question_mask, marker] == "r",
        df.loc[derived_question_mask, "target"],
        df.loc[derived_question_mask, "derived_target"],
    )

    return df  # Return the modified DataFrame


def check_imputed_values_constrained(
    df: pd.DataFrame, derive_from: list[int]
) -> pd.DataFrame:
    # derive total again, DO NOT OVERWRITE RETURN

    pass


if __name__ == "__main__":
    df = pd.read_csv("test.csv")
    # df["target"] = df["target"].fillna(0)
    df_out = post_imputation_processing(
        df, "period", "reference", "question_no", "target", "marker"
    )

    print(df_out)
