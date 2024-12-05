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
        "derive": 4,
        "from": [1, 2, 3],
    }
    return derive_map


def post_imputation_processing(
    df: pd.DataFrame, period, reference, question_no, target
):
    question_no_mapping = create_derive_map()

    # Check if there is a returned total, remove this reference when deriving
    returned_total_reference = df.loc[
        (df[question_no] == question_no_mapping["derive"]) & (df["marker"] == "r"),
        reference,
    ].unique()
    print(returned_total_reference)

    # df_subset = df.loc[~df[reference].isin(returned_total_reference)]
    df_subset = df.set_index(
        [question_no, period, reference],
        verify_integrity=False,
    )
    df_subset = df_subset[[target]]

    derived_values = pd.concat(
        [
            calculate_totals(df_subset, question_no_mapping["from"]).assign(
                **{question_no: question_no_mapping["derive"]}
            )
        ]
    )

    # df_subset = df.loc[df[reference].isin(returned_total_reference)]
    # df_subset = df_subset.set_index(
    #     [question_no, period, reference],
    #     verify_integrity=False,
    # )
    # df_subset = df_subset[[target]]

    # constrain_imputed_values(df_subset, question_no_mapping["from"])

    # # Only checking references with returned total

    # df_returned_total_2 = df.loc[df[reference].isin(returned_total_reference)]
    # df_returned_total_2 = df_returned_total_2.set_index(
    #     [question_no, period, reference, "marker"],
    #     verify_integrity=False,
    # )
    # derived_values_2 = pd.concat(
    #     [
    #         calculate_totals(df_returned_total_2, question_no_mapping["from"]).assign(
    #             **{"question_no": question_no_mapping["derive"]}
    #         )
    #     ]
    # )
    # print("derived 2", derived_values_2)

    # check_imputed_values_constrained(df, question_no_mapping["from"])

    final_constrained = pd.concat([df, derived_values])

    return final_constrained


def check_imputed_values_constrained(
    df: pd.DataFrame, derive_from: list[int]
) -> pd.DataFrame:
    # derive total again, DO NOT OVERWRITE RETURN

    pass


if __name__ == "__main__":
    df = pd.read_csv("test.csv")
    df_out = post_imputation_processing(
        df, "period", "reference", "question_no", "target"
    )

    print(df_out)
