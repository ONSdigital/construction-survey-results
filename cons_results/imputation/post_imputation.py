import os
import warnings

import pandas as pd


def rescale_290_case(
    df: pd.DataFrame,
    period: str,
    reference: str,
    question_no: str,
    adjusted_response: str,
) -> pd.DataFrame:

    """
    Forward impute and rescale components for flagged 290 special cases

    Parameters
    ----------
    df : pd.Dataframe
        Input DataFrame which has imputed responses for flagged 290 special cases.
    period : str
        Column name containing period variable.
    reference : str
        Column name containing reference variable.
    question_no : str
        Column name containing question_col variable.
    adjusted_response: str
        Column name containing adjusted response for a question code.

    Returns
    -------
    pd.DataFrame
        Output DataFrame with rescaled adjusted responses for flagged 290 special cases.
    """

    flagged_pairs = df[df["290_flag"]].groupby([period, reference]).sum().index

    all_pairs = pd.MultiIndex.from_frame(df[[period, reference]])

    # initialise the failed_rescale imputed column as false
    # This indicates where components sum to zero and we cannot scale correctly
    df["failed_rescale"] = False

    for per, ref in flagged_pairs:

        impute_source = df[all_pairs.isin([(per, ref)])]

        numer = impute_source[impute_source[question_no] == 290][
            adjusted_response
        ].sum()

        denom = impute_source[impute_source[question_no] != 290][
            adjusted_response
        ].sum()

        if denom != 0:
            rescale_factor = numer / denom

            imputed_data = impute_source[impute_source[question_no] != 290][
                [question_no, adjusted_response]
            ]

            imputed_data[adjusted_response] *= rescale_factor

            for entry in imputed_data.to_dict("records"):

                df.loc[
                    (all_pairs.isin([(per, ref)]))
                    & (df[question_no] == entry[question_no]),
                    [adjusted_response],
                ] = entry[adjusted_response]
        else:
            # If denom is 0, we need to create new flag to stop deriving q290 in
            # this special case
            df.loc[
                (df["period"] == per) & (df["reference"] == ref), "failed_rescale"
            ] = True
            print("here")

    return df


def create_q290(
    df: pd.DataFrame,
    config: dict,
    reference: str,
    period: str,
    question_no: str,
    adjustedresponse: str,
    imputation_flag: str,
) -> pd.DataFrame:
    """
    Creates rows for questioncode = 290 when these do not exist in
    a period/reference group.

    Parameters
      ----------
      df: pd.Dataframe
        The main construction dataframe following the staging module.
      config: pd.DataFrame
        The config as a dictionary
      reference: str
        Column name containing reference variable
      period: str
        Column name containing period variable
      question_no: str
        Column name containing the question number variable
      adjustedresponse: str
        Column name containing the adjustedresponse variable
      imputation_flag: str
        Column name containing the imputation flag variable


    Returns
    -------
    df
      A dataframe with rows added for questioncode 290 where these were missing
      in the period/reference group.
    """

    missing_290 = df.groupby([period, reference]).filter(
        lambda x: 290 not in x[question_no].values
    )[[period, reference]]

    missing_290.drop_duplicates(inplace=True)

    # d_create is temporary for debugging but shouldn't appear in final data
    missing_290 = missing_290.assign(
        **{
            question_no: 290,
            adjustedresponse: 0.0,
            imputation_flag: "d_create",
            "290_flag": False,
        }
    )

    missing_290.set_index([period, reference], inplace=True)

    contributors_cols = list(
        set(config["contributors_keep_cols"] + config["finalsel_keep_cols"])
    )
    contributors_df = df[contributors_cols]
    contributors_df = contributors_df.groupby([period, reference]).first()

    missing_290 = pd.concat(
        [missing_290, contributors_df.loc[missing_290.index]], axis=1
    )
    missing_290.reset_index(inplace=True)

    df = pd.concat([df, missing_290]).reset_index(drop=True)

    return df


def derive_q290(
    df: pd.DataFrame,
    question_no: str,
    imputation_flag: str,
    period: str,
    reference: str,
    adjustedresponse: str,
) -> pd.DataFrame:
    """
    Parameters
      ----------
      df: pd.Dataframe
        The main construction dataframe following the staging module.
      question_no: str
        Column name containing the question number variable
      imputation_flag: str
        Column name containing the imputation flag variable
      period: str
        Column name containing the period variable
      reference: str
        Column name containing reference variable
      adjustedresponse: str
        Column name containing the adjustedresponse variable


      Returns
      -------
      df
        A dataframe with 290 derived in rows where components were imputed.
    """

    imputed_components_mask = (
        (df[question_no] != 290) & (df[imputation_flag] != "r") & (~df["290_flag"])
    )

    imputed_components_sum = (
        df[imputed_components_mask]
        .groupby([period, reference])[adjustedresponse]
        .sum()
        .reset_index()
        .rename(columns={adjustedresponse: "imputed_components_sum"})
    )

    df = df.merge(imputed_components_sum, on=[period, reference], how="left")

    q290_mask = df[question_no] == 290
    df["imputed_components_sum"].fillna(df[adjustedresponse], inplace=True)
    df.loc[q290_mask, adjustedresponse] = df.loc[q290_mask, "imputed_components_sum"]

    df.loc[(q290_mask) & (df[imputation_flag] != "r"), imputation_flag] = "d"

    df = df.drop(columns=["imputed_components_sum"])

    return df


def validate_q290(
    df: pd.DataFrame,
    question_no: str,
    period: str,
    reference: str,
    adjustedresponse: str,
    output_path: str = "",
    output_file_name: str = "",
) -> None:
    """
    validation function to check q290 values and raise warnings if they
    are not as expected.

    Parameters
    ----------
    df : pd.DataFrame
        _description_
    """
    q290_mask = df[question_no] == 290
    df_q290 = df[q290_mask]
    temp = (
        df[~q290_mask]
        .groupby([period, reference])[adjustedresponse]
        .sum()
        .reset_index()
        .rename(columns={adjustedresponse: "components_sum"})
    )
    df_q290 = df_q290.merge(temp, on=[period, reference], how="left")
    mismatched_totals = df_q290.loc[
        abs(df_q290[adjustedresponse] - df_q290["components_sum"]) >= 1e-3,
        [period, reference, adjustedresponse, "components_sum", "failed_rescale"],
    ]
    if not mismatched_totals.empty:
        warnings.warn(
            "q290 values do not match the sum of components for "
            f"{len(mismatched_totals)} periods and references: "
            f"{mismatched_totals[[period, reference]].to_dict(orient='records')}"
        )
        if output_file_name != "":
            # Only output file if a name is provided
            output_file = os.path.join(output_path, output_file_name)
            print(f"Saving mismatched q290 totals to {output_file}")
            mismatched_totals.to_csv(output_file, index=False)
    else:
        print("q290 values match the sum of components for all periods and references.")
