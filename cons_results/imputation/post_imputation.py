import logging
import os
import warnings

import numpy as np
import pandas as pd
from mbs_results.utilities.outputs import write_csv_wrapper


def rescale_290_case(
    df: pd.DataFrame,
    period: str,
    reference: str,
    question_no: str,
    adjusted_response: str,
) -> pd.DataFrame:

    """
    Will re adjust the components questions so their sum will match the question
    290. The ratio of rescaling is sum of components / question 290, then each
    component is multipied with the ratio.


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

    df["adjustedresponse_pre_rescale"] = df[adjusted_response].copy()

    # 290_flag hard coded column from flag_290_cases in staging
    numer = (
        df[(df["290_flag"]) & (df[question_no] == 290)]
        .groupby([period, reference])[adjusted_response]
        .sum()
    )

    denom = (
        df[(df["290_flag"]) & (df[question_no] != 290)]
        .groupby([period, reference])[adjusted_response]
        .sum()
    )

    ratio = numer / denom  # has inf when dem is 0

    ratio.name = "ratio"

    df["ratio"] = df.set_index([period, reference]).index.map(ratio)

    # want to avoid multiplying with inf and np.nan,
    # np.inf comes from division with 0

    multiple_mask = (df[question_no] != 290) & (
        np.isfinite(df["ratio"]) & (df["290_flag"])
    )

    df.loc[multiple_mask, adjusted_response] = (
        df[multiple_mask]["ratio"] * df[multiple_mask][adjusted_response]
    )

    df["failed_rescale"] = np.isinf(df["ratio"])

    df.drop(columns=["ratio"], inplace=True)

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
        (df[question_no] != 290) & (df[imputation_flag] != "r") & (df["290_flag"] == 0)
    )

    imputed_components_sum = (
        df[imputed_components_mask]
        .groupby([period, reference])[adjustedresponse]
        .sum()
        .reset_index()
        .rename(columns={adjustedresponse: "imputed_components_sum"})
    )

    df = df.merge(imputed_components_sum, on=[period, reference], how="left")

    df["imputed_components_sum"] = df["imputed_components_sum"].fillna(
        df[adjustedresponse]
    )

    q290_non_response = (df[question_no] == 290) & (df[imputation_flag] != "r")
    df.loc[q290_non_response, adjustedresponse] = df.loc[
        q290_non_response, "imputed_components_sum"
    ]

    df.loc[q290_non_response, imputation_flag] = "d"

    df = df.drop(columns=["imputed_components_sum"])

    return df


def validate_q290(
    df: pd.DataFrame,
    question_no: str,
    period: str,
    reference: str,
    adjustedresponse: str,
    config,
    output_path: str = "",
    output_file_name: str = "",
    import_platform: str = "network",
    bucket_name: str = "",
) -> None:
    """
    Validates that Q290 (total) values match the sum of their
    component questions for each period and reference.
    Raises a warning and optionally outputs a CSV if mismatches are found.
    df : pd.DataFrame
        Dataframe containing response-level data.
    question_no : str
        Name of the column in `df` containing question codes (e.g., "questioncode").
    period : str
        Name of the column in `df` representing the period (e.g., "period").
    reference : str
        Name of the column in `df` representing the reference variable.
    adjustedresponse : str
        Name of the column in `df` containing the adjusted response values.
    config : dict
        Configuration dictionary.
    output_path : str, optional
        Directory path where the output CSV of mismatches will be saved, if requested.
    output_file_name : str, optional
        Base name for the output CSV file. If empty, no file is written.
    import_platform : str, optional
        Platform identifier for file writing (e.g., "network" or "local").
        Default is "network".
    bucket_name : str, optional
        Name of the storage bucket if saving to S3. Default is empty.
    Returns
    -------
    None
    Warns
    -----
    UserWarning
        If any Q290 values do not match the sum of their components within a tolerance.
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
            output_file = f"{output_file_name}_{config['run_id']}"
            output_file = os.path.join(output_path, output_file_name)
            print(f"Saving mismatched q290 totals to {output_file}")

            write_csv_wrapper(
                df=mismatched_totals,
                save_path=output_file,
                import_platform=import_platform,
                bucket_name=bucket_name,
                index=False,
            )
    else:
        print("q290 values match the sum of components for all periods and references.")


def validate_r_before_derived_zero(
    df: pd.DataFrame,
    question_no: str,
    imputation_flag: str,
    period: str,
    reference: str,
) -> None:
    """
    Validates that for each period and reference, any component question with a
    derived_zero ('d') imputation flag is preceded by at least one response ('r')
    imputation flag. If a 'd' flag is found without a preceding 'r' flag, a warning
    is logged with the relevant period and reference combinations.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe containing response-level data.
    question_no : str
        Name of the column in `df` containing question codes (e.g., "questioncode").
    imputation_flag : str
        Name of the column in `df` containing imputation flags.
    period : str
        Name of the column in `df` representing the period (e.g., "period").
    reference : str
        Name of the column in `df` representing the reference variable.

    Returns
    -------
    None

    Warns
    -----
    """
    logger = logging.getLogger(__name__)

    components_only = df.copy()
    components_only = components_only[components_only[question_no] != 290]
    components_only.sort_values(by=[reference, period], inplace=True)

    grouped = components_only.groupby([reference, question_no]).agg(
        {imputation_flag: lambda x: x.to_list()}
    )

    result = grouped.apply(check_r_before_d)

    false_indices = result[result.eq(False)].index

    false_indices_list = false_indices.to_list()[0:5]

    if false_indices_list:
        logger.warning(
            f"""These reference and questioncode combinations
                       have a 'd' flag for components without being preceded
                       by a response, which may be an error.
                       Please check these: {false_indices_list}"""
        )


def check_r_before_d(list_of_flags):
    """Helper function to check if 'r' precedes 'd' in a list of imputation flags.
    Parameters
    ----------
    list_of_flags : list
        A list of imputation flags for a specific reference and question code."""

    d_indices = [i for i, j in enumerate(list_of_flags) if j == "d"]

    if "d" not in list_of_flags:
        return True

    if list_of_flags[d_indices[0] - 1] != "r":
        return False

    for index in d_indices[::-1]:
        if list_of_flags[index - 1] not in ["r", "d"]:
            return False

    return True
