import os

import pandas as pd
from mbs_results.staging.back_data import append_back_data
from mbs_results.staging.data_cleaning import (
    convert_annual_thousands,
    convert_nil_values,
    enforce_datatypes,
    filter_out_questions,
    run_live_or_frozen,
)
from mbs_results.staging.dfs_from_spp import get_dfs_from_spp
from mbs_results.staging.stage_dataframe import read_and_combine_colon_sep_files
from mbs_results.utilities.inputs import read_csv_wrapper

from cons_results.staging.create_missing_questions import create_missing_questions
from cons_results.staging.derive_imputation_class import derive_imputation_class


def stage_dataframe(config: dict) -> pd.DataFrame:
    """
    wrapper function to stage and pre process the dataframe, ready to be passed onto the
    imputation wrapper (impute)

    Parameters
    ----------
    config : dict
        config containing paths and column names and file paths

    Returns
    -------
    pd.DataFrame
        Combined dataframe containing response and contributor data
    """

    staging_config = config.copy()
    period = staging_config["period"]
    reference = staging_config["reference"]
    snapshot_file_path = staging_config["snapshot_file_path"]

    contributors, responses = get_dfs_from_spp(
        snapshot_file_path,
        staging_config["platform"],
        staging_config["bucket"],
    )

    # Filter columns and set data types
    contributors = contributors[staging_config["contributors_keep_cols"]]
    contributors = enforce_datatypes(
        contributors,
        keep_columns=staging_config["contributors_keep_cols"],
        **staging_config,
    )

    # Drop imputation marker for contributors as it is only neccessary for responses
    contributors = append_back_data(contributors, staging_config).drop(
        columns=[staging_config["imputation_marker_col"]]
    )

    responses = responses[staging_config["responses_keep_cols"]]
    responses = enforce_datatypes(
        responses, keep_columns=staging_config["responses_keep_cols"], **staging_config
    )

    responses = append_back_data(responses, staging_config)

    # Add an extra month to the revison window to include the back data
    staging_config["revision_window"] = config["revision_window"] + 1

    finalsel = read_and_combine_colon_sep_files(staging_config)

    # keep columns is applied in data reading from source, enforcing dtypes
    # in all columns of finalsel
    finalsel = enforce_datatypes(
        finalsel, keep_columns=list(finalsel), **staging_config
    )

    # Filter contributors files here to temp fix this overlap

    contributors = pd.merge(
        left=contributors,
        right=finalsel,
        on=[period, reference],
        suffixes=["_spp", "_finalsel"],
        how="outer",
    )

    df = create_missing_questions(
        contributors=contributors,
        responses=responses,
        all_questions=staging_config["all_questions"],
        reference=staging_config["reference"],
        period=staging_config["period"],
        question_col=staging_config["question_no"],
    )

    df = pd.merge(left=df, right=contributors, on=[period, reference], how="left")

    snapshot_name = os.path.basename(snapshot_file_path).split(".")[0]

    df = filter_out_questions(
        df=df,
        column=staging_config["question_no"],
        questions_to_filter=staging_config["filter_out_questions"],
        save_full_path=staging_config["output_path"]
        + snapshot_name
        + "_filter_out_questions.csv",
        **staging_config,
    )

    df = run_live_or_frozen(
        df,
        staging_config["target"],
        status=staging_config["status"],
        state=staging_config["state"],
        error_values=[201],
    )

    df[staging_config["auxiliary_converted"]] = df[staging_config["auxiliary"]].copy()
    df = convert_annual_thousands(df, staging_config["auxiliary_converted"])

    df = derive_imputation_class(
        df,
        staging_config["bands"],
        staging_config["cell_number"],
        staging_config["imputation_class"],
    )

    if staging_config["manual_constructions_path"]:
        manual_constructions = read_csv_wrapper(
            staging_config["manual_constructions_path"],
            staging_config["platform"],
            staging_config["bucket"],
        )
    else:
        manual_constructions = None

    if staging_config["filter"]:
        filter_df = read_csv_wrapper(
            staging_config["filter"],
            staging_config["platform"],
            staging_config["bucket"],
        )
        filter_df = enforce_datatypes(filter_df, list(filter_df), **staging_config)

    else:
        filter_df = None

    df = flag_290_case(
        df,
        staging_config["period"],
        staging_config["reference"],
        staging_config["question_no"],
        staging_config["target"],
    )

    print("Staging Completed")

    df = convert_nil_values(
        df, config["nil_status_col"], config["target"], config["nil_values"]
    )
    return df, manual_constructions, filter_df


def flag_290_case(
    df: pd.DataFrame,
    period: str,
    reference: str,
    question_no: str,
    adjusted_response: str,
) -> pd.DataFrame:

    """
    Function to flag cases for imputation where value for question is 290
    is given with no other components

    Parameters
    ----------
    df : pd.Dataframe
        Input DataFrame which has unflagged 290 special cases.
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
        Output DataFrame with variable that flags 290 special cases.
    """

    # Group and sum adjusted responses for question 290
    question_290_df = (
        df[df[question_no] == 290].groupby([period, reference])[adjusted_response].sum()
    )

    # Group and sum adjusted responses for all other questions
    other_questions_df = (
        df[df[question_no] != 290].groupby([period, reference])[adjusted_response].sum()
    )

    # Merge groupings
    df_joined = pd.merge(
        question_290_df,
        other_questions_df,
        on=[period, reference],
    )

    # Create index of pairs of period and reference numbers which need to be
    # flagged as the special 290 case
    flagged_pairs = df_joined[
        (df_joined[f"{adjusted_response}_x"] != df_joined[f"{adjusted_response}_y"])
        & (df_joined[f"{adjusted_response}_y"] == 0)
    ].index

    # Initialise flag
    df["290_flag"] = False

    # Set flag based on index
    df.loc[
        pd.MultiIndex.from_frame(df[[period, reference]]).isin(flagged_pairs),
        ["290_flag"],
    ] = True

    # Return modified DataFrame
    return df
