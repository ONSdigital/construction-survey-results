import numpy as np
import pandas as pd
from mbs_results.staging.back_data import append_back_data
from mbs_results.staging.data_cleaning import (
    convert_annual_thousands,
    convert_nil_values,
    enforce_datatypes,
    filter_out_questions,
)
from mbs_results.staging.dfs_from_spp import get_dfs_from_spp
from mbs_results.staging.stage_dataframe import read_and_combine_colon_sep_files
from mbs_results.utilities.inputs import read_csv_wrapper

from cons_results.staging.create_missing_questions import create_missing_questions
from cons_results.staging.create_skipped_questions import create_skipped_questions
from cons_results.staging.derive_imputation_class import derive_imputation_class
from cons_results.staging.live_or_frozen import run_live_or_frozen
from cons_results.staging.total_as_zero import flag_total_only_and_zero
from cons_results.staging.validate_snapshot import validate_snapshot


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

    validate_snapshot(
        responses=responses,
        contributors=contributors,
        status="status",
        reference=staging_config["reference"],
        period=staging_config["period"],
        non_response_statuses=config["non_response_statuses"]  # noqa
        + config["nil_values"],  # noqa
    )  # noqa

    # Filter columns and set data types
    contributors = contributors[staging_config["contributors_keep_cols"]]
    contributors = enforce_datatypes(
        contributors,
        keep_columns=staging_config["contributors_keep_cols"],
        **staging_config,
    )

    # Drop imputation marker for contributors as it is only neccessary for responses
    contributors = (
        append_back_data(contributors, staging_config)
        .drop(columns=[staging_config["imputation_marker_col"]])
        .drop_duplicates()
    )

    responses = responses[staging_config["responses_keep_cols"]]

    responses = enforce_datatypes(
        responses, keep_columns=staging_config["responses_keep_cols"], **staging_config
    )

    responses = append_back_data(responses, staging_config)

    responses, unprocessed_data = filter_out_questions(
        df=responses,
        column=staging_config["question_no"],
        questions_to_filter=staging_config["filter_out_questions"],
    )

    responses = enforce_datatypes(
        responses, keep_columns=staging_config["responses_keep_cols"], **staging_config
    )

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

    responses, frozen_responses_in_error = run_live_or_frozen(
        responses=responses,
        contributors=contributors,
        period="period",
        reference="reference",
        question_no="questioncode",
        target=staging_config["target"],
        status=staging_config["status"],
        current_period=config["current_period"],
        revision_window=config["revision_window"],
        state=staging_config["state"],
        error_values=[201],
    )

    responses = flag_290_case(
        responses,
        contributors,
        staging_config["period"],
        staging_config["reference"],
        staging_config["question_no"],
        staging_config["target"],
    )

    responses = flag_total_only_and_zero(
        responses,
        contributors,
        staging_config["reference"],
        staging_config["period"],
        staging_config["target"],
        staging_config["question_no"],
    )

    if staging_config["manual_constructions_path"]:
        manual_constructions = read_csv_wrapper(
            staging_config["manual_constructions_path"],
            staging_config["platform"],
            staging_config["bucket"],
        )

        manual_constructions = enforce_datatypes(
            manual_constructions, keep_columns=list(manual_constructions), **config
        )

    else:
        manual_constructions = None

    df = create_missing_questions(
        contributors=contributors,
        responses=responses,
        manual_constructions=manual_constructions,
        components_questions=staging_config["components_questions"],
        reference=staging_config["reference"],
        period=staging_config["period"],
        question_col=staging_config["question_no"],
    )

    df = pd.merge(left=df, right=contributors, on=[period, reference], how="left")

    unprocessed_data = enforce_datatypes(
        unprocessed_data, list(unprocessed_data), **staging_config
    )
    # Get extra variables for unprocessed_data too
    unprocessed_data = pd.merge(
        left=unprocessed_data, right=contributors, on=[period, reference], how="left"
    )

    # Skipping questions for clear, clear overridden and nil contributors
    status_values_to_skip = ["Clear", "Clear - overridden"] + config["nil_values"]

    df = create_skipped_questions(
        df=df,
        all_questions=staging_config["components_questions"],
        reference=staging_config["reference"],
        period=staging_config["period"],
        question_col=staging_config["question_no"],
        target_col=staging_config["target"],
        contributors_keep_col=staging_config["contributors_keep_cols"],
        responses_keep_col=staging_config["responses_keep_cols"],
        finalsel_keep_col=staging_config["finalsel_keep_cols"],
        status_col=staging_config["nil_status_col"],
        status_filter=status_values_to_skip,
        flag_col_name="skipped_question",
        imputation_marker_col=staging_config["imputation_marker_col"],
    )

    df = pd.merge(
        left=df,
        right=frozen_responses_in_error,
        on=["period", "reference", "questioncode"],
        how="left",
    )

    df = convert_annual_thousands(
        df, staging_config["auxiliary_converted"], staging_config["auxiliary"]
    )

    df = derive_imputation_class(
        df,
        staging_config["bands"],
        staging_config["cell_number"],
        staging_config["imputation_class"],
    )

    if staging_config["filter"]:
        filter_df = read_csv_wrapper(
            staging_config["filter"],
            staging_config["platform"],
            staging_config["bucket"],
        )
        filter_df = enforce_datatypes(filter_df, list(filter_df), **staging_config)

    else:
        filter_df = None

    df = convert_nil_values(
        df, config["nil_status_col"], config["target"], config["nil_values"]
    )

    print("Staging Completed")

    return df, unprocessed_data, manual_constructions, filter_df


def flag_290_case(
    responses: pd.DataFrame,
    contributors: pd.DataFrame,
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
    responses : pd.Dataframe
        Input responses DataFrame which has unflagged 290 special cases.
    contributors : pd.Dataframe
        Input contributors dataframe
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

    df = responses.merge(contributors, how="left", on=["period", "reference"]).copy()
    df = df[df["status"].isin(["Clear - overridden", "Clear"])]

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
        how="left",
    )

    # Create index of pairs of period and reference numbers which need to be
    # flagged as the special 290 case
    flagged_pairs = df_joined[
        (df_joined[f"{adjusted_response}_x"] != df_joined[f"{adjusted_response}_y"])
        & (df_joined[f"{adjusted_response}_y"].isin([0, np.nan]))
    ].index

    # Initialise flag
    responses["290_flag"] = False

    # Set flag based on index
    responses.loc[
        pd.MultiIndex.from_frame(responses[[period, reference]]).isin(flagged_pairs),
        ["290_flag"],
    ] = True

    # Return modified DataFrame
    return responses
