import pandas as pd
from mbs_results.staging.back_data import append_back_data
from mbs_results.staging.data_cleaning import (
    convert_annual_thousands,
    enforce_datatypes,
    filter_out_questions,
    run_live_or_frozen,
)
from mbs_results.staging.dfs_from_spp import get_dfs_from_spp
from mbs_results.staging.stage_dataframe import read_and_combine_colon_sep_files
from mbs_results.utilities.utils import get_snapshot_alternate_path

from cons_results.staging.create_missing_questions import create_missing_questions


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

    period = config["period"]
    reference = config["reference"]

    snapshot_file_path = get_snapshot_alternate_path(config)

    contributors, responses = get_dfs_from_spp(
        snapshot_file_path + config["construction_file_name"],
        config["platform"],
        config["bucket"],
    )

    # Filter columns and set data types
    contributors = contributors[config["contributors_keep_cols"]]
    contributors = enforce_datatypes(
        contributors, keep_columns=config["contributors_keep_cols"], **config
    )

    responses = responses[config["responses_keep_cols"]]
    responses = enforce_datatypes(
        responses, keep_columns=config["responses_keep_cols"], **config
    )

    finalsel = read_and_combine_colon_sep_files(config)

    # keep columns is applied in data reading from source, enforcing dtypes
    # in all columns of finalsel
    finalsel = enforce_datatypes(finalsel, keep_columns=list(finalsel), **config)

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
        all_questions=config["all_questions"],
        reference=config["reference"],
        period=config["period"],
        question_col=config["question_no"],
    )

    df = pd.merge(left=df, right=contributors, on=[period, reference], how="left")

    df = append_back_data(df, config)

    snapshot_name = config["construction_file_name"].split(".")[0]

    df = filter_out_questions(
        df=df,
        column=config["question_no"],
        questions_to_filter=config["filter_out_questions"],
        save_full_path=config["output_path"]
        + snapshot_name
        + "_filter_out_questions.csv",
        **config,
    )

    df = run_live_or_frozen(
        df,
        config["target"],
        status=config["status"],
        state=config["state"],
        error_values=[201],
    )

    df[config["auxiliary_converted"]] = df[config["auxiliary"]].copy()
    df = convert_annual_thousands(df, config["auxiliary_converted"])

    df = flag_290_case(
        df,
        period=config["period"],
        reference=config["reference"],
        question_no=config["question_no"],
        adjusted_response=config["target"],
    )

    print("Staging Completed")
    return df


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
