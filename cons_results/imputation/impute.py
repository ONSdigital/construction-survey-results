import pandas as pd
from mbs_results.imputation.ratio_of_means import ratio_of_means
from mbs_results.staging.data_cleaning import convert_annual_thousands

from cons_results.imputation.post_imputation import (
    create_q290,
    derive_q290,
    rescale_290_case,
    validate_q290,
)
from cons_results.staging.create_skipped_questions import create_skipped_questions


def impute(
    df: pd.DataFrame,
    config: dict,
    manual_constructions: pd.DataFrame = None,
    filter_df: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    wrapper function to apply imputation to the given df

    Parameters
    ----------
    df : pd.DataFrame
        df with both contributors and responses from snapshot
    manual_constructions : pd.DataFrame
        manual_constructions : pd.DataFrame
    config : dict
        config file containing column names and manual construction path
    filter_df : pd.DataFrame
        filter_df df from the staging module


    Returns
    -------
    pd.DataFrame
        post imputation df, values have been derived and constrained following
        imputation
    """

    df = df.groupby(config["question_no"])[df.columns].apply(
        lambda df_q_code: ratio_of_means(
            df=df_q_code,
            manual_constructions=manual_constructions,
            reference=config["reference"],
            target=config["target"],
            period=config["period"],
            current_period=config["current_period"],
            revision_window=config["revision_window"],
            question_no=config["question_no"],
            strata="imputation_class",
            auxiliary=config["auxiliary_converted"],
            filters=filter_df,
        )
    )

    df = df.reset_index(drop=True)  # remove groupby leftovers

    df = df[~df["is_backdata"]]  # remove backdata
    df.drop(columns=["is_backdata"], inplace=True)

    df = create_skipped_questions(
        df=df,
        all_questions=config["components_questions"],
        reference=config["reference"],
        period=config["period"],
        question_col=config["question_no"],
        target_col=config["target"],
        contributors_keep_col=config["contributors_keep_cols"],
        responses_keep_col=config["responses_keep_cols"],
        finalsel_keep_col=config["finalsel_keep_cols"],
        status_col=config["nil_status_col"],
        status_filter=["Check needed"] + config["non_response_statuses"],
        flag_col_name="derived_zeros",
        imputation_marker_col=config["imputation_marker_col"],
    )

    # derived zeros types is object, has true false and na
    df.loc[df["derived_zeros"] == 1, config["imputation_marker_col"]] = "d"

    # Updating derived questions with converted auxiliary values
    null_rows = df[df[config["auxiliary_converted"]].isna()]

    updated_rows = convert_annual_thousands(
        null_rows, config["auxiliary_converted"], config["auxiliary"]
    )

    updated_rows.set_index(["reference", "period", "questioncode"], inplace=True)

    df.set_index(["reference", "period", "questioncode"], inplace=True)

    df.loc[updated_rows.index, config["auxiliary_converted"]] = updated_rows[
        config["auxiliary_converted"]
    ]

    df.reset_index(inplace=True)

    df = rescale_290_case(
        df,
        config["period"],
        config["reference"],
        config["question_no"],
        config["target"],
    )

    df = create_q290(
        df,
        config,
        config["reference"],
        config["period"],
        config["question_no"],
        config["target"],
        config["imputation_marker_col"],
    )

    df = derive_q290(
        df,
        config["question_no"],
        config["imputation_marker_col"],
        config["period"],
        config["reference"],
        config["target"],
    )

    validate_q290(
        df=df,
        question_no=config["question_no"],
        period=config["period"],
        reference=config["reference"],
        adjustedresponse=config["target"],
        config=config,
        output_path=config["output_path"],
        output_file_name="validate_q290_output.csv",
        import_platform=config["platform"],
        bucket_name=config["bucket"],
    )

    df[config["period"]] = df[config["period"]].dt.strftime("%Y%m").astype("int")

    return df
