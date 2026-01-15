import pandas as pd


def produce_qa_output(
    additional_outputs_df: pd.DataFrame, **config: dict
) -> pd.DataFrame:
    """
    Creates QA output

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame.
    config : dict
        A dictionary containing configuration parameters. Must include:
        - "period" : str
            The column name for the period.
        - "sic" : str
            The column name containing sic.
        - "reference" : str
            The column name for the reference.
        - "question_no" : str
            The column name for the question number.
        - "target" : str
            The column name for the target data.
        - "cell_number" : str
            The column name for the cell number.
        - "auxiliary" : str
            The column name for the auxiliary variable.
        - "froempment" : str
            The column name for the froempment.
        - "imputation_marker_col" : str
            The column name for the imputation marker.

    Returns
    -------
    pd.DataFrame
        QA output dataframe with a MultiIndex on columns, where the first level is
        question_no and the second level contains value columns such as target,
        imputation_marker_col,
    """

    additional_outputs_df = replace_imputation_markers_total_only(
        additional_outputs_df,
        reference=config["reference"],
        period=config["period"],
        question_no=config["question_no"],
        imputation_marker_col=config["imputation_marker_col"],
        suffix="_c",
    )

    additional_outputs_df = change_derived_zeros_to_fir(
        additional_outputs_df,
        config["imputation_marker_col"],
    )

    index_columns = [
        config["period"],
        config["sic"],
        config["reference"],
        config["cell_number"],
        config["auxiliary"],  # check if aux or converted aux
        config["froempment"],
        "runame1",
    ]

    additional_outputs_df = additional_outputs_df[
        ~additional_outputs_df[config["question_no"]].isin(
            config["filter_out_questions"]
        )
    ]

    # Create value for adj_targer*a*o*g weights
    additional_outputs_df["weighted adjusted value"] = (
        additional_outputs_df[config["pound_thousand_col"]]
        * additional_outputs_df["design_weight"]
        * additional_outputs_df["outlier_weight"]
        * additional_outputs_df["calibration_factor"]
    )

    # selecting 4 value columns
    value_columns = [
        config["target"],
        config["imputation_marker_col"],
        "outlier_weight",
        "weighted adjusted value",
    ]

    additional_outputs_df = additional_outputs_df.loc[
        additional_outputs_df[config["question_no"]] != 290
    ].copy()

    # rename adjustedresponse_pounds_thousands to adjustedresponse
    # to match what's on the extract
    additional_outputs_df = additional_outputs_df.drop(config["target"], axis=1)
    additional_outputs_df = additional_outputs_df.rename(
        columns={config["pound_thousand_col"]: config["target"]}
    )

    # creating pivot table
    # Converting question no to string, this becomes a column name
    # and should be a string
    qa_output_df = additional_outputs_df.pivot_table(
        index=index_columns,
        columns=config["question_no"],
        values=value_columns,
        aggfunc="first",
    )

    main_pivot = (
        qa_output_df.swaplevel(axis=1).sort_index(axis=1, level=0).reset_index()
    )
    extra_information_columns = [
        config["period"],
        config["reference"],
        "design_weight",
        "calibration_factor",
        config["nil_status_col"],
    ]

    extra_information = additional_outputs_df[
        extra_information_columns
    ].drop_duplicates(subset=[config["period"], config["reference"]])
    extra_information.columns = pd.MultiIndex.from_tuples(
        [(col, "") for col in extra_information.columns]
    )
    main_pivot = pd.merge(
        main_pivot,
        extra_information.sort_index(axis=1),
        on=[config["period"], config["reference"]],
        how="left",
    )

    # convert question_no column names to strings
    main_pivot.columns = pd.MultiIndex.from_tuples(
        [(str(l0), l1) for l0, l1 in main_pivot.columns]
    )

    period_dict = {
        period: df.copy()
        for period, df in main_pivot.groupby(config["period"], dropna=False)
    }

    return period_dict


def replace_imputation_markers_total_only(
    df: pd.DataFrame, reference, period, question_no, imputation_marker_col, suffix="_c"
) -> pd.DataFrame:
    """
    Appends a suffix to imputation markers for component questions when they are created
    from a total_only record.
    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame.
    reference : str
        The column name for the reference.
    period : str
        The column name for the period.
    question_no : str
        The column name for the question numbers.
    imputation_marker_col : str
        The column name for the imputation markers.
    suffix : str, optional
        The suffix to append to eligible imputation markers. Default is "_c".

    Returns
    -------
    pd.DataFrame
        The DataFrame with updated imputation markers for eligible rows.
    """

    has_true_290_flag = (
        df["290_flag"].groupby([df[reference], df[period]]).transform("any")
    )

    imputation_markers_to_change = (df[question_no] != 290) & (
        ~df[imputation_marker_col].isin(["r", "c", "mc"])
    )

    mask = has_true_290_flag & imputation_markers_to_change

    df.loc[mask, imputation_marker_col] = df.loc[mask, imputation_marker_col] + suffix

    return df


def change_derived_zeros_to_fir(df, imputation_flag_col):
    df.loc[df["derived_zeros"] == True, imputation_flag_col] = "fir"  # noqa
    return df
