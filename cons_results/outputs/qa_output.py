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

    index_columns = [
        config["period"],
        config["sic"],
        config["reference"],
        config["cell_number"],
        config["auxiliary"],  # check if aux or converted aux
        config["froempment"],
        "runame1",
    ]

    # Create value for adj_targer*a*o*g weights
    additional_outputs_df["weighted adjusted value"] = (
        additional_outputs_df[config["target"]]
        * additional_outputs_df["design_weight"]
        * additional_outputs_df["outlier_weight"]
        * additional_outputs_df["calibration_factor"]
    )

    # selecting 4 value columns
    value_columns = [
        config["target"],
        config["imputation_marker_col"],
        "outlier_weight",
        "weighted adjusted value",  #
    ]

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

    #convert question_no column names to strings
    main_pivot.columns = pd.MultiIndex.from_tuples(
        [(str(l0), l1) for l0, l1 in main_pivot.columns]
    )

    period_dict = {
        period: df.drop(columns=[config["period"]], errors="ignore")
        for period, df in main_pivot.groupby(config["period"], dropna=False)
    }

    return period_dict
