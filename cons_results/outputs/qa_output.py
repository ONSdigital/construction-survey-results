import pandas as pd


def get_qa_output(df: pd.DataFrame, config: dict) -> pd.DataFrame:
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
        "runame1"
        # potentially add entname1?
    ]

    # Create value for adj_targer*a*o*g weights
    df["curr_grossed_value"] = (
        df[config["target"]]
        * df["design_weight"]
        * df["outlier_weight"]
        * df["calibration_factor"]
    )

    # selecting 4 value columns
    value_columns = [
        config["target"],
        config["imputation_marker_col"],
        "outlier_weight",
        "curr_grossed_value",  #
    ]

    # creating pivot table
    qa_output_df = df.pivot_table(
        index=index_columns,
        columns=config["question_no"],
        values=value_columns,
        aggfunc="first",
    )

    # swapping index levels to have question_no as the top level as requested in task
    return qa_output_df.swaplevel(axis=1).sort_index(axis=1, level=0)
