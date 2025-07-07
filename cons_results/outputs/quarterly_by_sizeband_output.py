import pandas as pd


def get_quarterly_by_sizeband_output(
    additional_outputs_df: pd.DataFrame, config: dict[str, str]
) -> pd.DataFrame:
    """
    Generates a quarterly summary of data grouped by sizeband and question number.

    Parameters
    ----------
    additional_outputs_df : pd.DataFrame
        Input DataFrame containing the data to be processed. Must include columns
        specified in the config.
    config : dict[str, str]
        Configuration dictionary with the following keys:
        - "period" : str
            Column name in `additional_outputs_df` representing the period.
        - "question_no" : str
            Column name in `additional_outputs_df` representing the question number.
        - "target" : str
            Column name in `additional_outputs_df` representing the values to aggregate.

    Returns
    -------
    pd.DataFrame
        A DataFrame with quarterly data aggregated by sizeband and question number.
        The output contains the following columns:
        - "quarter" : str
            Quarterly period in "YYYYQX" format.
        - "sizeband" : int
            Sizeband grouping.
        - Columns corresponding to question numbers, with aggregated values for each.
    """
    additional_outputs_df = additional_outputs_df[
        [config["period"], "sizeband", config["question_no"], config["target"]]
    ]

    additional_outputs_df["quarter"] = (
        pd.to_datetime(additional_outputs_df[config["period"]], format="%Y%m")
        .dt.to_period("Q")
        .astype(str)
    )

    quarterly_by_sizeband_output = (
        additional_outputs_df.pivot_table(
            index=["quarter", "sizeband"],
            columns=config["question_no"],
            values=config["target"],
            aggfunc="sum",
            dropna=False,
        )
        .reset_index()
        .fillna(0)
        .rename_axis(None, axis=1)
    )

    quarterly_by_sizeband_output.columns = quarterly_by_sizeband_output.columns.astype(
        str
    )

    return quarterly_by_sizeband_output
