import numpy as np
import pandas as pd


def get_quarterly_by_sizeband_output(
    additional_outputs_df: pd.DataFrame, **config: dict[str, any]
) -> pd.DataFrame:
    """
    Generates a quarterly summary of data grouped by sizeband and question number.

    Parameters
    ----------
    additional_outputs_df : pd.DataFrame
        Input DataFrame containing the data to be processed. Must include columns
        specified in the `config` dictionary.
    config : dict[str, str]
        Configuration dictionary with the following keys:
            - "period" : str
                Column name in `additional_outputs_df` representing the period to
                create quarter.
            - "question_no" : str
                Column name in `additional_outputs_df` representing the question number.
            - "target" : str
                Column name in `additional_outputs_df` representing the values to
                aggregate.
            - "cell_number" : str
                Column name in `additional_outputs_df` representing the cell number to
                create sizeband grouping.

    Returns
    -------
    pd.DataFrame
        A DataFrame with quarterly data aggregated by sizeband and question number.
        The output contains the following columns:
            - "quarter" : str
                Quarterly period in "YYYYQX" format.
            - "sizeband" : int
                Sizeband grouping.
            - Columns corresponding to question numbers, with aggregated values for
            each.
    """
    filtered_data = additional_outputs_df.copy()[
        [
            config["period"],
            config["cell_number"],
            config["question_no"],
            config["target"],
        ]
    ]

    # selecting only components + q290 (so not to include filtered qs with no cell no)
    filtered_data = filtered_data[
        filtered_data[config["question_no"]].isin(
            config["components_questions"] + [290]
        )
    ]

    filtered_data["sizeband"] = np.where(
        filtered_data[config["cell_number"]].isna(),
        filtered_data[config["cell_number"]],
        filtered_data[config["cell_number"]].astype(str).str[-1],
    ).astype(int)

    filtered_data.drop(columns=[config["cell_number"]], inplace=True)

    filtered_data["quarter"] = (
        pd.to_datetime(filtered_data[config["period"]], format="%Y%m")
        .dt.to_period("Q")
        .astype(str)
    )

    filtered_data.sort_values(
        ["quarter", "sizeband", config["question_no"]],
        inplace=True,
    )

    quarterly_by_sizeband_output = (
        filtered_data.pivot_table(
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
