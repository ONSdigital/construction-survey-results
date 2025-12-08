import logging

import pandas as pd
from mbs_results.outputs.scottish_welsh_gov_outputs import read_and_combine_ludets_files

logger = logging.getLogger(__name__)


def filter_region(
    local_unit_data: pd.DataFrame, region: str, region_code: str
) -> pd.DataFrame:

    percent_col = f"percentage_{region}"

    # compute the grossed UK turnover or returns

    # Calculate froempment ratio:
    # (sum of froempment in filtered LU data) / (froempment in df)
    # Filter LU data for the devolved nation region
    region_col = "region"
    employment_col = "employment"

    # Filter LU data for the devolved nation region and sum employment by reference
    local_unit_data["reference"] = local_unit_data["ruref"]
    regional_employment = (
        local_unit_data[local_unit_data[region_col].isin(region_code)]
        .groupby(["reference", "period"])[employment_col]
        .sum()
        .reset_index()
        .rename(columns={employment_col: f"{employment_col}_{region}"})
    )

    # Sum total employment by reference from the pipeline data
    total_employment = (
        local_unit_data.groupby(["reference", "period"])["employment"]
        .sum()
        .reset_index()
        .rename(columns={"employment": "total_employment"})
    )

    # Merge the two Dataframes and calculate the percentage
    merged_df = pd.merge(
        regional_employment,
        total_employment,
        on=["reference", "period"],
        how="left",
    )

    merged_df[percent_col] = (
        merged_df[f"{employment_col}_{region}"] / merged_df["total_employment"]
    )

    return merged_df


def calculate_regional_percent(
    df: pd.DataFrame, merged_df: pd.DataFrame, region: str, region_code: str
):
    df = df.merge(
        merged_df[["reference", "period", f"percentage_{region}"]],
        on=["reference", "period"],
        how="left",
    )

    # Set percentage to 100% where region code matches but no data in ludets
    df.loc[
        df[f"percentage_{region}"].isnull() & (df["region"].isin(region_code)),
        f"percentage_{region}",
    ] = 1

    df[f"turnover_{region}"] = df[f"percentage_{region}"] * df["gross_turnover_uk"]

    return df


def reformat_r_m_output(df: pd.DataFrame, **config):
    """
    Function to reformat repair and maintenance output. Turnover is summed for each
    region-question-period combination and arranged in a df with a column for each
    question.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with regional proportions calculated for each r+m question
    **config : dict
        main pipeline config

    Returns
    -------
    pd.DataFrame
        Reformatted output

    """

    df["quarter"] = pd.PeriodIndex(
        pd.to_datetime(df["period"], format="%Y%m"), freq="Q"
    )
    if config["r_m_quarter"] is None:
        chosen_quarter = df["quarter"].max()
    else:
        chosen_quarter = pd.Period(config["r_m_quarter"])
    df = df[df["quarter"] == chosen_quarter]

    df[["quarter", "questioncode"]] = df[["quarter", "questioncode"]].astype(str)
    df = (
        df.drop(columns=["reference", "period"])
        .groupby(["quarter", "questioncode"])
        .sum()
        .reset_index()
    )

    df = df.melt(
        id_vars=["quarter", "questioncode"],
        value_vars=[col for col in df.columns if col.startswith("turnover")],
        var_name="region",
        value_name="turnover",
    )
    df["region"] = df["region"].str.replace("turnover_", "")

    df = (
        df.pivot_table(
            index=["quarter", "region"], columns="questioncode", values="turnover"
        )
        .sort_values(
            by=["region"],
            key=lambda x: x.map(config["r_m_region_order"]),
        )
        .reset_index()
    )

    df = df.fillna(0).rename(columns=config["question_no_plaintext"])

    return df.rename_axis(None, axis=1), str(chosen_quarter)


def produce_r_m_output(additional_outputs_df: pd.DataFrame, **config):
    """
    Function to produce regional repair and maintenance output from standard
    cons output. Repair and maintenance refers to questions 202, 212, 222, 232 and 243

    Parameters
    ----------
    additional_outputs_df : pd.DataFrame
        Output of construction data containing all necessary information
    **config : dict
        main pipeline config

    Returns
    -------
    pd.DataFrame
        Completed R+M output, with each question summarised by region and quarterly
        period.

    """

    df = additional_outputs_df[
        [
            "reference",
            "period",
            "questioncode",
            "adjustedresponse",
            "region",
            "design_weight",
            "calibration_factor",
            "outlier_weight",
        ]
    ]
    df = df[df["questioncode"].isin(config["r_m_questions"])]

    # TODO: Replace with dedicated cons functions
    ludets = read_and_combine_ludets_files(config)

    region_to_code = {
        "Scotland": ["XX"],
        "Wales": ["WW"],
        "North East": ["AA"],
        "North West": ["BB", "BA"],
        "Yorkshire and The Humber": ["DC"],
        "East Midlands": ["ED"],
        "West Midlands": ["FE"],
        "East of England": ["GF", "GG"],
        "London": ["HH"],
        "South East": ["JG"],
        "South West": ["KJ"],
    }

    df["gross_turnover_uk"] = (
        df["adjustedresponse"]
        * df["design_weight"]
        * df["outlier_weight"]
        * df["calibration_factor"]
        / 1000
    )

    for region in region_to_code.keys():
        region_code = region_to_code[region]
        logger.info(f"Filtering for {region} with region code {region_code}")
        merged_df = filter_region(ludets, region, region_code)
        df = calculate_regional_percent(df, merged_df, region, region_code)

    df, quarter = reformat_r_m_output(df, **config)

    filename = f"r_and_m_regional_extracts_{quarter}_{config['run_id']}.csv"

    return (df, filename)
