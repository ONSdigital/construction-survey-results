import logging

import pandas as pd
from mbs_results.outputs.scottish_welsh_gov_outputs import read_and_combine_ludets_files

logger = logging.getLogger(__name__)


def calculate_regional_employment(
    local_unit_data: pd.DataFrame,
    region: str,
    region_code: str,
    employment_col: str = "employment",
    region_col: str = "region",
) -> pd.DataFrame:
    """
    Using local unit data to calculate employment for a given region

    Parameters
    ----------
    local_unit_data : pd.DataFrame
        local unit data (ludets) read in first part of produce_r_m_output wrapper.
    region : str
        name of region to filter by (i.e. "Scotland", "South East")
    region_code : str
        Code of region in local unit data, to apply filter.
    employment_col : str, optional
        Name of column with employment data in ludets. The default is "employment".
    region_col : str, optional
        Name of column with region code in ludets. The default is "region".

    Returns
    -------
    regional_employment : pd.DataFrame
        Regional employment figures for given region across all references and
        periods.

    """

    regional_employment = (
        local_unit_data[local_unit_data[region_col].isin(region_code)]
        .groupby(["reference", "period"])[employment_col]
        .sum()
        .reset_index()
        .rename(columns={employment_col: f"{employment_col}_{region}"})
    )

    return regional_employment


def calculate_total_employment(local_unit_data: pd.DataFrame, employment_col: str = "employment"):
    """
    Calculate total employment across all periods in local unit data

    Parameters
    ----------
    local_unit_data : pd.DataFrame
        local unit data (ludets) read in first part of produce_r_m_output wrapper.
    employment_col : str, optional
        Name of column with employment data in ludets. The default is "employment".

    Returns
    -------
    total_employment : pd.DataFrame
        Total employment figures by reference and period.

    """

    total_employment = (
        local_unit_data.groupby(["reference", "period"])[employment_col]
        .sum()
        .reset_index()
        .rename(columns={employment_col: "total_employment"})
    )

    return total_employment


def calculate_regional_percent(
    regional_employment: pd.DataFrame,
    total_employment: pd.DataFrame,
    region: str,
    employment_col: str = "employment",
):
    """
    Calculate percent of employees in given region for each reference.

    Parameters
    ----------
    regional_employment : pd.DataFrame
        Regional employment figures for given region, calculated in
        `calculate_regional_employment`.
    total_employment : pd.DataFrame
        Total employment figures, calculated in `calculate_total_employment`.
    region : str
        Name of region.
    employment_col : str, optional
        Name of column with employment data in ludets. The default is "employment".

    Returns
    -------
    merged_df : pd.DataFrame
        Final output with percentage of employment in given region, calculated
        from regional and total employment figures.

    """

    percent_col = f"percentage_{region}"

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


def handle_rus_not_in_ludets(df: pd.DataFrame, region: str, region_code: str):
    """
    Helper function called within `calculate_regional_turnover`. If any reference
    is assigned to one region in df, but has no data in ludets, assign a percentage
    of 100% to that region.

    """

    df.loc[
        df[f"percentage_{region}"].isnull() & (df["region"].isin(region_code)),
        f"percentage_{region}",
    ] = 1

    return df


def calculate_regional_turnover(
    df: pd.DataFrame, merged_df: pd.DataFrame, region: str, region_code: str
):
    """
    Calculating regional proportions of turnover from regional percent and overall
    turnover figures. Uses result of `calculate_regional_percent`

    Parameters
    ----------
    df : pd.DataFrame
        Cons results df with turnover figure
    merged_df : pd.DataFrame
        Output from `calculate_regional_percent`
    region : str
        Name of region.
    region_code : str
        Code of region.

    Returns
    -------
    df : pd.DataFrame
        DataFrame output containing regional proportion of turnover figures.

    """

    df = df.merge(
        merged_df[["reference", "period", f"percentage_{region}"]],
        on=["reference", "period"],
        how="left",
    )

    # Set percentage to 100% where region code matches but no data in ludets
    df = handle_rus_not_in_ludets(df, region, region_code)

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

    # Set to 'is not True' to capture any misspellings etc. of False.
    if config["produce_r_m_output"] is not True:
        logger.info(
            "Skipping R+M output as config option is not set to True."
            + "\nIf you want to produce the R+M output, please set 'produce_r_m_output'"
            + " in the output config to True."
        )
        return None

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
    ludets["reference"] = ludets["ruref"]
    total_employment = calculate_total_employment(ludets)

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
        regional_employment = calculate_regional_employment(ludets, region, region_code)
        merged_df = calculate_regional_percent(
            regional_employment, total_employment, region
        )
        df = calculate_regional_turnover(df, merged_df, region, region_code)

    df, quarter = reformat_r_m_output(df, **config)

    filename = f"r_and_m_regional_extracts_{quarter}_{config['run_id']}.csv"

    return (df, filename)
