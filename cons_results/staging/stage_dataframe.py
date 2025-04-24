import pandas as pd
from mbs_results.staging.data_cleaning import enforce_datatypes
from mbs_results.staging.dfs_from_spp import get_dfs_from_spp
from mbs_results.staging.stage_dataframe import read_and_combine_colon_sep_files
from mbs_results.utilities.utils import get_snapshot_alternate_path


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

    return contributors
