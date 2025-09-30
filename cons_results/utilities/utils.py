import os
from importlib import metadata

import pandas as pd
from mbs_results.utilities.outputs import write_csv_wrapper


# Temp function - need to generalise the MBS get_versioned_filename
def get_versioned_filename(prefix, config):

    file_version = metadata.metadata("construction-survey-results")["version"]
    snapshot_name = os.path.basename(config["snapshot_file_path"].split(".")[0])

    # temp workaround whilst cons isn't on Artifactory
    try:
        file_version = metadata.version("construction-survey-results")
    except metadata.PackageNotFoundError:
        file_version = "v0.0.0"

    filename = f"{prefix}_v{file_version}_{snapshot_name}.csv"

    return filename


def save_df(df: pd.DataFrame, base_filename: str, config: dict, on_demand=True):
    """
    Adds a version tag to the filename and saves the dataframe based on
    settings in the config.

    Parameters
    ----------
    df: pd.DataFrame
        The dataframe to write to the specified path.
    base_filename : str
        The base text for the filename.
    config : str, optional
        The pipeline configuration
    on_demand: bool
        Wether to foce the save, default is True.

    Returns
    -------
    None
    """

    # export on demand
    if on_demand:

        filename = get_versioned_filename(base_filename, config)

        write_csv_wrapper(
            df,
            config["output_path"] + filename,
            config["platform"],
            config["bucket"],
            index=False,
        )
