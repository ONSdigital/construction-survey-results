import logging
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


def generate_run_id() -> str:
    """
    Generate a unique run identifier based on the current UTC timestamp and a
    random UUID segment.

    Returns
    -------
    str
        A unique run identifier in the format YYYYMMDDTHHMMSSZ_XXXXXXXX
    """
    from datetime import datetime
    from uuid import uuid4

    return f"{datetime.utcnow():%Y%m%dT%H%M%SZ}_{uuid4().hex[:8]}"


# Custom S3 Handler for logging S3 (since S3 doesn't support direct append, this
# buffers and periodically flushes to S3)
class S3LoggingHandler(logging.Handler):
    def __init__(self, s3_bucket, s3_key, s3_client, capacity=100):
        logging.Handler.__init__(self)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_client = s3_client
        self.buffer = []
        self.capacity = capacity

    def flush(self):
        if not self.buffer:
            return

        log_data = "\n".join(self.buffer) + "\n"
        try:
            # Get existing log data from S3
            existing_obj = self.s3_client.get_object(
                Bucket=self.s3_bucket, Key=self.s3_key
            )
            existing_data = existing_obj["Body"].read().decode("utf-8")
        except self.s3_client.exceptions.NoSuchKey:
            existing_data = ""

        # Combine existing data with new log data
        combined_data = existing_data + log_data

        # Upload combined data back to S3
        self.s3_client.put_object(
            Bucket=self.s3_bucket, Key=self.s3_key, Body=combined_data.encode("utf-8")
        )

        # Clear the buffer
        self.buffer = []

    def emit(self, record):
        log_entry = self.format(record) + "\n"
        self.buffer.append(log_entry)
        if len(self.buffer) >= self.capacity:
            self.flush()

    def close(self):
        self.flush()
        super


def configure_s3_client(config):
    """Configure and return an S3 client with RAZ authentication."""
    import boto3
    import raz_client

    s3_client = boto3.client("s3")
    ssl_file = config.get("ssl_file", "/etc/pki/tls/certs/ca-bundle.crt")
    raz_client.configure_ranger_raz(s3_client, ssl_file=ssl_file)

    return s3_client
