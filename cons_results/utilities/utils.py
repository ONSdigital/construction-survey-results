import os
from importlib import metadata


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
