from mbs_results.utilities.copy_script_and_config import copy_script_and_config


def setup() -> None:
    """Console entrypoint for the cons_results package.

    This wrapper exists so the package's entry point can be a single importable callable
    (required by setuptools' console_scripts). It delegates to the shared mbs_results
    utility which copies the package's main.py and configs/config_user.json into the
    current working directory.

    Usage (after installing cons_results package):
        $ setup_cons

    The wrapper intentionally keeps a tiny surface so callers can pass through optional
    behaviour by editing this file if needed.
    """

    # copy cons_results/main.py and cons_results/configs/config_user.json
    copy_script_and_config(package_name="cons_results")
