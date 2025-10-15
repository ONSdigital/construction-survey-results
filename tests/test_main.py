from glob import glob
from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from cons_results.main import run_pipeline


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/test_main/input/")


@pytest.fixture(scope="class")
def test_config(filepath):
    return {
        "platform": "network",
        "bucket": "",
        "calibration_group_map_path": "",
        "classification_values_path": filepath / "sic_classification_mapping.csv",
        "idbr_folder_path": filepath,
        "l_values_path": filepath / "classification_questioncode_l_value_mapping.csv",
        "manual_outlier_path": "",
        "snapshot_file_path": str(filepath / "dummy_snapshot.json"),
        "manual_constructions_path": filepath / "manual_constructions.csv",
        "filter": None,
        "output_path": "tests/data/test_main/output/",
        "population_prefix": "test_universe009",
        "sample_prefix": "finalsel228",
        "back_data_qv_path": filepath / "test_qv_009_202212.csv",
        "back_data_cp_path": filepath / "test_cp_009_202212.csv",
        "back_data_finalsel_path": str(filepath / "finalsel228_dummy_202212"),
        "current_period": 202303,
        "revision_window": 3,
        "debug_mode": True,
    }


class TestMain:
    """Test for main"""

    def test_run_pipeline(self, test_config):
        """Run main pipeline based on test_config"""
        run_pipeline(test_config)

        out_path = "tests/data/test_main/output/"

        # check pattern due different version in file name
        patern = glob(out_path + "cons_results_*.csv")

        actual = pd.read_csv(patern[0])
        expected = pd.read_csv(out_path + "expected_from_cons_main.csv")

        assert_frame_equal(actual, expected, check_like=False)

    def test_run_pipeline_live(self, test_config):
        """Run main pipeline based on test_config"""

        test_config["state"] = "live"

        run_pipeline(test_config)
