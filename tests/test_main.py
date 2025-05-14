from pathlib import Path

import pytest

from cons_results.main import run_pipeline


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/test_main/")


@pytest.fixture(scope="class")
def test_config(filepath):
    return {
        "platform": "network",
        "bucket": "",
        "calibration_group_map_path": "",
        "classification_values_path": "",
        "folder_path": filepath,
        "l_values_path": "",
        "manual_outlier_path": "",
        "construction_file_name": "dummy_snapshot.json",
        "output_path": "",
        "population_prefix": "",
        "sample_prefix": "finalsel228",
        "back_data_qv_path": str(filepath / "test_qv_009_202202.csv"),
        "back_data_cp_path": str(filepath / "test_cp_009_202202.csv"),
        "back_data_finalsel_path": str(filepath / "finalsel228_dummy_202202"),
        "sic_domain_mapping_path": "",
        "current_period": 202303,
        "revision_window": 13,
        "state": "frozen",
    }


class TestMain:
    """Test for main"""

    def test_run_pipeline(self, test_config):
        """Run main pipeline based on test_config"""
        run_pipeline(test_config)
