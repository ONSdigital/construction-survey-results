from pathlib import Path

import pytest

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
        "sic_domain_mapping_path": "",
        "current_period": 202303,
        "revision_window": 3,
        "state": "frozen",
        "optional_outputs": [],  # "all" is broken
    }


class TestMain:
    """Test for main"""

    def test_run_pipeline(self, test_config):
        """Run main pipeline based on test_config"""
        run_pipeline(test_config)
