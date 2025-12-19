from pathlib import Path

import pytest

from cons_results.produce_additional_outputs import produce_additional_outputs_wrapper


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/test_main/input/")


@pytest.fixture(scope="class")
def test_config(filepath):

    return {
        "platform": "network",
        "bucket": "",
        "idbr_folder_path": filepath,
        "snapshot_file_path": str(filepath / "dummy_snapshot.json"),
        "main_cons_output_folder_path": "tests/data/test_main/output/",
        "cons_output_prefix": "expected_from_cons_main",
        "cdid_data_path": "tests/data/outputs/csdb_output/cdid_mapping.csv",
        "output_path": "tests/data/test_main/output/",
        "current_period": 202206,
        "revision_window": 6,
        "region_mapping_path": filepath / "region_code_name_mapping.csv",
        "r_and_m_quarter": None,
        "sizeband_quarter": ["2023Q2"],
        "imputation_contribution_periods": [202302],
        "ludets_prefix": "dummy_ludets228_",
        "local_unit_columns": [
            "ruref",
            "entref",
            "lu ref",
            "check letter",
            "sic03",
            "sic07",
            "employees",
            "employment",
            "fte",
            "Name1",
            "Name2",
            "Name3",
            "Address1",
            "Address2",
            "Address3",
            "Address4",
            "Address5",
            "Postcode",
            "trading as 1",
            "trading as 2",
            "trading as 3",
            "region",
        ],
        "r_m_questions": [11, 12, 202, 212, 222, 232, 243],
        "question_no_plaintext": {
            11: "start_date",
            12: "end_date",
            202: "public_housing",
            212: "private_housing",
            222: "infrastructure",
            232: "public_non_housing",
            243: "private_non_housing",
        },
        "run_id": "1",
        "produce_r_m_output": True,
        "r_m_quarter": None,
        "r_m_region_order": {
            "North East": 1,
            "Yorkshire and The Humber": 2,
            "East Midlands": 3,
            "East of England": 4,
            "London": 5,
            "South East": 6,
            "South West": 7,
            "Wales": 8,
            "West Midlands": 9,
            "North West": 10,
            "Scotland": 11,
        },
    }


def test_produce_additional_outputs_wrapper(test_config):
    """Triggers a produce_additional_outputs_wrapper run, the output
    of mbs main is the input of this"""

    produce_additional_outputs_wrapper(config_user_dict=test_config)
