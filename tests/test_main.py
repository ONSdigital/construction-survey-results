from pathlib import Path

import pytest

from cons_results.main import run_pipeline


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/test_main/")


# Either pass test_config as run pipeline arg, or save the test_config in
# wdir (working directory)
@pytest.fixture(scope="class")
def test_config(filepath):
    return {}


# Comment out the skip if testing can be done, otherwise please
# add a reason why the testing should be skipped
@pytest.mark.skip(reason="Nothing to test, no moodules in main")
class TestMain:
    """Test for main"""

    def test_run_pipeline(self, test_config):
        """Run main pipeline based on test_condig"""
        run_pipeline()
