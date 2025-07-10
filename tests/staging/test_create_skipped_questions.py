from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from cons_results.staging.create_skipped_questions import create_skipped_questions


@pytest.fixture()
def filepath():
    return Path("tests/data/staging/create_skipped_questions")


def test_create_skipped_questions(filepath):
    df_input = pd.read_csv(filepath / "create_skipped_questions_input.csv")
    df_expected_output = pd.read_csv(filepath / "create_skipped_questions_output.csv")
    actual_output = create_skipped_questions(df_input, config={})

    assert_frame_equal(actual_output, df_expected_output)
