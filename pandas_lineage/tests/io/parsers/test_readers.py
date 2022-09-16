from os.path import abspath, dirname
from pathlib import Path
from uuid import uuid4

import pandas
import pytest

from pandas_lineage import read_csv
from pandas_lineage.custom_types import lineage
from pandas_lineage.custom_types.lineage import JobRun

DATA_PATH = Path(dirname(dirname(dirname(abspath(__file__))))) / Path("data")


class MockOpenLineageClient:
    @staticmethod
    def emit(*args, **kwargs):
        return dict(args=args, kwargs=kwargs)


@pytest.fixture
def mock_job_run():
    job_run = lineage.JobRun(run_id=uuid4().hex, namespace="test-namespace", name="test-name")
    job_run.client = MockOpenLineageClient()
    return job_run


@pytest.mark.parametrize("path", [("abc123_dataframe.csv"), ("null_abc123_dataframe.csv")])
def test_read_csv_abc123_data_no_job_run(path, caplog):
    _path = DATA_PATH / Path(path)
    test_df = read_csv(_path)
    expected_df = pandas.read_csv(_path)
    assert test_df.equals(expected_df)
    assert "WARNING: job_run argument must be supplied to emit lineage events" in caplog.text


@pytest.mark.parametrize("path", [("abc123_dataframe.csv"), ("null_abc123_dataframe.csv")])
def test_read_csv_abc123_data(path, caplog, mock_job_run):
    _path = DATA_PATH / Path(path)
    test_df = read_csv(_path, job_run=mock_job_run)
    expected_df = pandas.read_csv(_path)
    assert test_df.equals(expected_df)
