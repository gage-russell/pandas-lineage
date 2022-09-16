from os import environ
from os.path import abspath, dirname
from pathlib import Path
from unittest import mock
from uuid import uuid4

import pandas
import pytest

from pandas_lineage import read_csv, read_parquet
from pandas_lineage.custom_types import lineage

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
@mock.patch.dict(environ, {"OPENLINEAGE_URL": "https://not-real.com"})
def test_read_csv_abc123_data_invalid_http(path, caplog, mock_job_run):
    _path = DATA_PATH / Path(path)
    job_run = lineage.JobRun(run_id=uuid4().hex, namespace="test-namespace", name="test-name")
    test_df = read_csv(_path, job_run=job_run)
    expected_df = pandas.read_csv(_path)
    assert test_df.equals(expected_df)
    assert "Request Error: failed to emit lineage event" in caplog.text


@pytest.mark.parametrize("path", [("abc123_dataframe.csv"), ("null_abc123_dataframe.csv")])
def test_read_csv_abc123_data(path, mock_job_run):
    _path = DATA_PATH / Path(path)
    test_df = read_csv(_path, job_run=mock_job_run)
    expected_df = pandas.read_csv(_path)
    assert test_df.equals(expected_df)


@pytest.mark.parametrize("path", [("abc123_dataframe.snappy.parquet")])
def test_read_parquet_abc123_data(path, mock_job_run):
    _path = DATA_PATH / Path(path)
    test_df = read_parquet(_path, job_run=mock_job_run)
    expected_df = pandas.read_parquet(_path)
    assert test_df.equals(expected_df)
