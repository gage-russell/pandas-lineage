from pathlib import Path
from os.path import dirname, abspath
import pytest

from uuid import uuid4

import pandas

from pandas_lineage import read_csv
from pandas_lineage.custom_types.lineage import JobRun
from pandas_lineage.custom_types import lineage



DATA_PATH = Path(dirname(dirname(dirname(abspath(__file__))))) / Path('data')


class MockOpenLineageClient:
    @staticmethod
    def emit(*args, **kwargs):
        return dict(args=args, kwargs=kwargs)


@pytest.fixture
def mock_job_run():
    job_run = lineage.JobRun(run_id=uuid4().hex, namespace="test-namespace", name="test-name")
    job_run.client = MockOpenLineageClient()
    return job_run


@pytest.mark.parametrize('path, expected', [('abc123_dataframe.csv', True), ('null_abc123_dataframe', True)])
def test_read_csv_abc123_data(path, expected, caplog):
    print(caplog.text)
    _path = DATA_PATH / Path(path)
    test_df = read_csv(_path)
    expected_df = pandas.read_csv(_path)
    assert test_df.equals(expected_df)