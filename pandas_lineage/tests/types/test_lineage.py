"""
unit tests for pandas_lineage.types.lineage module
"""
from uuid import uuid4

import pytest
from openlineage.client.run import RunEvent

from pandas_lineage.custom_types import lineage


class MockOpenLineageClient:
    @staticmethod
    def emit(*args, **kwargs):
        return dict(args=args, kwargs=kwargs)


@pytest.fixture
def mock_job_run():
    job_run = lineage.JobRun(run_id=uuid4().hex, namespace="test-namespace", name="test-name")
    job_run.client = MockOpenLineageClient()
    return job_run


# TODO: turn into test classes to mirror module classes
def test_JobRun_emit_start(mock_job_run):
    start = mock_job_run.emit_start()
    assert isinstance(start["args"][0], RunEvent)
    assert start["args"][0].job.name == "test-name"
