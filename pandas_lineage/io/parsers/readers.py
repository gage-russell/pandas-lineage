"""
Module containing openlineage extension to pandas.read_csv

```
import pandas
import inspect

inspect.getmodule(pandas.read_csv)
```

***formated with Black***
"""
from typing import Optional, Union
from uuid import uuid4

from openlineage.client import OpenLineageClient
from pandas_lineage.types import JobRun, PandasDataSet
from pandas import read_csv as pandas_read_csv
from pandas._typing import FilePath, ReadCsvBuffer


def read_csv(
    filepath_or_buffer: Union[FilePath, ReadCsvBuffer[bytes], ReadCsvBuffer[str]],
    job_run: Optional[JobRun] = None,
    dataset_name=None,
    *args,
    **kwargs
):
    """
    TODO
    """
    # Config file is located by:
    # looking at OPENLINEAGE_CONFIG environment variable
    # looking for file openlineage.yml at current working directory
    # looking for file openlineage.yml at $HOME/.openlineage directory
    if not job_run:
        job_run = JobRun(run_id=uuid4().hex, namespace="pandas", name="empty")

    if not dataset_name:
        if isinstance(filepath_or_buffer, str):
            dataset_name = filepath_or_buffer

    dataframe = pandas_read_csv(filepath_or_buffer, *args, **kwargs)
    openlineage_dataset = PandasDataSet.from_pandas(
        dataframe=dataframe, dataset_name=dataset_name, job_run=job_run
    )
    openlineage_dataset.emit_input()
    return dataframe
