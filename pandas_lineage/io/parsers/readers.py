"""
Module containing openlineage extension to pandas.read_csv

```
import pandas
import inspect

inspect.getmodule(pandas.read_csv)
```

***formated with Black***
"""
from pathlib import Path
from typing import Optional, Union

from pandas import read_csv as pandas_read_csv
from pandas import read_parquet as pandas_read_parquet
from pandas._typing import FilePath, ReadCsvBuffer

from pandas_lineage.custom_types.lineage import JobRun
from pandas_lineage.custom_types.pandas import LineageDataFrame
from pandas_lineage.decorators.input import lineage_read


@lineage_read(
    filepath_kwarg="filepath_or_buffer",
    filepath_arg=0,
)
def read_csv(
    filepath_or_buffer: Union[FilePath, ReadCsvBuffer[bytes], ReadCsvBuffer[str]], job_run: Optional[JobRun] = None, dataset_name=None, *args, **kwargs
) -> LineageDataFrame:
    """
    Mirrors pandas.read_csv functionality with OpenLineage RunEvent emission
    """
    dataframe = pandas_read_csv(filepath_or_buffer, *args, **kwargs)
    return dataframe


@lineage_read(
    filepath_kwarg="path",
    filepath_arg=0,
)
def read_parquet(path: Union[str, Path], job_run: Optional[JobRun] = None, dataset_name=None, *args, **kwargs) -> LineageDataFrame:
    """
    Mirrors pandas.raed_parquet functionality with OpenLineage RunEvent emission
    """
    dataframe = pandas_read_parquet(path, *args, **kwargs)
    return dataframe
