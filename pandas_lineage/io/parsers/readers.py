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

from pandas import read_csv as pandas_read_csv
from pandas._typing import FilePath, ReadCsvBuffer

from pandas_lineage.custom_types.lineage import JobRun, PandasDataSet
from pandas_lineage.custom_types.pandas import LineageDataFrame
from pandas_lineage.decorators.input import lineage_read


@lineage_read(
    filepath_kwarg="filepath_or_buffer",
    filepath_arg=0,
    job_run_kwarg="job_run",
    job_run_arg=1,
    dataset_name_kwarg="dataset_name",
    dataset_name_arg=2,
)
def read_csv(
    filepath_or_buffer: Union[FilePath, ReadCsvBuffer[bytes], ReadCsvBuffer[str]], job_run: Optional[JobRun] = None, dataset_name=None, *args, **kwargs
) -> LineageDataFrame:
    """
    TODO
    """
    dataframe = pandas_read_csv(filepath_or_buffer, *args, **kwargs)
    return dataframe
