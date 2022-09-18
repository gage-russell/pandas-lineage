from typing import Optional, Union

from pandas import DataFrame as PandasDataFrame
from pandas._typing import FilePath, ReadCsvBuffer
from requests.exceptions import ConnectionError, HTTPError  # type: ignore

from pandas_lineage.custom_types.lineage import JobRun
from pandas_lineage.custom_types.pandas import LineageDataFrame
from pandas_lineage.decorators._utils import (
    check_emitability,
    identify_argumet,
    try_emit,
)


def lineage_read(
    filepath_kwarg: str = "filepath_or_buffer",
    filepath_arg: int = 0,
    job_run_kwarg: str = "job_run",
    job_run_arg: int = 1,
    dataset_name_kwarg: str = "dataset_name",
    dataset_name_arg: int = 2,
    silence_lineage_failures: bool = True,
    #  TODO: make silence_lineage_failures overridable via env var/config file/etc.
):
    """
    decorator for read-related IO events
    TODO
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            dataframe: PandasDataFrame = func(*args, **kwargs)

            _should_emit: bool = True
            filepath: Union[FilePath, ReadCsvBuffer[bytes], ReadCsvBuffer[str]] = identify_argumet(kwargs, filepath_kwarg, args, filepath_arg)
            job_run: Optional[JobRun] = identify_argumet(kwargs, job_run_kwarg, args, job_run_arg)
            dataset_name: Optional[str] = identify_argumet(kwargs, dataset_name_kwarg, args, dataset_name_arg)

            _dataset_name: str = dataset_name if dataset_name else filepath

            _should_emit = check_emitability(job_run=job_run, dataset_name=_dataset_name, silence_lineage_failures=silence_lineage_failures)
            if _should_emit:
                try_emit(
                    dataframe=dataframe,
                    dataset_name=dataset_name,
                    job_run=job_run,
                    silence_lineage_failures=silence_lineage_failures,
                    io="input",
                )

            return LineageDataFrame(dataframe)

        return wrapper

    return decorator
