import logging
from typing import Any, Dict, List, Optional, Union

from pandas import DataFrame as PandasDataFrame
from pandas._typing import FilePath, ReadCsvBuffer
from requests.exceptions import ConnectionError, HTTPError  # type: ignore

from pandas_lineage.convention.error_handling import (
    EMISSION_ERROR_STR,
    LineageEmissionError,
    silenceable_failure,
)
from pandas_lineage.custom_types.lineage import JobRun, PandasDataSet
from pandas_lineage.custom_types.pandas import LineageDataFrame

logger = logging.getLogger()


def _identify_argumet(kwargs: Dict[str, Any], kwarg_name: str, args: List[Any], arg_index: int):
    _argument = kwargs.get(kwarg_name)
    if not _argument:
        try:
            _argument = args[arg_index]
        except IndexError:
            _argument = None
    return _argument


def _check_emitability(job_run: JobRun, dataset_name: str, silence_lineage_failures: bool):
    should_emit = True
    if not isinstance(dataset_name, str):
        silenceable_failure(silenced=silence_lineage_failures, message=f"dataset_name expected <str>; received {type(dataset_name)}", error_type=TypeError)
        should_emit = False

    if not job_run:
        silenceable_failure(
            silenced=silence_lineage_failures, message="WARNING: job_run argument must be supplied to emit lineage events", error_type=TypeError
        )
        should_emit = False

    return should_emit


def _try_emit(dataframe: PandasDataFrame, dataset_name: str, job_run: JobRun, silence_lineage_failures: bool, io: str):
    openlineage_dataset = PandasDataSet.from_pandas(dataframe=dataframe, dataset_name=dataset_name, job_run=job_run)
    try:
        if io == "input":
            openlineage_dataset.emit_input()
        elif io == "output":
            openlineage_dataset.emit_output()
        else:
            # TODO: implement proper enum pattern
            pass
    except (HTTPError, ConnectionError) as e:
        silenceable_failure(silenced=silence_lineage_failures, message=EMISSION_ERROR_STR.format(e), error_type=LineageEmissionError)
    except Exception as e:
        silenceable_failure(silenced=silence_lineage_failures, message=EMISSION_ERROR_STR.format(e), error_type=LineageEmissionError)


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
            filepath: Union[FilePath, ReadCsvBuffer[bytes], ReadCsvBuffer[str]] = _identify_argumet(kwargs, filepath_kwarg, args, filepath_arg)
            job_run: Optional[JobRun] = _identify_argumet(kwargs, job_run_kwarg, args, job_run_arg)
            dataset_name: Optional[str] = _identify_argumet(kwargs, dataset_name_kwarg, args, dataset_name_arg)

            _dataset_name: str = dataset_name if dataset_name else filepath

            _should_emit = _check_emitability(job_run=job_run, dataset_name=_dataset_name, silence_lineage_failures=silence_lineage_failures)
            if _should_emit:
                _try_emit(
                    dataframe=dataframe,
                    dataset_name=dataset_name,
                    job_run=job_run,
                    silence_lineage_failures=silence_lineage_failures,
                    io="input",
                )

            return LineageDataFrame(dataframe)

        return wrapper

    return decorator


def lineage_write(
    dataframe_arg: int = 0,
    filepath_kwarg: str = "path_or_buf",
    filepath_arg: int = 1,
    job_run_kwarg: str = "job_run",
    job_run_arg: int = 2,
    dataset_name_kwarg: str = "dataset_name",
    dataset_name_arg: int = 3,
    silence_lineage_failures: bool = True,
    #  TODO: make silence_lineage_failures overridable via env var/config file/etc.
):
    """
    decorator for read-related IO events
    TODO
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            _ = func(*args, **kwargs)

            _should_emit: bool = True
            dataframe = args[dataframe_arg]
            filepath: Union[FilePath, ReadCsvBuffer[bytes], ReadCsvBuffer[str]] = _identify_argumet(kwargs, filepath_kwarg, args, filepath_arg)
            job_run: Optional[JobRun] = _identify_argumet(kwargs, job_run_kwarg, args, job_run_arg)
            dataset_name: Optional[str] = _identify_argumet(kwargs, dataset_name_kwarg, args, dataset_name_arg)

            _dataset_name: str = dataset_name if dataset_name else filepath

            _should_emit = _check_emitability(job_run=job_run, dataset_name=_dataset_name, silence_lineage_failures=silence_lineage_failures)
            if _should_emit:
                if _should_emit:
                    _try_emit(
                        dataframe=dataframe,
                        dataset_name=dataset_name,
                        job_run=job_run,
                        silence_lineage_failures=silence_lineage_failures,
                        io="output",
                    )

            return

        return wrapper

    return decorator
