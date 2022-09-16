from pathlib import Path
from typing import Any, Dict, List, Union

from pandas import DataFrame as PandasDataFrame
from pandas._typing import FilePath
from requests.exceptions import ConnectionError, HTTPError  # type: ignore

from pandas_lineage.convention.error_handling import (
    EMISSION_ERROR_STR,
    LineageEmissionError,
    silenceable_failure,
)
from pandas_lineage.custom_types.lineage import JobRun, PandasDataSet


def identify_argumet(kwargs: Dict[str, Any], kwarg_name: str, args: List[Any], arg_index: int):
    _argument = kwargs.get(kwarg_name)
    if not _argument:
        try:
            _argument = args[arg_index]
        except IndexError:
            _argument = None
    return _argument


def check_emitability(job_run: JobRun, dataset_name: Union[str, Path], silence_lineage_failures: bool):
    should_emit = True
    if not isinstance(dataset_name, (str, Path)):
        silenceable_failure(silenced=silence_lineage_failures, message=f"dataset_name expected {FilePath}; received {type(dataset_name)}", error_type=TypeError)
        should_emit = False

    if not job_run:
        silenceable_failure(
            silenced=silence_lineage_failures, message="WARNING: job_run argument must be supplied to emit lineage events", error_type=TypeError
        )
        should_emit = False

    return should_emit


def try_emit(dataframe: PandasDataFrame, dataset_name: str, job_run: JobRun, silence_lineage_failures: bool, io: str):
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
        silenceable_failure(silenced=silence_lineage_failures, message=EMISSION_ERROR_STR.format(message=e), error_type=LineageEmissionError)
    except Exception as e:
        silenceable_failure(silenced=silence_lineage_failures, message=EMISSION_ERROR_STR.format(message=e), error_type=LineageEmissionError)
