"""
Module containing custom types related to pandas
"""
from typing import Optional, Union

from pandas import DataFrame as PandasDataFrame
from pandas import Series as PandasSeries
from pandas._typing import FilePath, WriteBuffer

from pandas_lineage.custom_types.lineage import JobRun, PandasDataSet
from pandas_lineage.decorators.output import lineage_write


class LineageSeries(PandasSeries):
    @property
    def _constructor(self):
        """
        reference: https://pandas.pydata.org/pandas-docs/stable/development/extending.html#subclassing-pandas-data-structures
        """
        return LineageSeries

    @property
    def _constructor_expanddim(self):
        """
        reference: https://pandas.pydata.org/pandas-docs/stable/development/extending.html#subclassing-pandas-data-structures
        """
        return LineageDataFrame


class LineageDataFrame(PandasDataFrame):
    """
    class inheriting from pandas.DataFrame which overrides functionality to emit lineage events
    TODO: investigate if we should be using accessor extensions: https://pandas.pydata.org/pandas-docs/stable/development/extending.html
    """

    @property
    def _constructor(self):
        """
        reference: https://pandas.pydata.org/pandas-docs/stable/development/extending.html#subclassing-pandas-data-structures
        """
        return LineageDataFrame

    @property
    def _constructor_sliced(self):
        """
        reference: https://pandas.pydata.org/pandas-docs/stable/development/extending.html#subclassing-pandas-data-structures
        """
        return LineageSeries

    @lineage_write(
        dataframe_arg=0,
        filepath_kwarg="path_or_buf",
        filepath_arg=1,
        job_run_kwarg="job_run",
        job_run_arg=2,
        dataset_name_kwarg="dataset_name",
        dataset_name_arg=3,
    )
    def to_csv(
        self,
        path_or_buf: Optional[Union[FilePath, WriteBuffer[bytes], WriteBuffer[str]]] = None,
        job_run: Optional[JobRun] = None,
        dataset_name=None,
        *args,
        **kwargs
    ) -> None:
        """
        extending the functionality of pandas.DataFrame.to_csv to emit lineage events
        TODO: is there a more central I/O code location in pandas such that we don't have to override each format?
        references:
        * https://github.com/pandas-dev/pandas/blob/main/pandas/io/formats/csvs.py
        """
        super().to_csv(path_or_buf, *args, **kwargs)
