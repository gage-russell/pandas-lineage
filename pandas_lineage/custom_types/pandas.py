"""
Module containing custom types related to pandas
"""
from typing import Optional, Union

from pandas import DataFrame as PandasDataFrame
from pandas import Series as PandasSeries
from pandas._typing import FilePath, WriteBuffer

from pandas_lineage.custom_types.lineage import JobRun
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
        Mirrors pandas.DataFrame.to_parquet functionality with OpenLineage RunEvent emission
        """
        super().to_csv(path_or_buf, *args, **kwargs)

    @lineage_write(
        dataframe_arg=0,
        filepath_kwarg="path",
    )
    def to_parquet(
        self, path: Optional[Union[FilePath, WriteBuffer[bytes]]] = None, job_run: Optional[JobRun] = None, dataset_name=None, *args, **kwargs
    ) -> None:
        """
        Mirrors pandas.DataFrame.to_parquet functionality with OpenLineage RunEvent emission
        """
        super().to_parquet(path, *args, **kwargs)
