"""
Module containing custom types related to pandas
"""
from typing import Optional, Union
from uuid import uuid4

from pandas import DataFrame as PandasDataFrame
from pandas import Series as PandasSeries
from pandas._typing import FilePath, WriteBuffer

from pandas_lineage.custom_types.lineage import JobRun, PandasDataSet


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
        # TODO: some of this boilerplate lineage event emitting code might get repetitive. Might a decorator save us code duplication?
        if not job_run:
            job_run = JobRun(run_id=uuid4().hex, namespace="pandas", name="empty")

        if not dataset_name:
            if isinstance(path_or_buf, str):
                dataset_name = path_or_buf
            else:
                raise TypeError("path_or_buf must be supplied as a string path or dataset_name is required")

        super().to_csv(path_or_buf, *args, **kwargs)

        openlineage_dataset = PandasDataSet.from_pandas(dataframe=self, dataset_name=dataset_name, job_run=job_run)
        openlineage_dataset.emit_output()
