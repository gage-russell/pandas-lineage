"""
Module containing custom types related to OpenLineage events

Example dataset usage from core openlineage

```
facets = {
    "schema": SchemaDatasetFacet(fields=[SchemaField(name, dtype.name) for name, dtype in zip(df.columns, df.dtypes)])}
dataset = Dataset(namespace='', name='', facets=facets)
download = RunEvent(eventType=RunState.OTHER, eventTime=now(), run=run, job=job, producer='', inputs=[dataset])
client.emit(download)
```

***formated with Black***
"""
import copy
from dataclasses import dataclass, field
from datetime import datetime
from typing import List

from openlineage.client import OpenLineageClient
from openlineage.client.facet import (
    BaseFacet,
    DataSourceDatasetFacet,
    DocumentationDatasetFacet,
    SchemaDatasetFacet,
    SchemaField,
)
from openlineage.client.run import Dataset, InputDataset, Job, Run, RunEvent, RunState
from openlineage.client.utils import RedactMixin
from pandas import DataFrame as PandasDataFrame


@dataclass
class JobRun:
    """
    TODO
    """

    run_id: str
    namespace: str
    name: str
    producer: str = "pandas-lineage"
    run_facets: dict = field(default_factory=dict)
    job_facets: dict = field(default_factory=dict)

    def __post_init__(self):
        self.run = Run(runId=self.run_id, facets=self.run_facets)
        self.job = Job(namespace=self.namespace, name=self.name, facets=self.job_facets)
        self.client = OpenLineageClient()

    def _emit(self, **kwargs):
        event = RunEvent(eventTime=datetime.now().isoformat(), run=self.run, job=self.job, producer=self.producer, **kwargs)
        return self.client.emit(event)

    def emit_start(self):
        return self._emit(eventType=RunState.START)

    def emit_complete(self):
        return self._emit(eventType=RunState.COMPLETE)

    def emit_datasets(
        self,
        inputs: List[Dataset] = [],
        outputs: List[Dataset] = [],
        run_state: RunState = RunState.OTHER,
    ):
        return self._emit(eventType=run_state, inputs=inputs, outputs=outputs)


class PandasDataSet(Dataset):
    """
    inherits attributes: namespace, name, facets
    TODO:
    """

    def __init__(self, dataset_name: str, job_run: JobRun, facets: dict = {}, *args, **kwargs):
        super().__init__(namespace=job_run.namespace, name=dataset_name, facets=facets, *args, **kwargs)
        self._job_run = job_run

    @classmethod
    def from_pandas(cls, dataframe: PandasDataFrame, dataset_name: str, job_run: JobRun, facets: dict = {}, *args, **kwargs):
        """
        TODO
        create openlineage Dataset from pandas DataFrame
        """
        schema_fields = [SchemaField(name, dtype.name) for name, dtype in dataframe.dtypes.to_dict().items()]
        facets.update({"schema": SchemaDatasetFacet(fields=schema_fields)})
        return cls(dataset_name=dataset_name, job_run=job_run, facets=facets, *args, **kwargs)  # type: ignore

    def copy(self):
        return copy.deepcopy(self)

    def emit_input(self):
        """
        TODO
        """
        return self._job_run.emit_datasets(inputs=[self.copy()])

    def emit_output(self):
        """
        TODO
        """
        return self._job_run.emit_datasets(outputs=[self.copy()])
