"""
```
cd examples/marquez-example/
source ./start_marquez.sh
python getting_started.py
```

***formated with Black***
"""

from uuid import uuid4

from pandas_lineage import read_csv
from pandas_lineage.custom_types.lineage import JobRun

# run 1
job_run = JobRun(run_id=uuid4().hex, namespace="marquez-examples", name="marquez-example-1")
start = job_run.emit_start()

input_1 = read_csv("./mock_csv.csv", dataset_name="input_dataset_1", job_run=job_run)
output_1 = input_1.dropna(how="all", axis=1)
output_1.to_csv("./test.csv", dataset_name="output_dataset_1", job_run=job_run)

complete = job_run.emit_complete()
