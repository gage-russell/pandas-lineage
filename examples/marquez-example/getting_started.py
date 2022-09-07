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
from pandas_lineage.types import JobRun

# run 1
job_run = JobRun(run_id=uuid4().hex, namespace="test", name="test")
start = job_run.emit_start()

df = read_csv("./mock_csv.csv", job_run=job_run)

complete = job_run.emit_complete()

# run 2
job_run = JobRun(run_id=uuid4().hex, namespace="test", name="test")
start = job_run.emit_start()

# specify dataset_name
df = read_csv("./mock_csv.csv", dataset_name="test-dataset", job_run=job_run)
# allow dataset_name to be defaulted to file path
df = read_csv("./mock_csv.csv", job_run=job_run)

complete = job_run.emit_complete()
