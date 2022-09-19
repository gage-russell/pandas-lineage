"""
```
cd examples/marquez-example/
source ./start_marquez.sh
python getting_started.py
```

***formated with Black***
"""

from uuid import uuid4

import pandas

from pandas_lineage import JobRun, read_csv, read_parquet, read_json

# run 1
job_run = JobRun(run_id=uuid4().hex, namespace="marquez-examples", name="marquez-example-1")
start = job_run.emit_start()

csv_input_1 = read_csv("./mock_csv.csv", dataset_name="csv_input_dataset_1", job_run=job_run)
parquet_input_2 = read_parquet("./mock_parquet.snappy.parquet", dataset_name="parquet_input_dataset_2", job_run=job_run)
json_input_1 = read_json("./mock_json.json", dataset_name="json_input_dataset_1", job_run=job_run)

csv_output_1 = csv_input_1.dropna(how="all", axis=1)
parquet_output_2 = pandas.concat([csv_output_1, parquet_input_2])
json_output_1 = json_input_1.drop('col3', axis=1)

csv_output_1.to_csv("./test.csv", dataset_name="csv_output_dataset_1", job_run=job_run)
parquet_output_2.to_parquet("./test.snappy.parquet", dataset_name="parquet_output_dataset_2", job_run=job_run)
json_output_1.to_json("./test.json", dataset_name="json_output_dataset_2", job_run=job_run)

complete = job_run.emit_complete()
