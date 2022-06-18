import dask.dataframe as dd
import pandas as pd
import dask
from helpers import benchmark, get_results
from dask_h2o_groupby_queries import *
import sys

print("dask version: %s" % dask.__version__)

csv_path = sys.argv[1]

dask_data = dd.read_csv(csv_path)

dask_csv_benchmarks = {
    "duration": [],
    "task": [],
}

benchmark(q1, df=dask_data, benchmarks=dask_csv_benchmarks, name="q1")
benchmark(q2, df=dask_data, benchmarks=dask_csv_benchmarks, name="q2")
benchmark(q3, df=dask_data, benchmarks=dask_csv_benchmarks, name="q3")
benchmark(q4, df=dask_data, benchmarks=dask_csv_benchmarks, name="q4")
benchmark(q5, df=dask_data, benchmarks=dask_csv_benchmarks, name="q5")
benchmark(q7, df=dask_data, benchmarks=dask_csv_benchmarks, name="q7")
benchmark(q8, df=dask_data, benchmarks=dask_csv_benchmarks, name="q8")
benchmark(q9, df=dask_data, benchmarks=dask_csv_benchmarks, name="q9")

dask_res_csv_temp = get_results(dask_csv_benchmarks).set_index("task")

print(dask_res_csv_temp)
