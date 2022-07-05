import dask.dataframe as dd
import pandas as pd
import dask
from helpers import benchmark, get_results
import sys
from dask_mrpowers_filter_queries import *

print("dask version: %s" % dask.__version__)

path = sys.argv[1]

df = dd.read_csv(path)

benchmarks = {
    "duration": [],  # in seconds
    "task": [],
}

benchmark(q1, df=df, benchmarks=benchmarks, name="q1")
benchmark(q2, df=df, benchmarks=benchmarks, name="q2")
benchmark(q3, df=df, benchmarks=benchmarks, name="q3")
benchmark(q4, df=df, benchmarks=benchmarks, name="q4")
benchmark(q5, df=df, benchmarks=benchmarks, name="q5")

res = get_results(benchmarks).set_index("task")
print(res)
