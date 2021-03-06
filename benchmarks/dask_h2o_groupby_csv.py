import dask.dataframe as dd
import pandas as pd
import dask
from helpers import benchmark, get_results
from dask_h2o_groupby_queries import *
import sys

print("dask version: %s" % dask.__version__)

path = sys.argv[1]

d = dd.read_csv(path)
df = d.persist()

benchmarks = {
    "duration": [],
    "task": [],
}

benchmark(q1, df=df, benchmarks=benchmarks, name="q1")
benchmark(q2, df=df, benchmarks=benchmarks, name="q2")
benchmark(q3, df=df, benchmarks=benchmarks, name="q3")
benchmark(q4, df=df, benchmarks=benchmarks, name="q4")
benchmark(q5, df=df, benchmarks=benchmarks, name="q5")
benchmark(q7, df=df, benchmarks=benchmarks, name="q7")
benchmark(q8, df=df, benchmarks=benchmarks, name="q8")
benchmark(q9, df=df, benchmarks=benchmarks, name="q9")

res = get_results(benchmarks).set_index("task")

print(res)
