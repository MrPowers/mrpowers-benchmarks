from helpers import benchmark, get_results
import sys
import pandas as pd
from duckdb_h2o_groupby_queries import *

path = sys.argv[1]

duckdb_benchmarks = {
    "duration": [],  # in seconds
    "task": [],
}

benchmark(q1, dfs=path, benchmarks=duckdb_benchmarks, name="q1")
benchmark(q2, dfs=path, benchmarks=duckdb_benchmarks, name="q2")
benchmark(q3, dfs=path, benchmarks=duckdb_benchmarks, name="q3")
benchmark(q4, dfs=path, benchmarks=duckdb_benchmarks, name="q4")
benchmark(q5, dfs=path, benchmarks=duckdb_benchmarks, name="q5")
benchmark(q6, dfs=path, benchmarks=duckdb_benchmarks, name="q6")
benchmark(q7, dfs=path, benchmarks=duckdb_benchmarks, name="q7")
benchmark(q8, dfs=path, benchmarks=duckdb_benchmarks, name="q8")
benchmark(q9, dfs=path, benchmarks=duckdb_benchmarks, name="q9")
# benchmark(q10, dfs=path, benchmarks=duckdb_benchmarks, name="q10")

duckdb_res_temp = get_results(duckdb_benchmarks).set_index("task")

print(duckdb_res_temp)
