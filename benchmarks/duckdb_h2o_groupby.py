from helpers import benchmark, get_results
import sys
import pandas as pd
from duckdb_h2o_groupby_queries import *

path = sys.argv[1]

polars_benchmarks = {
    "duration": [],  # in seconds
    "task": [],
}

benchmark(q1, df=path, benchmarks=polars_benchmarks, name="q1")
# benchmark(q2, df=df, benchmarks=polars_benchmarks, name="q2")
# benchmark(q3, df=df, benchmarks=polars_benchmarks, name="q3")
# benchmark(q4, df=df, benchmarks=polars_benchmarks, name="q4")
# benchmark(q5, df=df, benchmarks=polars_benchmarks, name="q5")
# benchmark(q6, df=df, benchmarks=polars_benchmarks, name="q6")
# benchmark(q7, df=df, benchmarks=polars_benchmarks, name="q7")
# benchmark(q8, df=df, benchmarks=polars_benchmarks, name="q8")
# benchmark(q9, df=df, benchmarks=polars_benchmarks, name="q9")
# benchmark(q10, df=df, benchmarks=polars_benchmarks, name="q10")

polars_res_temp = get_results(polars_benchmarks).set_index("task")

print(polars_res_temp)
