import pandas as pd
from helpers import benchmark, get_results
from pandas_h2o_groupby_queries import *
import sys

path = sys.argv[1]

benchmarks = {
    "duration": [],
    "task": [],
}

df = pd.read_parquet(path)

benchmark(q1, df=df, benchmarks=benchmarks, name="q1")
benchmark(q2, df=df, benchmarks=benchmarks, name="q2")
# benchmark(q3, df=df, benchmarks=benchmarks, name="q3")
# benchmark(q4, df=df, benchmarks=benchmarks, name="q4")
# benchmark(q5, df=df, benchmarks=benchmarks, name="q5")
# benchmark(q6, df=df, benchmarks=benchmarks, name="q6")
# benchmark(q7, df=df, benchmarks=benchmarks, name="q7")
# benchmark(q8, df=df, benchmarks=benchmarks, name="q8")
# benchmark(q9, df=df, benchmarks=benchmarks, name="q9")
# benchmark(q10, df=df, benchmarks=benchmarks, name="q10")

res = get_results(benchmarks).set_index("task")

print(res)

