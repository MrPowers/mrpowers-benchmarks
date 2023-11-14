import datafusion
from helpers import benchmark, get_results
import sys
import pandas as pd
from datafusion_h2o_groupby_queries import *
from pyarrow import csv as pacsv

path = sys.argv[1]

ctx = datafusion.SessionContext()

# ctx.register_csv("x", path)

data = pacsv.read_csv(path, convert_options=pacsv.ConvertOptions(auto_dict_encode=True))
print("dataset loaded")

# ctx = df.ExecutionContext()
ctx.register_record_batches("x", [data.to_batches()])
print("registered record batches")

benchmarks = {
    "duration": [],
    "task": [],
}

benchmark(q1, df=ctx, benchmarks=benchmarks, name="q1")
benchmark(q2, df=ctx, benchmarks=benchmarks, name="q2")
benchmark(q3, df=ctx, benchmarks=benchmarks, name="q3")
benchmark(q4, df=ctx, benchmarks=benchmarks, name="q4")
benchmark(q5, df=ctx, benchmarks=benchmarks, name="q5")
# benchmark(q6, df=ctx, benchmarks=benchmarks, name="q6")
benchmark(q7, df=ctx, benchmarks=benchmarks, name="q7")
benchmark(q8, df=ctx, benchmarks=benchmarks, name="q8")
benchmark(q9, df=ctx, benchmarks=benchmarks, name="q9")
benchmark(q10, df=ctx, benchmarks=benchmarks, name="q10")

res = get_results(benchmarks).set_index("task")
print(res)

