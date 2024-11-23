import daft
import pandas as pd
from daft_h2o_groupby_queries import *
import sys

path = sys.argv[1]

df = daft.read_parquet(path)

res = run_benchmarks(df)

print("start")
print(res)
print("end")
