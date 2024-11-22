import pandas as pd
from pandas_h2o_groupby_queries import *
import sys

path = sys.argv[1]

df = pd.read_parquet(path)

res = run_benchmarks(df)

print(res)
