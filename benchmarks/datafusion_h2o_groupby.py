from datafusion import SessionContext
import sys
import pandas as pd
from datafusion_h2o_groupby_queries import *
from pyarrow import csv as pacsv

path = sys.argv[1]

ctx = datafusion.SessionContext()

path = sys.argv[1]

df = ctx.register_parquet("x", path)

res = run_benchmarks(ctx)

print(res)
