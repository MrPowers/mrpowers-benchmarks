from datafusion import SessionContext, SessionConfig
import sys
import pandas as pd
from datafusion_h2o_join_queries import *

x = sys.argv[1]
small = sys.argv[2]
medium = sys.argv[3]
large = sys.argv[4]

ctx = SessionContext()

ctx.register_parquet("x", x)
ctx.register_parquet("small", small)
ctx.register_parquet("medium", medium)
ctx.register_parquet("large", large)

res = run_benchmarks([ctx])

print(res)
