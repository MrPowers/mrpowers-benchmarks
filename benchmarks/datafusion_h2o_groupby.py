from datafusion import SessionContext, SessionConfig
import sys
import pandas as pd
from datafusion_h2o_groupby_queries import *
from pyarrow import csv as pacsv

path = sys.argv[1]

config = (
    SessionConfig()
    .set("datafusion.execution.parquet.pushdown_filters", "true")
)

ctx = SessionContext(config)

path = sys.argv[1]

df = ctx.register_parquet("x", path)

res = run_benchmarks(ctx)

print(res)
