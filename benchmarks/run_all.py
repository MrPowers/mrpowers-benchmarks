import polars as pl

import sys
import pandas as pd
import polars_h2o_groupby_queries
import datafusion_h2o_groupby_queries
from datafusion import SessionContext

path = sys.argv[1]

# polars
# print("*** Polars ***")
# df = pl.scan_parquet(path, low_memory=True)
# polars_res = polars_h2o_groupby_queries.run_benchmarks(df).rename(columns={"duration": "polars"})
# print(polars_res)

# datafusion
print("*** DataFusion ***")
ctx = SessionContext()
df = ctx.register_parquet("x", path)
datafusion_res = datafusion_h2o_groupby_queries.run_benchmarks(ctx).rename(columns={"duration": "datafusion"})
print(datafusion_res)
