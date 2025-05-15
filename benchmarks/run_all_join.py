import polars as pl
import daft

import sys
import pandas as pd
import polars_h2o_join_queries
import datafusion_h2o_join_queries
import daft_h2o_join_queries
from datafusion import SessionContext


print("*** We starting this run ***")

x = sys.argv[1]
small = sys.argv[2]
medium = sys.argv[3]
large = sys.argv[4]

# polars
print("*** Polars ***")
x_df = pl.scan_parquet(x, low_memory=True)
small_df = pl.scan_parquet(small, low_memory=True)
medium_df = pl.scan_parquet(medium, low_memory=True)
large_df = pl.scan_parquet(large, low_memory=True)
polars_res = polars_h2o_join_queries.run_benchmarks([x_df, small_df, medium_df, large_df]).rename(columns={"duration": "polars"})
print(polars_res)

# datafusion
print("*** DataFusion ***")
ctx = SessionContext()
ctx.register_parquet("x", x)
ctx.register_parquet("small", small)
ctx.register_parquet("medium", medium)
ctx.register_parquet("large", large)
datafusion_res = datafusion_h2o_join_queries.run_benchmarks([ctx]).rename(columns={"duration": "datafusion"})
print(datafusion_res)

# daft
x = daft.read_parquet(x)
small = daft.read_parquet(small)
medium = daft.read_parquet(medium)
large = daft.read_parquet(large)

daft_res = daft_h2o_join_queries.run_benchmarks([x, small, medium, large]).rename(columns={"duration": "daft"})
print(daft_res)

# all results
res = polars_res.join(datafusion_res, on="task").join(daft_res, on="task")
print(res)

ax = res.plot.bar(rot=0)
ax.set_title('h2o join queries')
ax.set_ylabel('Seconds')
ax.set_xlabel('Queries')
ax.figure.savefig("images/h2o-join.png")


print("*** We finished this run ***")