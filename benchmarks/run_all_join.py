import polars as pl
import daft

import sys
import pandas as pd
import polars_h2o_join_queries
import datafusion_h2o_join_queries
import daft_h2o_join_queries
import duckdb_h2o_join_queries
from datafusion import SessionContext
import os


print("*** We starting this run ***")

if sys.argv[1] == "1e8" or sys.argv[1] == "1e7":
    num_rows = sys.argv[1]

if sys.argv[1] == "1e7":
    x = os.getenv("J1_1e7_NA")
    small = os.getenv("J1_1e7_1e1")
    medium = os.getenv("J1_1e7_1e4")
    large = os.getenv("J1_1e7_1e7")
elif sys.argv[1] == "1e8":
    x = os.getenv("J1_1e8_NA")
    small = os.getenv("J1_1e8_1e2")
    medium = os.getenv("J1_1e8_1e5")
    large = os.getenv("J1_1e8_1e8")
else:
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
print("*** Daft ***")
daft_x = daft.read_parquet(x)
daft_small = daft.read_parquet(small)
daft_medium = daft.read_parquet(medium)
daft_large = daft.read_parquet(large)
daft_res = daft_h2o_join_queries.run_benchmarks([daft_x, daft_small, daft_medium, daft_large]).rename(columns={"duration": "daft"})
print(daft_res)

# duckdb
# print("*** DuckDB ***")
# duckdb_res = duckdb_h2o_join_queries.run_benchmarks([x, small, medium, large]).rename(columns={"duration": "duckdb"})
# print(duckdb_res)

# all results
res = (
    polars_res
    .join(datafusion_res, on="task")
    .join(daft_res, on="task")
    # .join(duckdb_res, on="task"
)
print(res)

if num_rows:
    ax = res.plot.bar(rot=0)
    ax.set_title(f'h2o join queries ({num_rows})')
    ax.set_ylabel('Seconds')
    ax.set_xlabel('Queries')
    ax.figure.savefig(f"images/h2o-join-{num_rows}.png")

