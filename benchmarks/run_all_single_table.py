import polars as pl
import daft

import sys
import pandas as pd
import polars_single_table_queries
import datafusion_single_table_queries
import daft_single_table_queries
from datafusion import SessionContext
import os

path = sys.argv[1]
if path == "1e8" or path == "1e7":
    num_rows = sys.argv[1]
if num_rows == "1e8":
    path = os.getenv("G1_1e8_1e2")
elif num_rows == "1e7":
    path = os.getenv("G1_1e7_1e2")

# polars
print("*** Polars ***")
df = pl.scan_parquet(path, low_memory=True)
polars_res = polars_single_table_queries.run_benchmarks([df]).rename(columns={"duration": "polars"})
print(polars_res)

# datafusion
print("*** DataFusion ***")
ctx = SessionContext()
df = ctx.register_parquet("x", path)
datafusion_res = datafusion_single_table_queries.run_benchmarks(ctx).rename(columns={"duration": "datafusion"})
print(datafusion_res)

# daft
print("*** Daft ***")
df = daft.read_parquet(path)
daft_res = daft_single_table_queries.run_benchmarks([df]).rename(columns={"duration": "daft"})

# all results
res = polars_res.join(datafusion_res, on="task").join(daft_res, on="task")
print(res)

if num_rows:
    ax = res.plot.bar(rot=0)
    ax.set_title(f'Single table queries ({num_rows})')
    ax.set_ylabel('Seconds')
    ax.set_xlabel('Queries')
    ax.figure.savefig(f"images/single-table-{num_rows}.png")
