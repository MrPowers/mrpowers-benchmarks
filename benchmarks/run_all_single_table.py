import polars as pl
import daft

import sys
import pandas as pd
import polars_single_table_queries
import datafusion_single_table_queries
import daft_single_table_queries
from datafusion import SessionContext

path = sys.argv[1]

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

ax = res.plot.bar(rot=0)
ax.set_title('Single table queries')
ax.set_ylabel('Seconds')
ax.set_xlabel('Queries')
ax.figure.savefig("images/single-table.png")
