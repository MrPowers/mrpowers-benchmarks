import polars as pl
import daft

import sys
import pandas as pd
import polars_h2o_groupby_queries
import datafusion_h2o_groupby_queries
import daft_h2o_groupby_queries
import duckdb_h2o_groupby_queries
from datafusion import SessionContext

path = sys.argv[1]

# Fast group by queries

# polars
print("*** Polars ***")
df = pl.scan_parquet(path, low_memory=True)
polars_res = polars_h2o_groupby_queries.run_benchmarks([df]).rename(columns={"duration": "polars"})
print(polars_res)

# datafusion
print("*** DataFusion ***")
ctx = SessionContext()
df = ctx.register_parquet("x", path)
datafusion_res = datafusion_h2o_groupby_queries.run_benchmarks(ctx).rename(columns={"duration": "datafusion"})
print(datafusion_res)

# daft
print("*** Daft ***")
df = daft.read_parquet(path)
daft_res = daft_h2o_groupby_queries.run_benchmarks([df]).rename(columns={"duration": "daft"})

# duckdb
print("*** DuckDB ***")
duckdb_res = duckdb_h2o_groupby_queries.run_benchmarks([path]).rename(columns={"duration": "duckdb"})

# all results
res = (
    polars_res
    .join(datafusion_res, on="task")
    .join(daft_res, on="task")
    .join(duckdb_res, on="task")
)
print(res)

ax = res.plot.bar(rot=0)
ax.set_title('Fast h2o groupby queries')
ax.set_ylabel('Seconds')
ax.set_xlabel('Queries')
ax.figure.savefig("images/groupby-fast.png")

# Slow group by queries

# polars
print("*** Polars ***")
df = pl.scan_parquet(path, low_memory=True)
polars_res = polars_h2o_groupby_queries.run_benchmarks_slow([df]).rename(columns={"duration": "polars"})
print(polars_res)

# datafusion
print("*** DataFusion ***")
ctx = SessionContext()
df = ctx.register_parquet("x", path)
datafusion_res = datafusion_h2o_groupby_queries.run_benchmarks_slow(ctx).rename(columns={"duration": "datafusion"})
print(datafusion_res)

# daft
print("*** Daft ***")
df = daft.read_parquet(path)
daft_res = daft_h2o_groupby_queries.run_benchmarks_slow([df]).rename(columns={"duration": "daft"})

# duckdb
print("*** DuckDB ***")
duckdb_res = duckdb_h2o_groupby_queries.run_benchmarks_slow([path]).rename(columns={"duration": "duckdb"})

# all results
res = polars_res.join(datafusion_res, on="task").join(daft_res, on="task").join(duckdb_res, on="task")
print(res)

ax = res.plot.bar(rot=0)
ax.set_title('Slower h2o groupby queries')
ax.set_ylabel('Seconds')
ax.set_xlabel('Queries')
ax.figure.savefig("images/groupby-slow.png")
