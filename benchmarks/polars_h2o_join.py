import polars as pl

import sys
import pandas as pd
from polars_h2o_join_queries import *

x = sys.argv[1]
small = sys.argv[2]
medium = sys.argv[3]
large = sys.argv[4]

x = pl.scan_parquet(x, low_memory=True)
small = pl.scan_parquet(small, low_memory=True)
medium = pl.scan_parquet(medium, low_memory=True)
large = pl.scan_parquet(large, low_memory=True)

res = run_benchmarks([x, small, medium, large])

print(res)
