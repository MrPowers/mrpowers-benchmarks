import polars as pl

import sys
import pandas as pd
from polars_h2o_groupby_queries import *

path = sys.argv[1]

df = pl.scan_parquet(path, low_memory=True)

res = run_benchmarks([df])

print(res)
