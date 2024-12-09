import daft
import pandas as pd
from daft_h2o_join_queries import *
import sys

x = sys.argv[1]
small = sys.argv[2]
medium = sys.argv[3]
large = sys.argv[4]

x = daft.read_parquet(x)
small = daft.read_parquet(small)
medium = daft.read_parquet(medium)
large = daft.read_parquet(large)

res = run_benchmarks([x, small, medium, large])
print(res)
